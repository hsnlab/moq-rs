use std::sync::atomic;
use std::{io::Cursor, sync::Arc};

use crate::smartout::SmartOut;
use anyhow::Context;
use log::{debug, info, trace, warn};
use moq_transport::serve::{
    ServeError, SubgroupObjectReader, SubgroupReader, TrackReader, TrackReaderMode,
    Tracks, TracksReader, TracksWriter,
};
use moq_transport::session::{SubscribeFilter, Subscriber};
use mp4::ReadBox;
use tokio::io::AsyncWrite;
use tokio::{io::AsyncReadExt, sync::Mutex, task::JoinSet};

pub struct Media<O: AsyncWrite + Send + Unpin + 'static> {
    subscriber: Subscriber,
    subscribe_next: Arc<atomic::AtomicU64>,
    broadcast: TracksReader,
    tracks_writer: TracksWriter,
    output: Arc<Mutex<SmartOut<O>>>,
}

impl<O: AsyncWrite + Send + Unpin + 'static> Media<O> {
    pub async fn new(
        subscriber: Subscriber,
        tracks: Tracks,
        output: Arc<Mutex<SmartOut<O>>>,
    ) -> anyhow::Result<Self> {
        let (tracks_writer, _tracks_request, tracks_reader) = tracks.produce();
        let broadcast = tracks_reader; // breadcrumb for navigating API name changes
        Ok(Self {
            subscriber,
            subscribe_next: Arc::new(atomic::AtomicU64::new(0)),
            broadcast,
            tracks_writer,
            output,
        })
    }

    pub async fn run(&mut self) -> anyhow::Result<()> {
        let moov = {
            let init_track_name = "0.mp4";
            let track = self
                .tracks_writer
                .create(init_track_name)
                .context("failed to create init track")?;


            let subscribe_id = self.subscribe_next.fetch_add(1, atomic::Ordering::Relaxed);
            self.output.lock().await.add_subscriber(SmartOut::<O>::create_key(&track.info), subscribe_id, self.subscriber.clone());

            let mut subscriber = self.subscriber.clone();
            tokio::task::spawn(async move {
                subscriber
                    .subscribe(Some(subscribe_id), track, SubscribeFilter::LatestObject)
                    .await
                    .unwrap_or_else(|err| {
                        warn!("failed to subscribe to init track: {err:?}");
                    });
            });

            let track = self
                .broadcast
                .subscribe(init_track_name)
                .context("no init track")?;
            let mut group = match track.mode().await? {
                TrackReaderMode::Subgroups(mut groups) => {
                    groups.next().await?.context("no init group")?
                }
                _ => anyhow::bail!("expected init segment"),
            };

            let mut object = group.next().await?.context("no init fragment")?;
            let buf = Self::read_object(&mut object).await?;
            self.output.lock().await.write_object(object, &buf).await?;
            let mut reader = Cursor::new(&buf);

            let ftyp = read_atom(&mut reader).await?;
            anyhow::ensure!(&ftyp[4..8] == b"ftyp", "expected ftyp atom");

            let moov = read_atom(&mut reader).await?;
            anyhow::ensure!(&moov[4..8] == b"moov", "expected moov atom");
            let mut moov_reader = Cursor::new(&moov);
            let moov_header = mp4::BoxHeader::read(&mut moov_reader)?;

            mp4::MoovBox::read_box(&mut moov_reader, moov_header.size)?
        };

        let mut has_video = false;
        let mut has_audio = false;
        let mut tracks = vec![];
        for trak in &moov.traks {
            let id = trak.tkhd.track_id;
            let name = format!("{}.m4s", id);
            info!("found track {name}");
            let mut active = false;
            if !has_video && trak.mdia.minf.stbl.stsd.avc1.is_some() {
                active = true;
                has_video = true;
                info!("using {name} for video");
            }
            if !has_audio && trak.mdia.minf.stbl.stsd.mp4a.is_some() {
                active = true;
                has_audio = true;
                info!("using {name} for audio");
            }
            if active {
                let track = self
                    .tracks_writer
                    .create(&name)
                    .context("failed to create track")?;

                let subscribe_id = self.subscribe_next.fetch_add(1, atomic::Ordering::Relaxed);
                self.output.lock().await.add_subscriber(SmartOut::<O>::create_key(&track.info), subscribe_id, self.subscriber.clone());

                let mut subscriber = self.subscriber.clone();
                tokio::task::spawn(async move {

                    subscriber
                        .subscribe(Some(subscribe_id), track, SubscribeFilter::LatestObject)
                        .await
                        .unwrap_or_else(|err| {
                            warn!("failed to subscribe to track: {err:?}");
                        });
                });

                tracks.push(self.broadcast.subscribe(&name).context("no track")?);
            }
        }

        info!("playing {} tracks", tracks.len());
        let mut tasks = JoinSet::new();
        for track in tracks {
            let out = self.output.clone();
            tasks.spawn(async move {
                let name = track.name.clone();
                if let Err(err) = Self::recv_track(track, out).await {
                    warn!("failed to play track {name}: {err:?}");
                }
            });
        }
        while tasks.join_next().await.is_some() {}
        Err(anyhow::Error::msg("Finished receiving the media"))
        //Ok(())
    }

    async fn recv_track(track: TrackReader, out: Arc<Mutex<SmartOut<O>>>) -> anyhow::Result<()> {
        let name = track.name.clone();
        debug!("track {name}: start");
        if let TrackReaderMode::Subgroups(mut groups) = track.mode().await? {
            while let Some(group) = groups.next().await? {
                if let Err(err) = Self::recv_group(group, out.clone()).await {
                    warn!("failed to receive group: {err:?}");
                }
            }
        }
        debug!("track {name}: finish");
        Ok(())
    }

    async fn recv_group(mut group: SubgroupReader, out: Arc<Mutex<SmartOut<O>>>) -> anyhow::Result<()> {
        trace!("group={} start", group.group_id);

        let key = SmartOut::<O>::create_key(&group);
        while let Some(object) = group.next().await? {
            trace!(
                "group={} fragment={} start",
                group.group_id,
                object.object_id
            );

            Self::recv_object(key.clone(), object, out.clone()).await?;
        }

        Ok(())
    }

    async fn recv_object(
        key: String,
        mut object: SubgroupObjectReader,
        out: Arc<Mutex<SmartOut<O>>>,
    ) -> anyhow::Result<()> {
        let buf = Self::read_object(&mut object).await?;

        out.lock().await.write_group_object(key, object, &buf).await?;

        Ok(())
    }

    async fn read_object(object: &mut SubgroupObjectReader) -> Result<Vec<u8>, ServeError> {
        let mut buf = Vec::with_capacity(object.size);
        while let Some(chunk) = object.read().await? {
            buf.extend_from_slice(&chunk);
        }
        Ok(buf)
    }
}

// Read a full MP4 atom into a vector.
async fn read_atom<R: AsyncReadExt + Unpin>(reader: &mut R) -> anyhow::Result<Vec<u8>> {
    // Read the 8 bytes for the size + type
    let mut buf = [0u8; 8];
    reader.read_exact(&mut buf).await?;

    // Convert the first 4 bytes into the size.
    let size = u32::from_be_bytes(buf[0..4].try_into()?) as u64;

    let mut raw = buf.to_vec();

    let mut limit = match size {
        // Runs until the end of the file.
        0 => reader.take(u64::MAX),

        // The next 8 bytes are the extended size to be used instead.
        1 => {
            reader.read_exact(&mut buf).await?;
            let size_large = u64::from_be_bytes(buf);
            anyhow::ensure!(
                size_large >= 16,
                "impossible extended box size: {}",
                size_large
            );

            reader.take(size_large - 16)
        }

        2..=7 => {
            anyhow::bail!("impossible box size: {}", size)
        }

        size => reader.take(size - 8),
    };

    // Append to the vector and return it.
    let _read_bytes = limit.read_to_end(&mut raw).await?;

    Ok(raw)
}
