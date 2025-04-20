use moq_transport::serve::SubgroupObjectReader;
use moq_transport::{
    message::SubscribePair,
    serve::Track,
    session::{SubscribeFilter, Subscriber},
};
use std::collections::HashMap;
use tokio::io::{AsyncWrite, AsyncWriteExt};

struct TrackPlayoutStatus {
    last_id: Option<(u64, u64)>,
    // Given
    // - "A subscriber MUST NOT make multiple active subscriptions for a track within a single session [...]"
    // - "Subscribe ID is a variable length integer that MUST be unique [...]"
    // we can conclude, that Subscribe IDs present here will correspond to one and only one Track (in the context of a
    // session).
    // However, to identify which session sent a certain object and consequently which sessions to update, we need to
    // store the artificial session IDs alongside, that the caller of write_group_object has to specify.
    subscribers: Vec<(u64, u64, Subscriber)>,
}

#[derive(Debug)]
pub enum UpdateBasis {
    SameGroupNextObject(u64),
    NextGroupFirstObject(u64),
}

pub struct SmartOut<O: AsyncWrite + Send + Unpin + 'static> {
    out: O,
    tracks: HashMap<String, TrackPlayoutStatus>, // For each unique track
    basis: UpdateBasis,
}

impl<O: AsyncWrite + Send + Unpin + 'static> SmartOut<O> {
    pub fn new(out: O, basis: UpdateBasis) -> Self {
        Self {
            out,
            tracks: HashMap::new(),
            basis,
        }
    }

    pub fn create_key(track: &Track) -> String {
        track.namespace.to_utf8_path() + ":" + &track.name
    }

    pub async fn write_object(
        &mut self,
        sender_session_id: u64,
        object: SubgroupObjectReader,
        buf: &Vec<u8>,
    ) -> Result<(), std::io::Error> {
        let key = Self::create_key(&object.group);
        return self
            .write_group_object(sender_session_id, key, object, buf)
            .await;
    }

    pub async fn write_group_object(
        &mut self,
        sender_session_id: u64,
        key: String,
        object: SubgroupObjectReader,
        buf: &Vec<u8>,
    ) -> Result<(), std::io::Error> {
        let group_id = object.group_id;
        let object_id = object.object_id;

        let id = (group_id, object_id);
        if let Some(playout) = self.tracks.get(&key) {
            if let Some(last_id) = playout.last_id {
                if id <= last_id {
                    // Already seen this pair
                    return Ok(());
                }
            }
        }

        self.out.write_all(&buf).await?;

        let playout = self
            .tracks
            .get_mut(&key)
            .expect("trying to write object with no corresponding subscriptions");
        playout.last_id = Some(id);

        let next = match self.basis {
            UpdateBasis::SameGroupNextObject(n) => SubscribeFilter::AbsoluteStart(SubscribePair {
                group: group_id,
                object: object_id + n,
            }),
            UpdateBasis::NextGroupFirstObject(n) => SubscribeFilter::AbsoluteStart(SubscribePair {
                group: group_id + n,
                object: 0,
            }),
        };

        for (session_id, subscribe_id, subscriber) in &mut playout.subscribers {
            if *session_id == sender_session_id {
                continue;
            }

            let _ = subscriber.subscribe_update(*subscribe_id, next.clone(), 127);
        }

        Ok(())
    }

    pub fn add_subscriber(
        self: &mut Self,
        key: String,
        session_id: u64,
        subscribe_id: u64,
        subscriber: Subscriber,
    ) {
        if let Some(playout) = self.tracks.get_mut(&key) {
            playout
                .subscribers
                .push((session_id, subscribe_id, subscriber));
        } else {
            self.tracks.insert(
                key,
                TrackPlayoutStatus {
                    last_id: None,
                    subscribers: vec![(session_id, subscribe_id, subscriber)],
                },
            );
        }
    }
}
