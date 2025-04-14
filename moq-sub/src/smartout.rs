use std::collections::HashMap;
use moq_transport::{message::SubscribePair, serve::Track, session::{SubscribeFilter, Subscriber}};
use moq_transport::serve::SubgroupObjectReader;
use tokio::io::{AsyncWrite, AsyncWriteExt};

struct TrackPlayoutStatus {
    last_id: Option<(u64, u64)>,
    subscribers: Vec<(u64, Subscriber)>,
}

pub struct SmartOut<O: AsyncWrite + Send + Unpin + 'static> {
    out: O,
    tracks: HashMap<String, TrackPlayoutStatus>, // For each unique track
}

impl<O: AsyncWrite + Send + Unpin + 'static> SmartOut<O> {
    pub fn new(out: O) -> Self {
        Self {
            out,
            tracks: HashMap::new(),
        }
    }

    pub fn create_key(track: &Track) -> String {
        track.namespace.to_utf8_path() + ":" + &track.name
    }


    pub async fn write_object(
        &mut self,
        object: SubgroupObjectReader,
        buf: &Vec<u8>,
    ) -> Result<(), std::io::Error> {
        let key = Self::create_key(&object.group);
        return self.write_group_object(key, object, buf).await;
    }

    pub async fn write_group_object(
        &mut self,
        key: String,
        object: SubgroupObjectReader,
        buf: &Vec<u8>,
    ) -> Result<(), std::io::Error> {
        let group_id = object.group_id;
        let object_id = object.object_id;

        let id = (group_id, object_id);
        if let Some(playout) = self.tracks.get(&key) {
            if let Some(last_id) = playout.last_id {
                if id <= last_id { // Already seen this pair
                    return Ok(());
                }
            }
        }

        self.out.write_all(&buf).await?;
        let playout = self.tracks.get_mut(&key).expect("trying to write object with no corresponding subscriptions");
        playout.last_id = Some(id);

        for (subscribe_id, subscriber) in &mut playout.subscribers {
            let _ = subscriber.subscribe_update(
                *subscribe_id,
                SubscribeFilter::AbsoluteStart(SubscribePair {
                    group: group_id,
                    object: object_id + 1,
                }),
                127,
            );
        }

        Ok(())
    }

    pub fn add_subscriber(
        self: &mut Self,
        key: String,
        subscribe_id: u64,
        subscriber: Subscriber,
    ) {
        if let Some(playout) = self.tracks.get_mut(&key) {
            playout.subscribers.push((subscribe_id, subscriber));
        } else {
            self.tracks.insert(key, TrackPlayoutStatus{ last_id: None, subscribers: vec![(subscribe_id, subscriber)] });
        }
    }
}
