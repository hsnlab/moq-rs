use std::collections::HashMap;
use moq_transport::{message::SubscribePair, session::{SubscribeFilter, Subscriber}};
use moq_transport::serve::{SubgroupInfo, SubgroupObjectReader};
use tokio::io::{AsyncWrite, AsyncWriteExt};

pub trait SmartWriter {
    fn create_key(&mut self, group: &SubgroupInfo) -> String;

    fn write_object(
        &mut self,
        object: SubgroupObjectReader,
        buf: &Vec<u8>,
    ) -> impl std::future::Future<Output = Result<(), std::io::Error>> + Send;

    fn write_group_object(
        &mut self,
        key: String,
        object: SubgroupObjectReader,
        buf: &Vec<u8>,
    ) -> impl std::future::Future<Output = Result<(), std::io::Error>> + Send;
    fn add_subscriber(
        &mut self,
        subscribe_id: u64,
        subscriber: Subscriber,
    );
}

pub struct SmartOut<O: AsyncWrite + Send + Unpin + 'static> {
    out: O,
    largest_ids: HashMap<String, (u64, u64)>, // For each unique track
    subscribers: Vec<(u64, Subscriber)>,
}

impl<O: AsyncWrite + Send + Unpin + 'static> SmartOut<O> {
    pub fn new(out: O) -> Self {
        Self {
            out,
            largest_ids: HashMap::new(),
            subscribers: Vec::new(),
        }
    }

}

impl<O: AsyncWrite + Send + Unpin + 'static> SmartWriter for SmartOut<O> {
    fn create_key(&mut self, group: &SubgroupInfo) -> String {
        group.namespace.to_utf8_path() + ":" + &group.name
    }

    async fn write_object(
        &mut self,
        object: SubgroupObjectReader,
        buf: &Vec<u8>,
    ) -> Result<(), std::io::Error> {
        let key = self.create_key(&object.group);
        return self.write_group_object(key, object, buf).await;
    }

    async fn write_group_object(
        &mut self,
        key: String,
        object: SubgroupObjectReader,
        buf: &Vec<u8>,
    ) -> Result<(), std::io::Error> {
        let group_id = object.group_id;
        let object_id = object.object_id;

        let id = &(group_id, object_id);
        if let Some(largest_id) = self.largest_ids.get(&key) {
            if id < largest_id { // Already seen this pair
                return Ok(());
            }
        }

        self.out.write_all(&buf).await?;
        self.largest_ids.insert(key, *id);

        for (subscribe_id, subscriber) in &mut self.subscribers {
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

    fn add_subscriber(
        self: &mut Self,
        subscribe_id: u64,
        subscriber: Subscriber,
    ) {
        self.subscribers.push((subscribe_id, subscriber));
    }
}
