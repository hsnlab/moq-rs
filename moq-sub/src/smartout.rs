use std::collections::HashMap;

use moq_transport::{message::SubscribePair, session::{SubscribeFilter, Subscriber}};
use tokio::io::{AsyncWrite, AsyncWriteExt};

pub trait SmartWriter {
    fn last_object_id(&self, id: &str) -> Option<u64>;
    fn write_object(
        &mut self,
        id: String,
        object_id: u64,
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
    last_ids: HashMap<String, u64>,
    subscribers: Vec<(u64, Subscriber)>,
}

impl<O: AsyncWrite + Send + Unpin + 'static> SmartOut<O> {
    pub fn new(out: O) -> Self {
        Self {
            out,
            last_ids: HashMap::new(),
            subscribers: Vec::new(),
        }
    }
}

unsafe impl<O: AsyncWrite + Send + Unpin + 'static> Send for SmartOut<O> {}

impl<O: AsyncWrite + Send + Unpin + 'static> SmartWriter for SmartOut<O> {
    fn last_object_id(&self, id: &str) -> Option<u64> {
        match self.last_ids.get(id) {
            Some(x) => Some(*x),
            None => None,
        }
    }

    async fn write_object(
        &mut self,
        id: String,
        object_id: u64,
        buf: &Vec<u8>,
    ) -> Result<(), std::io::Error> {
        self.out.write_all(&buf).await?;
        self.last_ids.insert(id.clone(), object_id);

        // little hack until Felician's patch is merged
        let group_id: u64 = id
            .split(':')
            .last()
            .expect("group_id was not provided in the id")
            .parse()
            .expect("group_id could not be converted to u64");
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
