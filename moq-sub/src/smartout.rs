use std::collections::HashMap;

use tokio::io::{AsyncWrite, AsyncWriteExt};

pub trait SmartWriter {
    fn last_group_id(&self, id: &str) -> Option<u64>;
    fn write_group(
        &mut self,
        id: String,
        group_id: u64,
        buf: &Vec<u8>,
    ) -> impl std::future::Future<Output = Result<(), std::io::Error>> + Send;
}

pub struct SmartOut<O: AsyncWrite + Send + Unpin + 'static> {
    out: O,
    last_ids: HashMap<String, u64>,
}

impl<O: AsyncWrite + Send + Unpin + 'static> SmartOut<O> {
    pub fn new(out: O) -> Self {
        Self {
            out,
            last_ids: HashMap::new(),
        }
    }
}

unsafe impl<O: AsyncWrite + Send + Unpin + 'static> Send for SmartOut<O> {}

impl<O: AsyncWrite + Send + Unpin + 'static> SmartWriter for SmartOut<O> {
    fn last_group_id(&self, id: &str) -> Option<u64> {
        match self.last_ids.get(id) {
            Some(x) => Some(*x),
            None => None,
        }
    }

    async fn write_group(
        &mut self,
        id: String,
        group_id: u64,
        buf: &Vec<u8>,
    ) -> Result<(), std::io::Error> {
        self.out.write_all(&buf).await?;
        self.last_ids.insert(id, group_id);

        Ok(())
    }
}
