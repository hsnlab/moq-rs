use tokio::io::{AsyncWrite, AsyncWriteExt};

pub trait SmartWriter {
    fn last_group_id(&self) -> u64;
    async fn write_group(&mut self, group_id: u64, buf: &Vec<u8>) -> Result<(), std::io::Error>;
}

pub struct SmartOut<O: AsyncWrite + Send + Unpin + 'static> {
    out: O,
    group_id: u64,
}

impl<O: AsyncWrite + Send + Unpin + 'static> SmartOut<O> {
    pub fn new(out: O) -> Self {
        Self { out, group_id: 0 }
    }
}

unsafe impl<O: AsyncWrite + Send + Unpin + 'static> Send for SmartOut<O> {}

impl<O: AsyncWrite + Send + Unpin + 'static> SmartWriter for SmartOut<O> {
    fn last_group_id(&self) -> u64 {
        self.group_id
    }

    async fn write_group(&mut self, group_id: u64, buf: &Vec<u8>) -> Result<(), std::io::Error> {
        self.out.write_all(&buf).await?;
        self.group_id = group_id;
        log::debug!("🤡: group_id={group_id}");

        Ok(())
    }
}
