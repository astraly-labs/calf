use async_channel::Receiver;
use tokio::task::JoinSet;

use crate::types::services::Service;
use crate::types::TxBatch;

#[derive(Debug)]
pub(crate) struct BatchBroadcaster {
    batches_rx: Receiver<TxBatch>,
}

#[async_trait::async_trait]
impl Service for BatchBroadcaster {
    async fn start(&mut self, join_set: &mut JoinSet<anyhow::Result<()>>) -> anyhow::Result<()> {
        let batches_rx = self.batches_rx.clone();

        join_set.spawn(async move {
            let mut service = BatchBroadcaster {
                batches_rx,
            };
            service.run_forever().await?;
            Ok(())
        });
        Ok(())
    }
}

impl BatchBroadcaster {
    pub fn new(
        batches_rx: Receiver<TxBatch>,
    ) -> Self {
        Self {
            batches_rx,
        }
    }

    async fn run_forever(&mut self) -> anyhow::Result<()> {
        loop {

        }
    }

    async fn broadcast_batch(&mut self) -> anyhow::Result<()> {
        todo!()
    }
}
