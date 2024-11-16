use async_channel::Receiver;
use tokio::task::JoinSet;

use crate::types::TxBatch;

#[derive(Debug)]
pub(crate) struct BatchBroadcaster {
    batches_rx: Receiver<TxBatch>,
}

impl BatchBroadcaster {
    pub fn new(
        batches_rx: Receiver<TxBatch>,
    ) -> Self {
        Self {
            batches_rx,
        }
    }

    pub async fn run_forever(&mut self) -> anyhow::Result<()> {
        loop {

        }
    }

    async fn broadcast_batch(&mut self) -> anyhow::Result<()> {
        todo!()
    }
}
