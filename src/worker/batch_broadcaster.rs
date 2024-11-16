use futures_util::future::try_join_all;
use tokio::{sync::mpsc::Receiver, task::JoinHandle};
use tracing::Instrument;

use crate::types::TxBatch;

#[derive(Debug)]
pub(crate) struct BatchBroadcaster {
    batches_rx: Receiver<TxBatch>,
}

impl BatchBroadcaster {
    pub fn new(batches_rx: Receiver<TxBatch>) -> Self {
        Self { batches_rx }
    }

    pub fn spawn(self) -> JoinHandle<()> {
        tokio::spawn(
            self.run()
                .instrument(tracing::info_span!("batch_broadcaster")),
        )
    }

    pub async fn run(self) {
        let Self { batches_rx } = self;

        let tasks = vec![tokio::spawn(broadcast_task(batches_rx))];

        if let Err(e) = try_join_all(tasks).await {
            tracing::error!("Error in BatchBroadcaster: {:?}", e);
        }
    }
}

#[tracing::instrument(skip_all)]
async fn broadcast_task(mut rx: Receiver<TxBatch>) {
    while let Some(batch) = rx.recv().await {
        tracing::info!("Broadcasting batch: {:?}", batch);
    }
}
