use futures_util::future::try_join_all;
use tokio::{
    sync::mpsc::{Receiver, Sender},
    task::JoinHandle,
};
use tracing::Instrument;

use crate::types::TxBatch;

pub(crate) struct BatchBroadcaster {
    batches_rx: Receiver<TxBatch>,
    network_tx: Sender<Vec<u8>>,
}

impl BatchBroadcaster {
    pub fn new(batches_rx: Receiver<TxBatch>, network_tx: Sender<Vec<u8>>) -> Self {
        Self {
            batches_rx,
            network_tx,
        }
    }

    pub fn spawn(self) -> JoinHandle<()> {
        tokio::spawn(
            self.run()
                .instrument(tracing::info_span!("batch_broadcaster")),
        )
    }

    pub async fn run(self) {
        let Self {
            batches_rx,
            network_tx,
        } = self;

        let tasks = vec![tokio::spawn(broadcast_task(batches_rx, network_tx))];

        if let Err(e) = try_join_all(tasks).await {
            tracing::error!("Error in BatchBroadcaster: {:?}", e);
        }
    }
}

#[tracing::instrument(skip_all)]
async fn broadcast_task(mut rx: Receiver<TxBatch>, network_tx: Sender<Vec<u8>>) {
    while let Some(batch) = rx.recv().await {
        tracing::info!("Broadcasting batch: {:?}", batch);
        let encoded_batch = bincode::serialize(&batch).unwrap();
        network_tx.send(encoded_batch).await.unwrap();
    }
}
