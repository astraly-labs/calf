use proc_macros::Spawn;
use tokio::sync::{broadcast, mpsc};
use tokio_util::sync::CancellationToken;

use crate::types::{
    batch::Batch,
    network::{NetworkRequest, RequestPayload},
    transaction::Transaction,
};

#[derive(Spawn)]
pub(crate) struct BatchBroadcaster {
    batches_rx: broadcast::Receiver<Batch<Transaction>>,
    network_tx: mpsc::Sender<NetworkRequest>,
}

impl BatchBroadcaster {
    pub async fn run(mut self) -> anyhow::Result<()> {
        while let Ok(batch) = self.batches_rx.recv().await {
            tracing::info!("Broadcasting batch: {:?}", batch);
            self.network_tx
                .send(NetworkRequest::BroadcastCounterparts(
                    RequestPayload::Batch(batch),
                ))
                .await?;
        }
        Ok(())
    }
}
