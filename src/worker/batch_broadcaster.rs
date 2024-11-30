use proc_macros::Spawn;
use tokio::{
    sync::{broadcast, mpsc},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

use crate::types::{NetworkRequest, RequestPayload, TxBatch};

#[derive(Spawn)]
pub(crate) struct BatchBroadcaster {
    batches_rx: broadcast::Receiver<TxBatch>,
    network_tx: mpsc::Sender<NetworkRequest>,
}

impl BatchBroadcaster {
    pub async fn run(mut self) -> anyhow::Result<()> {
        while let Ok(batch) = self.batches_rx.recv().await {
            tracing::info!("Broadcasting batch: {:?}", batch);
            self.network_tx
                .send(NetworkRequest::Broadcast(RequestPayload::Batch(batch)))
                .await?;
        }
        Ok(())
    }
}
