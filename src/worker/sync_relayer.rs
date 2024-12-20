use crate::{
    network::worker::WorkerPeers,
    synchronizer::traits::{Fetch, Sourced},
    types::{
        batch::BatchId,
        network::{ReceivedObject, SyncRequest},
    },
};
use proc_macros::Spawn;
use std::{collections::HashSet, sync::Arc};
use tokio::sync::{broadcast, mpsc, RwLock};
use tokio_util::sync::CancellationToken;

#[derive(Spawn)]
pub struct SyncRelayer {
    fetcher_commands_tx: mpsc::Sender<Box<dyn Fetch + Send + Sync>>,
    sync_requests_rx: broadcast::Receiver<ReceivedObject<SyncRequest>>,
    peers: Arc<RwLock<WorkerPeers>>,
}

impl SyncRelayer {
    pub async fn run(mut self) -> anyhow::Result<()> {
        tracing::info!("SyncRelayer started");
        loop {
            tokio::select! {
                Ok(ReceivedObject { object: SyncRequest::SyncDigests(digests), ..}) = self.sync_requests_rx.recv() => {
                    tracing::info!("Received SyncDigests request");
                    let batch_ids: HashSet<BatchId> = digests.into_iter().map(|elm| elm.into()).collect();
                    self.fetcher_commands_tx.send(batch_ids.requested_with_source(self.peers.clone())).await?;
                    tracing::info!("Requested batch id sent to the fetcher");
                }
            }
        }
    }
}
