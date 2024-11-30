use crate::types::{certificate::Certificate, ReceivedObject, Round};
use proc_macros::Spawn;
use tokio::sync::{broadcast, mpsc, watch};
use tokio_util::sync::CancellationToken;

#[derive(Spawn)]
pub(crate) struct DagProcessor {
    peers_certificates_rx: broadcast::Receiver<ReceivedObject<Certificate>>,
    certificates_rx: mpsc::Receiver<Certificate>,
    rounds_tx: watch::Sender<(Round, Vec<Certificate>)>,
}

impl DagProcessor {
    pub async fn run(self) -> Result<(), anyhow::Error> {
        todo!()
    }
}
