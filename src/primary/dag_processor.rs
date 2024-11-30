use crate::types::{certificate::Certificate, dag::Dag, ReceivedObject, Round};
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
    /// advance rounds, verify certificates, start round 1.
    pub async fn run(mut self) -> Result<(), anyhow::Error> {
        let mut dag = Dag::new(Certificate::genesis())?;
        loop {
            tokio::select! {
                Some(certificate) = self.certificates_rx.recv() => {
                    match dag.insert_certificate(certificate) {
                        Ok(()) => {
                            tracing::info!("certificate inserted in the DAG");
                        },
                        Err(error) => {
                            tracing::warn!("error inserting certificate: {:?}", error);
                        }
                    }
                }
                Ok(certificate) = self.peers_certificates_rx.recv() => {
                    match dag.insert_certificate(certificate.object) {
                        Ok(()) => {
                            tracing::info!("certificate from {} inserted in the DAG", certificate.sender);
                        },
                        Err(error) => {
                            tracing::warn!("error inserting certificate from: {}, {:?}", certificate.sender, error);
                        }
                    }
                }
                else => break,
            }
        }
        Ok(())
    }
}
