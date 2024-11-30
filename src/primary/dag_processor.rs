use std::time::Duration;

use crate::{
    settings::parser::Committee,
    types::{certificate::Certificate, dag::Dag, ReceivedObject, Round},
};
use proc_macros::Spawn;
use tokio::sync::{broadcast, mpsc, watch};
use tokio_util::sync::CancellationToken;

#[derive(Spawn)]
pub(crate) struct DagProcessor {
    peers_certificates_rx: broadcast::Receiver<ReceivedObject<Certificate>>,
    certificates_rx: mpsc::Receiver<Certificate>,
    rounds_tx: watch::Sender<(Round, Vec<Certificate>)>,
    committee: Committee,
}

impl DagProcessor {
    /// TODO: test, verify cerificates
    pub async fn run(mut self) -> Result<(), anyhow::Error> {
        let genesis = Certificate::genesis();
        let mut dag = Dag::new(genesis.clone())?;
        let mut round = 1;
        tokio::time::sleep(Duration::from_secs(20)).await;
        self.rounds_tx.send((1, vec![Certificate::genesis()]))?;
        loop {
            tokio::select! {
                Some(certificate) = self.certificates_rx.recv() => {
                    match dag.insert_certificate(certificate) {
                        Ok(()) => {
                            tracing::info!("💾 current header certificate inserted in the DAG");
                        },
                        Err(error) => {
                            tracing::warn!("error inserting certificate: {:?}", error);
                        }
                    }
                }
                Ok(certificate) = self.peers_certificates_rx.recv() => {
                    tracing::info!("📡 received new certificate from {}", certificate.sender);
                    match dag.insert_certificate(certificate.object) {
                        Ok(()) => {
                            tracing::info!("💾 certificate from {} inserted in the DAG", certificate.sender);
                        },
                        Err(error) => {
                            tracing::warn!("error inserting certificate from: {}, {:?}", certificate.sender, error);
                        }
                    }
                }
                else => break,
            }
            let previous_round_certificates =
                dag.count_all(|certificate| certificate.round == round);
            if previous_round_certificates >= self.committee.quorum_threshold() as usize {
                tracing::info!(
                    "⚡ quorum reached for current round, advancing to round {}",
                    round + 1
                );
                self.rounds_tx.send((
                    round + 1,
                    dag.get_all(|certificate| certificate.round == round)
                        .into_iter()
                        .cloned()
                        .collect(),
                ))?;
                round += 1;
            }
        }
        Ok(())
    }
}
