use std::{collections::HashSet, sync::Arc};

use crate::{
    db::Db,
    settings::parser::{Committee, FileLoader},
    types::{certificate::Certificate, dag::Dag, network::ReceivedObject, Round},
};
use proc_macros::Spawn;
use tokio::sync::{broadcast, mpsc, watch};
use tokio_util::sync::CancellationToken;

#[derive(Spawn)]
pub(crate) struct DagProcessor {
    peers_certificates_rx: broadcast::Receiver<ReceivedObject<Certificate>>,
    certificates_rx: mpsc::Receiver<Certificate>,
    rounds_tx: watch::Sender<(Round, HashSet<Certificate>)>,
    committee: Committee,
    db: Arc<Db>,
}

impl DagProcessor {
    pub async fn run(mut self) -> Result<(), anyhow::Error> {
        let genesis = Certificate::genesis([0; 32]);
        let mut dag = Dag::new(genesis.clone())?;
        let mut round = 1;
        self.rounds_tx
            .send((round, HashSet::from_iter([genesis].into_iter())))?;
        loop {
            tokio::select! {
                Some(certificate) = self.certificates_rx.recv() => {
                    match dag.insert_certificate(certificate) {
                        Ok(()) => {
                            tracing::info!("💾 current header certificate inserted in the DAG");
                        },
                        Err(error) => {
                            tracing::warn!("error inserting certificate: {}", error);
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
                            tracing::warn!("error inserting certificate from: {}, {}", certificate.sender, error);
                        }
                    }
                }
                else => break,
            }
            if dag.round_certificates_number(round) >= self.committee.quorum_threshold() as usize {
                let certificates = dag.round_certificates(round);
                tracing::info!(
                    "🎉 round {} completed with {} certificates",
                    round,
                    certificates.len()
                );

                // TODO: gc round should be decided by consensus engine, for now we hardcode it
                // Garbage collect rounds before current - 10 to maintain some history
                if round > 10 {
                    dag.update_gc_round(round - 10);
                }

                // TODO: add under feature flag
                dag.simplified()
                    .write_to_file(format!("dag_log/round_{round}.json"))?;
                round += 1;
                self.rounds_tx.send((round, certificates))?;
            }
        }
        Ok(())
    }
}
