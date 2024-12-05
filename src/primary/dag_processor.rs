use std::{collections::HashSet, sync::Arc};

use crate::{
    db::{self, Db},
    settings::parser::Committee,
    types::{certificate::Certificate, dag::Dag, network::ReceivedObject, Round},
};
use proc_macros::Spawn;
use tokio::sync::{broadcast, mpsc, watch};
use tokio_util::sync::CancellationToken;

const GENESIS_SEED: [u8; 32] = [0; 32];

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
        let genesis = Certificate::genesis(GENESIS_SEED);
        let mut dag = Dag::new_with_root(0, genesis.clone());
        self.rounds_tx.send((
            dag.height() as u64 + 1,
            HashSet::from_iter([genesis].into_iter()),
        ))?;
        loop {
            tokio::select! {
                Some(certificate) = self.certificates_rx.recv() => {
                    match dag.insert_checked(certificate.clone().into()) {
                        Ok(()) => {
                            tracing::info!("💾 current header certificate inserted in the DAG");
                            self.db.insert(db::Column::Certificates, &certificate.id_as_hex(), &certificate)?;
                        },
                        Err(error) => {
                            tracing::warn!("error inserting certificate: {}", error);
                        }
                    }
                }
                Ok(certificate) = self.peers_certificates_rx.recv() => {
                    tracing::info!("📡 received new certificate from {}", certificate.sender);
                    match dag.insert_checked(certificate.object.clone().into()) {
                        Ok(()) => {
                            tracing::info!("💾 certificate from {} inserted in the DAG", certificate.sender);
                            self.db.insert(db::Column::Certificates, &certificate.object.id_as_hex(), &certificate.object)?;
                        },
                        Err(error) => {
                            tracing::warn!("error inserting certificate from: {}, {}", certificate.sender, error);
                        }
                    }
                }
                else => break,
            }
            let round_certificates_number = dag.layer_size(dag.height());
            if round_certificates_number >= self.committee.quorum_threshold() as usize {
                let certificates: HashSet<Certificate> =
                    dag.layer_data(dag.height()).into_iter().collect();
                tracing::info!(
                    "🎉 round {} completed with {} certificates",
                    dag.height(),
                    round_certificates_number
                );
                self.rounds_tx
                    .send((dag.height() as u64 + 1, certificates))?;
            }
        }
        Ok(())
    }
}
