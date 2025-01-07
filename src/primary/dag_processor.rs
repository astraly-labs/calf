use std::{collections::HashSet, sync::Arc};

#[cfg(feature = "dag_log")]
use std::{fs::OpenOptions, io::Write};

use crate::{
    db::{self, Db},
    settings::parser::Committee,
    types::{
        certificate::{Certificate, CertificateId},
        dag::{Dag, DagError},
        network::ReceivedObject,
        sync::OrphanCertificate,
        Round,
    },
};
use proc_macros::Spawn;

#[cfg(feature = "dag_log")]
use serde_json::json;

use tokio::sync::{broadcast, mpsc, watch};
use tokio_util::sync::CancellationToken;

const GENESIS_SEED: [u8; 32] = [0; 32];
#[cfg(feature = "dag_log")]
const DAG_OUTPUT_FILE: &str = "output.dag";

#[derive(Spawn)]
pub(crate) struct DagProcessor {
    peers_certificates_rx: broadcast::Receiver<ReceivedObject<Certificate>>,
    certificates_rx: mpsc::Receiver<Certificate>,
    certificates_tx: mpsc::Sender<ReceivedObject<Certificate>>,
    oprhans_tx: mpsc::Sender<ReceivedObject<OrphanCertificate>>,
    rounds_tx: watch::Sender<(Round, HashSet<Certificate>)>,
    committee: Committee,
    db: Arc<Db>,
    _reset_trigger_tx: mpsc::Sender<()>,
}

impl DagProcessor {
    pub async fn run(mut self) -> Result<(), anyhow::Error> {
        let genesis = Certificate::genesis(GENESIS_SEED);
        self.db
            .insert(db::Column::Certificates, &genesis.id_as_hex(), &genesis)?;
        let mut dag = Dag::new_with_root(0, genesis.clone());
        let mut current_round = dag.height() + 1;
        self.rounds_tx
            .send((current_round, HashSet::from_iter([genesis].into_iter())))?;

        // Create/truncate the DAG output file
        #[cfg(feature = "dag_log")]
        let _ = std::fs::File::create(DAG_OUTPUT_FILE)?;

        loop {
            tokio::select! {
                Some(certificate) = self.certificates_rx.recv() => {
                    match dag.insert_checked(certificate.clone().into()) {
                        Ok(()) => {
                            tracing::info!("💾 current header certificate inserted in the DAG");
                            self.db.insert(db::Column::Certificates, &certificate.id_as_hex(), &certificate)?;
                            #[cfg(feature = "dag_log")]
                            self.write_dag_state(&dag, current_round)?;
                        },
                        Err(error) => {
                            tracing::warn!("error inserting certificate: {}", error);
                        }
                    }
                }
                Ok(certificate) = self.peers_certificates_rx.recv() => {
                    tracing::info!("📡 received new certificate from {}", certificate.sender);
                    match certificate.object.verify_votes(&self.committee) {
                        Ok(()) => {
                            tracing::info!("🔍 valid votes in certificate from {}", certificate.sender);
                        },
                        Err(error) => {
                            tracing::warn!("🔍 invalid votes in certificate from {}: {}", certificate.sender, error);
                            continue;
                        }
                    }
                    match dag.check_parents(&certificate.object.clone().into()) {
                        Ok(()) => {
                            tracing::info!("💾 certificate from {} inserted in the DAG", certificate.sender);
                            let _ = dag.insert(certificate.object.clone().into());
                            self.db.insert(db::Column::Certificates, &certificate.object.id_as_hex(), &certificate.object)?;
                            #[cfg(feature = "dag_log")]
                            self.write_dag_state(&dag, current_round)?;
                        },
                        Err(error) => {
                            match error {
                                DagError::MissingParents(parents) => {
                                    tracing::warn!("🔍 missing parents for certificate from {}", certificate.sender);
                                    let missing_parents: Vec<CertificateId> = parents.into_iter().flat_map(|id| id.try_into()).collect();
                                    let orphan = OrphanCertificate::new(certificate.object.id(), missing_parents);
                                    self.oprhans_tx.send(ReceivedObject::new(orphan, certificate.sender)).await?;
                                    tracing::info!("📡 orphan certificate from {} sent to the sync tracker", certificate.sender);
                                },
                            }
                        }
                    }
                    // send the certificate to the tracker
                    self.certificates_tx.send(certificate).await?;
                }
                else => break,
            }

            let round_certificates_number = dag.layer_size(current_round);
            if round_certificates_number >= self.committee.quorum_threshold() as usize {
                let certificates: HashSet<Certificate> =
                    dag.layer_data(current_round).into_iter().collect();
                tracing::info!(
                    "🎉 round {} completed with {} certificates",
                    current_round,
                    round_certificates_number
                );
                current_round += 1;
                self.rounds_tx.send((current_round, certificates))?;

                #[cfg(feature = "dag_log")]
                self.write_dag_state(&dag, current_round)?;
            }
        }
        Ok(())
    }

    #[cfg(feature = "dag_log")]
    fn write_dag_state(
        &self,
        dag: &Dag<Certificate>,
        current_round: Round,
    ) -> Result<(), anyhow::Error> {
        let mut vertices = Vec::new();
        let mut edges = Vec::new();

        // Collect vertices and edges for each round up to current_round
        for round in 0..=current_round {
            for vertex in dag.layer_vertices(round) {
                let vertex_data = json!({
                    "id": vertex.id(),
                    "round": vertex.layer(),
                    "author": hex::encode(vertex.data().author().unwrap_or([0; 32])),
                    "timestamp": chrono::Utc::now().timestamp_millis()
                });
                vertices.push(vertex_data);

                // Add edges from this vertex to its parents
                for parent_id in vertex.parents() {
                    edges.push(json!({
                        "from": parent_id,
                        "to": vertex.id()
                    }));
                }
            }
        }

        let dag_state = json!({
            "current_round": current_round,
            "vertices": vertices,
            "edges": edges,
            "timestamp": chrono::Utc::now().timestamp_millis()
        });

        // Write to file with append mode
        let mut file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(DAG_OUTPUT_FILE)?;

        writeln!(file, "{}", dag_state)?;
        Ok(())
    }
}
