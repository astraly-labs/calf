use std::{collections::HashSet, sync::Arc};

use crate::{
    db::{self, Db},
    settings::parser::Committee,
    types::{
        block_header::{BlockHeader, HeaderId}, certificate::{Certificate, CertificateId}, dag::{Dag, DagError}, network::ReceivedObject, sync::OrphanCertificate, traits::AsHex, Round
    },
};
use proc_macros::Spawn;
use tokio::sync::{broadcast, mpsc, watch};
use tokio_util::sync::CancellationToken;

const GENESIS_SEED: [u8; 32] = [0; 32];

#[derive(Spawn)]
pub(crate) struct DagProcessor {
    peers_certificates_rx: broadcast::Receiver<ReceivedObject<Certificate>>,
    certificates_rx: mpsc::Receiver<Certificate>,
    certificates_tx: mpsc::Sender<ReceivedObject<Certificate>>,
    oprhans_tx: mpsc::Sender<ReceivedObject<OrphanCertificate>>,
    missing_headers_tx: mpsc::Sender<ReceivedObject<HeaderId>>,
    rounds_tx: watch::Sender<(Round, HashSet<Certificate>)>,
    committee: Committee,
    db: Arc<Db>,
    _reset_trigger_tx: mpsc::Sender<()>,
}

impl DagProcessor {
    pub async fn run(mut self) -> Result<(), anyhow::Error> {
        let genesis = Certificate::genesis(GENESIS_SEED);
        let mut dag = Dag::new_with_root(0, genesis.clone());
        self.rounds_tx
            .send((dag.height() + 1, HashSet::from_iter([genesis].into_iter())))?;
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
                    match certificate.object.verify_votes(&self.committee) {
                        Ok(()) => {
                            tracing::info!("🔍 valid votes in certificate from {}", certificate.sender);
                        },
                        Err(error) => {
                            tracing::warn!("🔍 invalid votes in certificate from {}: {}", certificate.sender, error);
                            continue;
                        }
                    }
                    if let Some(id) = certificate.object.header() {
                        if !check_header_storage(&id, &self.db) {
                            self.missing_headers_tx.send(ReceivedObject::new(id, certificate.sender)).await?;
                            tracing::info!("missing header referenced in {} certificate, sending to the tracker", certificate.object.id().0.as_hex_string());
                        }
                    }
                    match dag.check_parents(&certificate.object.clone().into()) {
                        Ok(()) => {
                            tracing::info!("💾 certificate from {} inserted in the DAG", certificate.sender);
                            let _ = dag.insert(certificate.object.clone().into());
                            self.db.insert(db::Column::Certificates, &certificate.object.id_as_hex(), &certificate.object)?;
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

            let round_certificates_number = dag.layer_size(dag.height());
            if round_certificates_number >= self.committee.quorum_threshold() as usize {
                let certificates: HashSet<Certificate> =
                    dag.layer_data(dag.height()).into_iter().collect();
                tracing::info!(
                    "🎉 round {} completed with {} certificates",
                    dag.height(),
                    round_certificates_number
                );
                self.rounds_tx.send((dag.height() + 1, certificates))?;
            }
        }
        Ok(())
    }
}

fn check_header_storage(id: &HeaderId, db: &Db) -> bool {
    if let Ok(Some(_)) = db.get::<BlockHeader>(db::Column::Headers, &id.0.as_hex_string()) {
        true
    } else {
        false
    }
}
