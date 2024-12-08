use std::collections::HashSet;

use crate::{
    synchronizer::traits::{Fetch, Sourced},
    types::{
        batch::BatchId,
        block_header::{BlockHeader, HeaderId},
        traits::AsHex,
    },
};
use derive_more::derive::Constructor;
use proc_macros::Spawn;
use tokio::sync::{broadcast, mpsc, watch};
use tokio_util::sync::CancellationToken;

use crate::types::{
    certificate::{Certificate, CertificateId},
    network::ReceivedObject,
};

#[derive(Spawn)]
pub struct SyncTracker {
    certificates_rx: broadcast::Receiver<ReceivedObject<Certificate>>,
    orphans_rx: mpsc::Receiver<ReceivedObject<OrphanCertificate>>,
    orphans_tx: watch::Sender<HashSet<CertificateId>>,
    fetcher_commands_tx: mpsc::Sender<Box<dyn Fetch + Send + Sync + 'static>>,
    reset_trigger: mpsc::Receiver<()>,
    incomplete_headers_rx: mpsc::Receiver<ReceivedObject<IncompleteHeader>>,
    missing_headers_rx: mpsc::Receiver<ReceivedObject<HeaderId>>,
}

#[derive(Debug, Constructor)]
pub struct OrphanCertificate {
    pub id: CertificateId,
    pub missing_parents: Vec<CertificateId>,
}

pub struct IncompleteHeader {
    pub missing_certificates: HashSet<CertificateId>,
    pub missing_batches: HashSet<BatchId>,
    pub header: BlockHeader,
}

impl SyncTracker {
    pub async fn run(mut self) -> anyhow::Result<()> {
        tracing::info!("🔄 Starting the synchronizer");
        let mut orphans: Vec<OrphanCertificate> = vec![];
        loop {
            publish_orphans(&orphans, &self.orphans_tx)?;
            tokio::select! {
                Some(orphan) = self.orphans_rx.recv() => {
                    tracing::info!("📡 Received orphan certificate {}", orphan.object.id.0.as_hex_string());
                    let missing_parents: HashSet<CertificateId> = orphan
                        .object
                        .missing_parents
                        .iter()
                        .filter(|parent| {
                            let id = parent.0;
                            !orphans.iter().any(|elm| elm.id.0 == id)
                        })
                        .cloned()
                        .collect();

                    self.fetcher_commands_tx.send(missing_parents.requested_with_source(orphan.sender.clone())).await?;
                    orphans.push(orphan.object);
                }
                Ok(certificate) = self.certificates_rx.recv() => {
                    let id = certificate.object.id();
                    orphans.iter_mut().for_each(|elm| {
                        // If a received certificate is a parent of an orphan certificate, we remove it from the missing parents of the orphan certificate
                        if elm.missing_parents.contains(&id) {
                            elm.missing_parents.retain(|parent| parent != &id);
                            if elm.missing_parents.is_empty() {
                                tracing::info!("📡 Certificate {} direct parents has been retrieved", id.0.as_hex_string());
                            }
                        }
                    });
                    // We can remove the orphan certificate from the list if all its parents have been retrieved. WARNING(TODO): really ?
                    orphans.retain(|elm| !elm.missing_parents.is_empty());
                }
                Some(_) = self.reset_trigger.recv() => {
                    orphans.clear();
                    tracing::info!("🔄 Resetting the synchronizer");
                }
                else => break Ok(()),
            }
        }
    }
}

fn publish_orphans(
    orphans: &Vec<OrphanCertificate>,
    orphans_tx: &watch::Sender<HashSet<CertificateId>>,
) -> anyhow::Result<()> {
    orphans_tx.send(orphans.iter().map(|elm| elm.id).collect())?;
    Ok(())
}
