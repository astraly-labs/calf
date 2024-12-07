use crate::types::traits::AsHex;
use derive_more::derive::Constructor;
use proc_macros::Spawn;
use tokio::sync::{broadcast, mpsc, watch};
use tokio_util::sync::CancellationToken;

use crate::types::{
    certificate::{Certificate, CertificateId},
    network::ReceivedObject,
};

use super::FetcherCommand;

#[derive(Spawn)]
pub struct SyncTracker {
    // The receiver for the certificates from the peers, our certificates cannot be orphans because they certify a header that has been built with the certificates from the local DAG
    certificates_rx: broadcast::Receiver<ReceivedObject<Certificate>>,
    // The receiver for the orphan certificates, already verified and inserted in the DAG: the role of the synchronizer will be to fetch all the missing parents for an orphan certificate and track them efficiently
    orphans_rx: mpsc::Receiver<OrphanCertificate>,
    // A watch channel to expose all orpahn certificates, to avoid to iterate avec all the DAG elements
    orphans_tx: watch::Sender<CertificateId>,
    fetcher_commands_tx: mpsc::Sender<FetcherCommand>,
    // When the DAG that we are trying to synchronize is outdated: TODO: continue anyway ? elsewhere ?
    reset_trigger: mpsc::Receiver<()>,
}

//TODO: store orphans by round for a more efficient search ?
#[derive(Debug, Constructor)]
struct OrphanCertificate {
    id: CertificateId,
    missing_parents: Vec<CertificateId>,
}

impl SyncTracker {
    pub async fn run(mut self) -> anyhow::Result<()> {
        tracing::info!("🔄 Starting the synchronizer");
        let mut orphans: Vec<OrphanCertificate> = vec![];
        loop {
            publish_orphans(&orphans, &self.orphans_tx)?;
            tokio::select! {
                Some(orphan) = self.orphans_rx.recv() => {
                    tracing::info!("📡 Received orphan certificate {}", orphan.id.as_hex_string());
                    self.fetcher_commands_tx.send(FetcherCommand::Push()).await?;
                    orphans.push(orphan);
                }
                Ok(certificate) = self.certificates_rx.recv() => {
                    let id = certificate.object.id();
                    orphans.iter_mut().for_each(|elm| {
                        // If a received certificate is a parent of an orphan certificate, we remove it from the missing parents of the orphan certificate
                        if elm.missing_parents.contains(&id) {
                            elm.missing_parents.retain(|parent| parent != &id);
                            if elm.missing_parents.is_empty() {
                                tracing::info!("📡 Certificate {} direct parents has been retrieved", id.as_hex_string());
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
    orphans_tx: &watch::Sender<CertificateId>,
) -> anyhow::Result<()> {
    for orphan in orphans {
        orphans_tx.send(orphan.id)?;
    }
    Ok(())
}
