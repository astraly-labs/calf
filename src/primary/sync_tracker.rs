use std::{collections::HashSet, sync::Arc};

use crate::{
    db::{self, Db},
    network::{primary::PrimaryConnector, Connect},
    synchronizer::traits::{Fetch, Sourced},
    types::{
        batch::BatchId,
        block_header::{BlockHeader, HeaderId},
        network::RequestPayload,
        traits::AsHex,
        Digest,
    },
};
use derive_more::derive::Constructor;
use libp2p::PeerId;
use proc_macros::Spawn;
use tokio::sync::{broadcast, mpsc, watch};
use tokio_util::sync::CancellationToken;

use crate::types::{
    certificate::{Certificate, CertificateId},
    network::ReceivedObject,
};

#[derive(Spawn)]
pub struct SyncTracker {
    // --v received data v--
    certificates_rx: mpsc::Receiver<ReceivedObject<Certificate>>,
    headers_rx: broadcast::Receiver<ReceivedObject<BlockHeader>>,
    digests_rx: broadcast::Receiver<ReceivedObject<BatchId>>,
    // --v data to sync v--
    orphans_rx: mpsc::Receiver<ReceivedObject<OrphanCertificate>>,
    incomplete_headers_rx: mpsc::Receiver<ReceivedObject<IncompleteHeader>>,
    missing_headers_rx: mpsc::Receiver<ReceivedObject<HeaderId>>,
    // to send commands to the fetcher
    fetcher_commands_tx: mpsc::Sender<Box<dyn Fetch + Send + Sync + 'static>>,
    // to expose all orphan certificates
    orphans_tx: watch::Sender<HashSet<CertificateId>>,
    reset_trigger: mpsc::Receiver<()>,
    network_router: PrimaryConnector,
    db: Arc<Db>,
}

#[derive(Debug, Constructor)]
pub struct OrphanCertificate {
    pub id: CertificateId,
    pub missing_parents: Vec<CertificateId>,
}

#[derive(Debug, Constructor)]
pub struct IncompleteHeader {
    pub missing_certificates: HashSet<CertificateId>,
    pub missing_batches: HashSet<BatchId>,
    pub header: BlockHeader,
    pub sender: PeerId,
}

impl SyncTracker {
    pub async fn run(mut self) -> anyhow::Result<()> {
        tracing::info!("🔄 Starting the synchronizer");
        let mut orphans_certificates: Vec<OrphanCertificate> = vec![];
        let mut missing_headers: Vec<HeaderId> = vec![];
        let mut incomplete_headers: Vec<IncompleteHeader> = vec![];
        loop {
            publish_orphans(&orphans_certificates, &self.orphans_tx)?;
            tokio::select! {
                // v-- reception of the data that we have to synchronize --v
                Some(orphan) = self.orphans_rx.recv() => {
                    tracing::info!("📡 Received orphan certificate {}", orphan.object.id.0.as_hex_string());
                    let missing_parents: HashSet<CertificateId> = orphan
                        .object
                        .missing_parents
                        .iter()
                        .filter(|parent| {
                            let id = parent.0;
                            !orphans_certificates.iter().any(|elm| elm.id.0 == id)
                        })
                        .cloned()
                        .collect();

                    self.fetcher_commands_tx.send(missing_parents.requested_with_source(orphan.sender.clone())).await?;
                    orphans_certificates.push(orphan.object);
                }
                Some(incomplete_header) = self.incomplete_headers_rx.recv() => {
                    tracing::info!("📡 Received incomplete header {}", incomplete_header.object.header.id().as_hex_string());
                    // Send fetch commands for missing certificates and batches
                    self.fetcher_commands_tx.send(incomplete_header.object.missing_certificates.clone().requested_with_source(incomplete_header.sender.clone())).await?;
                    self.fetcher_commands_tx.send(incomplete_header.object.missing_batches.clone().requested_with_source(incomplete_header.sender.clone())).await?;
                    // add the header in the tracked list
                    incomplete_headers.push(incomplete_header.object);
                }
                Some(missing_header) = self.missing_headers_rx.recv() => {
                    tracing::info!("📡 Received missing header {}", missing_header.object.0.as_hex_string());
                    self.fetcher_commands_tx.send(missing_header.object.clone().requested_with_source(missing_header.sender.clone())).await?;
                    // add the header in the tracked list
                    missing_headers.push(missing_header.object);
                }
                // v-- data received from peers, could be responses to sync requests or not --v
                Some(certificate) = self.certificates_rx.recv() => { // Particular case for certificates, we must ensure that we received all the orphans parents of an orphan certificate before removing it from oprhans list. The certificates are sent by the DAG processor after it proceesed it and identified missing parents
                    let id = certificate.object.id();
                    orphans_certificates.iter_mut().for_each(|elm| {
                        // If a received certificate is a parent of an orphan certificate, we remove it from the missing parents of the orphan certificate
                        elm.missing_parents.retain(|parent| parent != &id);
                        if elm.missing_parents.is_empty() {
                            tracing::info!("📡 Certificate {} direct parents has been retrieved", id.0.as_hex_string());
                        }
                    });
                    // We can remove the orphan certificate from the list if all its parents have been retrieved. WARNING(TODO): really ?
                    orphans_certificates.retain(|elm| !elm.missing_parents.is_empty());
                    incomplete_headers.iter_mut().for_each(|elm| {
                        elm.missing_certificates.retain(|certificate| certificate != &id);
                    });
                    process_incomplete_headers(&mut incomplete_headers, &self.network_router).await?;
                }
                Ok(digest) = self.digests_rx.recv() => {
                    // if an incomplete header depends on this digest, we remove it from the missing batches
                    incomplete_headers.iter_mut().for_each(|elm| {
                        elm.missing_batches.retain(|batch| batch != &digest.object);
                    });
                    process_incomplete_headers(&mut incomplete_headers, &self.network_router).await?;
                }
                Ok(header) = self.headers_rx.recv() => {
                    if missing_headers.contains(&header.object.id().into()) {
                        tracing::info!("📡 Header {} has been retrieved", header.object.id().as_hex_string());

                        missing_headers.retain(|elm| elm != &header.object.id().into());
                    }
                    match header_missing_data(&header.object, header.sender, self.db.clone()) {
                        Some(incomplete_header) => {
                            tracing::info!("📡 Header {} is incomplete", header.object.id().as_hex_string());
                            self.fetcher_commands_tx.send(incomplete_header.missing_certificates.clone().requested_with_source(incomplete_header.sender.clone())).await?;
                            self.fetcher_commands_tx.send(incomplete_header.missing_batches.clone().requested_with_source(incomplete_header.sender.clone())).await?;
                            incomplete_headers.push(incomplete_header);
                        }
                        None => {
                            tracing::info!("📡 Header {} inserted in DB", header.object.id().as_hex_string());
                            self.db.insert(db::Column::Headers, &header.object.id().as_hex_string(), header.object.clone())?;
                        }
                    }
                }
                Some(_) = self.reset_trigger.recv() => {
                    orphans_certificates.clear();
                    tracing::info!("🔄 Resetting the synchronizer");
                }
                else => break Ok(()),
            }
        }
    }
}

/// check if we have all the data referenced by the header
fn header_missing_data(
    header: &BlockHeader,
    sender: PeerId,
    db: Arc<Db>,
) -> Option<IncompleteHeader> {
    let missing_batches: HashSet<BatchId> = header
        .digests
        .iter()
        .filter(
            |digest| match db.get::<BatchId>(db::Column::Digests, &digest.0.as_hex_string()) {
                Ok(Some(_)) => true,
                _ => false,
            },
        )
        .cloned()
        .collect();

    let missing_certificates: HashSet<CertificateId> = header
        .certificates_ids
        .iter()
        .filter(|certificate| {
            match db.get::<CertificateId>(db::Column::Certificates, &certificate.0.as_hex_string())
            {
                Ok(Some(_)) => true,
                _ => false,
            }
        })
        .cloned()
        .collect();

    if missing_certificates.is_empty() && missing_batches.is_empty() {
        None
    } else {
        Some(IncompleteHeader {
            missing_certificates,
            missing_batches,
            header: header.clone(),
            sender,
        })
    }
}

fn publish_orphans(
    orphans: &Vec<OrphanCertificate>,
    orphans_tx: &watch::Sender<HashSet<CertificateId>>,
) -> anyhow::Result<()> {
    orphans_tx.send(orphans.iter().map(|elm| elm.id).collect())?;
    Ok(())
}

async fn process_incomplete_headers(
    headers: &mut Vec<IncompleteHeader>,
    router: &PrimaryConnector,
) -> anyhow::Result<()> {
    let to_dispatch: Vec<_> = headers
        .iter()
        .filter(|header| {
            header.missing_certificates.is_empty() && header.missing_batches.is_empty()
        })
        .map(|header| (header.header.clone(), header.sender.clone()))
        .collect();

    for (header, sender) in to_dispatch {
        router
            .dispatch(&RequestPayload::Header(header), sender)
            .await?;
    }

    headers.retain(|header| {
        !header.missing_certificates.is_empty() || !header.missing_batches.is_empty()
    });
    Ok(())
}
