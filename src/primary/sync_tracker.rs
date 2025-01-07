use std::{collections::HashSet, sync::Arc};

use crate::{
    db::{self, Db},
    network::{
        primary::{PrimaryConnector, PrimaryPeers},
        Connect,
    },
    synchronizer::{
        traits::{DataProvider, Fetch, Sourced},
        RequestedObject,
    },
    types::{
        batch::BatchId,
        block_header::{BlockHeader, HeaderId},
        network::{NetworkRequest, ObjectSource, RequestPayload, SyncRequest},
        sync::{IncompleteHeader, OrphanCertificate, SyncStatus, TrackedSet},
        traits::AsHex,
    },
};

use libp2p::PeerId;
use proc_macros::Spawn;
use tokio::sync::{broadcast, mpsc, watch, RwLock};
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
    digests_rx: broadcast::Receiver<ReceivedObject<(BatchId, ObjectSource)>>,
    // --v data to sync v--
    orphans_rx: mpsc::Receiver<ReceivedObject<OrphanCertificate>>,
    //incomplete_headers_rx: mpsc::Receiver<ReceivedObject<IncompleteHeader>>,
    //missing_headers_rx: mpsc::Receiver<ReceivedObject<HeaderId>>,
    // to send commands to the fetcher
    fetcher_commands_tx: mpsc::Sender<Box<dyn Fetch + Send + Sync + 'static>>,
    // to expose all orphan certificates
    sync_status_tx: watch::Sender<SyncStatus>,
    reset_trigger: mpsc::Receiver<()>,
    network_router: PrimaryConnector,
    db: Arc<Db>,
    network_tx: mpsc::Sender<NetworkRequest>,
    peers: Arc<RwLock<PrimaryPeers>>,
}

const TRACKED_OBJECT_TIMEOUT: u64 = 1000;

impl SyncTracker {
    pub async fn run(mut self) -> anyhow::Result<()> {
        tracing::info!("🔄 Starting the synchronizer");
        let mut missing_headers = TrackedSet::new(TRACKED_OBJECT_TIMEOUT);
        let mut incomplete_headers: Vec<IncompleteHeader> = vec![];
        let mut missing_certificates = TrackedSet::new(TRACKED_OBJECT_TIMEOUT);
        let mut missing_batches_digests = TrackedSet::new(TRACKED_OBJECT_TIMEOUT);
        let mut previous_sync_status = SyncStatus::Complete;
        loop {
            publish_sync_status(
                &missing_certificates,
                &missing_headers,
                &missing_batches_digests,
                &self.sync_status_tx,
                &mut previous_sync_status,
            )?;
            retry_timed_out_fetch(
                &mut missing_batches_digests,
                &mut missing_certificates,
                &mut missing_headers,
                self.peers.clone(),
                self.fetcher_commands_tx.clone(),
                self.network_tx.clone(),
            )
            .await?;
            tokio::select! {
                // v-- reception of the data that we have to synchronize --v
                Some(orphan) = self.orphans_rx.recv() => {
                    tracing::info!("📡 Received orphan certificate {}", orphan.object.id.0.as_hex_string());
                    fetch_missing_data_checked(
                        orphan.object.clone().missing_parents.into_iter().collect(),
                        &mut missing_certificates,
                        orphan.sender,
                        self.fetcher_commands_tx.clone(),
                    ).await?;
                }
                // v-- data received from peers, could be responses to sync requests or not --v
                Some(certificate) = self.certificates_rx.recv() => { // Particular case for certificates, we must ensure that we received all the orphans parents of an orphan certificate before removing it from oprhans list. The certificates are sent by the DAG processor after it proceesed it and identified missing parents
                    let id = certificate.object.id();
                    missing_certificates.retain(|elm| *elm != id);
                    incomplete_headers.iter_mut().for_each(|elm| {
                        elm.missing_certificates.retain(|certificate| certificate != &id);
                    });
                    process_incomplete_headers(&mut incomplete_headers, &self.network_router).await?;

                    if let Some(header_id) = certificate.object.header() {
                        if !check_header_storage(&header_id, &self.db) {
                            fetch_missing_data_checked([header_id].into_iter().collect(), &mut missing_headers, certificate.sender, self.fetcher_commands_tx.clone()).await?;
                        }
                    }
                }
                Ok(digest) = self.digests_rx.recv() => {
                    // if an incomplete header depends on this digest, we remove it from the missing batches
                    incomplete_headers.iter_mut().for_each(|elm| {
                        elm.missing_batches.retain(|batch| batch != &digest.object.0);
                    });
                    process_incomplete_headers(&mut incomplete_headers, &self.network_router).await?;
                    if missing_batches_digests.contains(&digest.object.0) {
                        tracing::info!("📡 Batch {} succesfully fetched", digest.object.0.0.as_hex_string());
                        missing_batches_digests.retain(|elm| elm != &digest.object.0);
                    }
                }
                Ok(header) = self.headers_rx.recv() => {
                    tracing::info!("📡 Header {} inserted in DB", header.object.id().as_hex_string());
                    self.db.insert(db::Column::Headers, &header.object.id().as_hex_string(), header.object.clone())?;

                    if missing_headers.contains(&header.object.id().into()) {
                        tracing::info!("📡 Header {} has been retrieved", header.object.id().as_hex_string());
                        missing_headers.retain(|elm| elm != &header.object.id().into());
                    }
                    if let Some(incomplete_header) = header_missing_data(&header.object, header.sender, self.db.clone()) {
                        tracing::info!("📡 Header {} is incomplete", header.object.id().as_hex_string());
                        if !incomplete_header.missing_certificates.is_empty() {
                            tracing::info!("📡 Requesting missing certificates for header {}", header.object.id().as_hex_string());
                            fetch_missing_data_checked(incomplete_header.missing_certificates.clone(), &mut missing_certificates, header.sender, self.fetcher_commands_tx.clone()).await?;
                        }
                        if !incomplete_header.missing_batches.is_empty() {
                            missing_batches_digests.extend(incomplete_header.missing_batches.iter().cloned());
                            tracing::info!("📡 Requesting missing batches for header {}", header.object.id().as_hex_string());
                            let req = RequestPayload::SyncRequest(SyncRequest::SyncDigests(incomplete_header.missing_batches.iter().map(|elm| elm.0).collect()));
                            self.network_tx.send(NetworkRequest::BroadcastSameNode(req)).await?;
                        }
                        incomplete_headers.push(incomplete_header);
                    }
                }
                Some(_) = self.reset_trigger.recv() => {
                    missing_batches_digests = TrackedSet::new(TRACKED_OBJECT_TIMEOUT);
                    missing_certificates = TrackedSet::new(TRACKED_OBJECT_TIMEOUT);
                    missing_headers = TrackedSet::new(TRACKED_OBJECT_TIMEOUT);
                    incomplete_headers = vec![];
                    previous_sync_status = SyncStatus::Complete;
                    tracing::info!("🔄 Resetting the synchronizer");
                }
                else => break Ok(()),
            }
        }
    }
}

async fn retry_timed_out_fetch<S>(
    missing_batches_digests: &mut TrackedSet<BatchId>,
    missing_certificates: &mut TrackedSet<CertificateId>,
    missing_headers: &mut TrackedSet<HeaderId>,
    source: S,
    fetcher_tx: mpsc::Sender<Box<dyn Fetch + Send + Sync + 'static>>,
    network_tx: mpsc::Sender<NetworkRequest>,
) -> anyhow::Result<()>
where
    S: DataProvider + Send + Sync + 'static + Clone,
{
    let timed_out_batches: HashSet<BatchId> = missing_batches_digests.drain_timed_out();
    let timed_out_certificates: HashSet<CertificateId> = missing_certificates.drain_timed_out();
    let timed_out_headers: HashSet<HeaderId> = missing_headers.drain_timed_out();

    if !timed_out_certificates.is_empty() {
        tracing::info!(
            "📡 Retrying to fetch certificates: {}",
            timed_out_certificates
                .iter()
                .map(|elm| elm.0.as_hex_string())
                .collect::<Vec<String>>()
                .join(", ")
        );
        fetcher_tx
            .send(timed_out_certificates.requested_with_source(source.clone()))
            .await?;
    }
    if !timed_out_headers.is_empty() {
        tracing::info!(
            "📡 Retrying to fetch headers: {}",
            timed_out_headers
                .iter()
                .map(|elm| elm.0.as_hex_string())
                .collect::<Vec<String>>()
                .join(", ")
        );
        fetcher_tx
            .send(timed_out_headers.requested_with_source(source.clone()))
            .await?;
    }
    if !timed_out_batches.is_empty() {
        tracing::info!(
            "📡 Retrying to fetch batches: {}",
            timed_out_batches
                .iter()
                .map(|elm| elm.0.as_hex_string())
                .collect::<Vec<String>>()
                .join(", ")
        );
        network_tx
            .send(NetworkRequest::BroadcastSameNode(
                RequestPayload::SyncRequest(SyncRequest::SyncDigests(
                    timed_out_batches.into_iter().map(|elm| elm.0).collect(),
                )),
            ))
            .await?;
    }
    Ok(())
}

fn check_header_storage(id: &HeaderId, db: &Db) -> bool {
    matches!(db.get::<BlockHeader>(db::Column::Headers, &id.0.as_hex_string()), Ok(Some(_)))
}

async fn fetch_missing_data_checked<T, S>(
    missing_data: HashSet<T>,
    tracked_data: &mut TrackedSet<T>,
    source: S,
    fetcher_tx: mpsc::Sender<Box<dyn Fetch + Send + Sync + 'static>>,
) -> anyhow::Result<()>
where
    T: Send + Sync + 'static + Eq + std::hash::Hash + Clone,
    S: DataProvider + Send + Sync + 'static,
    RequestedObject<HashSet<T>>: Fetch,
{
    let missing_data: HashSet<T> = missing_data
        .into_iter()
        .filter(|data| !tracked_data.contains(data))
        .collect();

    for data in &missing_data {
        tracked_data.insert(data.clone());
    }

    if missing_data.is_empty() {
        return Ok(());
    }

    fetcher_tx
        .send(missing_data.requested_with_source(source))
        .await?;
    Ok(())
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
            |digest| !matches!(db.get::<BatchId>(db::Column::Digests, &digest.0.as_hex_string()), Ok(Some(_)))
        )
        .cloned()
        .collect();

    let missing_certificates: HashSet<CertificateId> = header
        .certificates_ids
        .iter()
        .filter(|certificate| {
            !matches!(db.get::<CertificateId>(db::Column::Certificates, &certificate.0.as_hex_string()), Ok(Some(_)))
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

fn publish_sync_status(
    missing_certificates: &TrackedSet<CertificateId>,
    missing_headers: &TrackedSet<HeaderId>,
    missing_batches_digests: &TrackedSet<BatchId>,
    sync_status_tx: &watch::Sender<SyncStatus>,
    previous_sync_status: &mut SyncStatus,
) -> anyhow::Result<()> {
    if missing_certificates.is_empty()
        && missing_headers.is_empty()
        && missing_batches_digests.is_empty()
    {
        sync_status_tx.send(SyncStatus::Complete)?;
        if previous_sync_status != &SyncStatus::Complete {
            tracing::info!("🔄 Synchronized, all data has been retrieved");
        }
        *previous_sync_status = SyncStatus::Complete;
    } else {
        sync_status_tx.send(SyncStatus::Incomplete)?;
        tracing::info!(
            "🔄 Syncing, missing data: certificates: {}, headers: {}, batches: {}",
            missing_certificates.len(),
            missing_headers.len(),
            missing_batches_digests.len()
        );
        *previous_sync_status = SyncStatus::Incomplete;
    }
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
        .map(|header| (header.header.clone(), header.sender))
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
