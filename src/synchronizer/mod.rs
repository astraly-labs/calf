use std::collections::HashSet;

use async_trait::async_trait;
use tokio::sync::{broadcast, mpsc};
use traits::{DataProvider, Fetch, IntoSyncRequest};

use crate::types::{
    batch::BatchId,
    block_header::HeaderId,
    certificate::CertificateId,
    network::{NetworkRequest, ReceivedObject, RequestPayload, SyncRequest, SyncResponse},
    traits::{AsHex, Hash},
    Digest,
};

pub mod feeder;
pub mod fetcher;
pub mod traits;

const ONE_PEER_FETCH_TIMEOUT: u64 = 100;

/// May be used later, to cancel a fetch task if the searched data is received by another way
pub enum FetcherCommand {
    Push(Box<dyn Fetch + Send + Sync + 'static>),
    Remove(Box<dyn Fetch + Send + Sync + 'static>),
}

/// A structure that contains the id of an object to fetch and the source to fetch it from.
/// the dataProvider trait provides a list of peers ids (that could provide the data referenced by the 'object')
pub struct RequestedObject<T> {
    pub object: T,
    pub source: Box<dyn DataProvider + Send + Sync + 'static>,
}

#[async_trait]
impl<T> Fetch for RequestedObject<T>
where
    T: Fetch + Send + Sync + 'static,
{
    /// Fetch the RequestedObject from its source
    async fn try_fetch(
        &mut self,
        requests_tx: mpsc::Sender<NetworkRequest>,
        responses_rx: broadcast::Receiver<ReceivedObject<SyncResponse>>,
    ) -> anyhow::Result<Vec<ReceivedObject<RequestPayload>>> {
        self.object
            .try_fetch_from(requests_tx, responses_rx, &self.source)
            .await
    }
    /// To "bypass" the RequestedObject source and fecth it from another given source
    async fn try_fetch_from(
        &mut self,
        requests_tx: mpsc::Sender<NetworkRequest>,
        responses_rx: broadcast::Receiver<ReceivedObject<SyncResponse>>,
        source: &Box<dyn DataProvider + Send + Sync + 'static>,
    ) -> anyhow::Result<Vec<ReceivedObject<RequestPayload>>> {
        self.object
            .try_fetch_from(requests_tx, responses_rx, source)
            .await
    }
}

#[async_trait]
/// How we fetch things, the logic is defined here for all things that can be turned into a SyncRequest
impl<T> Fetch for T
where
    T: IntoSyncRequest + Send + Sync + 'static,
{
    async fn try_fetch(
        &mut self,
        _requests_tx: mpsc::Sender<NetworkRequest>,
        _responses_rx: broadcast::Receiver<ReceivedObject<SyncResponse>>,
    ) -> anyhow::Result<Vec<ReceivedObject<RequestPayload>>> {
        unimplemented!("Lucky Broadcast with retry");
    }
    /// Try to fetch a set of objects from a set of peers, peer per peer. If a peer answer all the data we need the process is over.
    /// If a peer provides only a subset of the data, this data is saved and we try to get the remaining missing data from the next peer.
    /// If a peer answer Failure, it doesn't have the data or is unable to provide it and we can simply try with the next peer.
    /// Note that self can also be be a single object id and source a single peer id.
    async fn try_fetch_from(
        &mut self,
        requests_tx: mpsc::Sender<NetworkRequest>,
        responses_rx: broadcast::Receiver<ReceivedObject<SyncResponse>>,
        source: &Box<dyn DataProvider + Send + Sync + 'static>,
    ) -> anyhow::Result<Vec<ReceivedObject<RequestPayload>>> {
        let mut request = self.into_sync_request();
        let mut responses: Vec<ReceivedObject<RequestPayload>> = vec![];
        for source in source.sources().await {
            let payload = RequestPayload::SyncRequest(request.clone());
            let id = request.digest();
            tracing::info!("Fetcher: sending a request of id: {}", id.as_hex_string());
            let req = NetworkRequest::SendTo(source, payload);
            let mut responses_rx_clone = responses_rx.resubscribe();
            requests_tx
                .send(req)
                .await
                .map_err(|_| FetchError::BrokenChannel)?;
            let wait_for_response = async move {
                loop {
                    if let Ok(elm) = responses_rx_clone.recv().await {
                        if elm.object.id() == id {
                            tracing::info!(
                                "Fetcher: response matches request: {}",
                                elm.object.id().as_hex_string()
                            );
                            return (elm.object, elm.sender);
                        } else {
                            tracing::info!(
                                "Fetcher: response doesn't match request: {} != {}",
                                elm.object.id().as_hex_string(),
                                id.as_hex_string()
                            );
                        }
                    }
                }
            };
            let (response, sender) = match tokio::time::timeout(
                std::time::Duration::from_millis(ONE_PEER_FETCH_TIMEOUT),
                wait_for_response,
            )
            .await
            {
                Ok((response, sender)) => (response, sender),
                Err(_) => continue,
            };
            match response {
                SyncResponse::Success(peer, data) => {
                    tracing::info!(
                        "Succes: all requested data feched from {}",
                        peer.as_hex_string()
                    );
                    tracing::info!("fecthed {} objects", data.into_payloads().len());
                    let payloads = data.into_payloads();
                    return Ok(payloads
                        .into_iter()
                        .map(|payload| ReceivedObject {
                            object: payload,
                            sender,
                        })
                        .collect());
                }
                SyncResponse::Partial(peer, data) => {
                    tracing::info!(
                        "requested data partially fetched from {}",
                        peer.as_hex_string()
                    );
                    let payloads = data.into_payloads();
                    let reached_data_ids: HashSet<Digest> = payloads
                        .iter()
                        .flat_map(|payload| payload.inner_id())
                        .collect();
                    request.remove_reached(reached_data_ids);
                    responses.extend(payloads.into_iter().map(|payload| ReceivedObject {
                        object: payload,
                        sender: source.clone(),
                    }));
                }
                SyncResponse::Failure(peer) => {
                    tracing::warn!(
                        "failure: {} doesn't have the requested data",
                        peer.as_hex_string()
                    );
                    continue;
                }
            }
        }
        if responses.is_empty() {
            Err(FetchError::Timeout)?
        } else {
            Ok(responses)
        }
    }
}

impl IntoSyncRequest for CertificateId {
    fn into_sync_request(&self) -> SyncRequest {
        SyncRequest::Certificates(vec![self.0])
    }
}

impl IntoSyncRequest for HashSet<CertificateId> {
    fn into_sync_request(&self) -> SyncRequest {
        SyncRequest::Certificates(self.iter().map(|id| id.0).collect())
    }
}

impl IntoSyncRequest for HeaderId {
    fn into_sync_request(&self) -> SyncRequest {
        SyncRequest::BlockHeaders(vec![self.0])
    }
}
impl IntoSyncRequest for HashSet<HeaderId> {
    fn into_sync_request(&self) -> SyncRequest {
        SyncRequest::BlockHeaders(self.iter().map(|id| id.0).collect())
    }
}

impl IntoSyncRequest for HashSet<BatchId> {
    fn into_sync_request(&self) -> SyncRequest {
        SyncRequest::Batches(self.iter().map(|id| id.0).collect())
    }
}

#[derive(thiserror::Error, Debug)]
pub enum FetchError {
    #[error("timeout")]
    Timeout,
    #[error("broken channel")]
    BrokenChannel,
    #[error("id error")]
    IdError,
}
