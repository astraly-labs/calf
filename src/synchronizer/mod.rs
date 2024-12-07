use std::collections::HashSet;

use async_trait::async_trait;
use tokio::sync::{broadcast, mpsc};
use traits::{DataProvider, Fetch, IntoSyncRequest};

use crate::types::{
    block_header::HeaderId,
    certificate::CertificateId,
    network::{NetworkRequest, ReceivedObject, RequestPayload, SyncRequest, SyncResponse},
};

pub mod fetcher;
pub mod traits;

const ONE_PEER_FETCH_TIMEOUT: u64 = 100;

pub enum FetcherCommand {
    Push(Box<dyn Fetch + Send + Sync + 'static>),
    Remove(Box<dyn Fetch + Send + Sync + 'static>),
}

/// A structure that contains and object to fetch and the source to fetch it from
pub struct RequestedObject<T> {
    pub object: T,
    pub source: Box<dyn DataProvider + Send + Sync + 'static>,
}

#[async_trait]
impl<T> Fetch for RequestedObject<T>
where
    T: Fetch + Send + Sync + 'static,
{
    async fn fetch(
        &mut self,
        requests_tx: mpsc::Sender<NetworkRequest>,
        responses_rx: broadcast::Receiver<ReceivedObject<SyncResponse>>,
    ) -> anyhow::Result<Vec<ReceivedObject<RequestPayload>>> {
        self.object
            .try_fetch_from(requests_tx, responses_rx, &self.source)
            .await
    }
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
    async fn fetch(
        &mut self,
        requests_tx: mpsc::Sender<NetworkRequest>,
        responses_rx: broadcast::Receiver<ReceivedObject<SyncResponse>>,
    ) -> anyhow::Result<Vec<ReceivedObject<RequestPayload>>> {
        todo!("random peers broadcast ?")
    }
    async fn try_fetch_from(
        &mut self,
        requests_tx: mpsc::Sender<NetworkRequest>,
        responses_rx: broadcast::Receiver<ReceivedObject<SyncResponse>>,
        source: &Box<dyn DataProvider + Send + Sync + 'static>,
    ) -> anyhow::Result<Vec<ReceivedObject<RequestPayload>>> {
        let request = self.into_sync_request();
        let keys = request.keys();
        for source in source.sources().await {
            let payload = RequestPayload::SyncRequest(request.clone());
            let id = payload.id().map_err(|_| FetchError::IdError)?;
            let req = NetworkRequest::SendTo(source, payload);
            let mut responses_rx_clone = responses_rx.resubscribe();
            requests_tx
                .send(req)
                .await
                .map_err(|_| FetchError::BrokenChannel)?;
            let wait_for_response = tokio::spawn(async move {
                loop {
                    if let Ok(elm) = responses_rx_clone.recv().await {
                        if elm.object.id() == id {
                            return (elm.object, elm.sender);
                        }
                    }
                }
            });
            let (response, sender) = match tokio::time::timeout(
                std::time::Duration::from_millis(ONE_PEER_FETCH_TIMEOUT),
                wait_for_response,
            )
            .await
            {
                Ok(Ok((response, sender))) => (response, sender),
                Ok(Err(_)) => continue,
                Err(_) => Err(FetchError::BrokenChannel)?,
            };
            match response {
                SyncResponse::Success(_, data) => {
                    let payloads = data.into_payloads();
                    return Ok(payloads
                        .into_iter()
                        .map(|payload| ReceivedObject {
                            object: payload,
                            sender,
                        })
                        .collect());
                    //TODO: check the accumulator too
                }
                SyncResponse::Partial(_, _data) => {
                    //TODO: remove fetched data from next request to next peer, add fetched data to an accumulator
                    todo!()
                }
                SyncResponse::Failure(_) => {
                    continue;
                }
            }
        }
        //TODO: return the accumulator
        Err(FetchError::Timeout.into())
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

#[derive(thiserror::Error, Debug)]
pub enum FetchError {
    #[error("timeout")]
    Timeout,
    #[error("broken channel")]
    BrokenChannel,
    #[error("id error")]
    IdError,
}
