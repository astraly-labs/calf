use std::sync::Arc;

use async_trait::async_trait;
use libp2p::PeerId;
use tokio::sync::{broadcast, mpsc, RwLock};

use crate::{
    network::ManagePeers,
    types::network::{NetworkRequest, ReceivedObject, RequestPayload, SyncRequest, SyncResponse},
};

use super::RequestedObject;

#[async_trait]
/// Detach the logic of fetching the objects from the logic of the object to fetch
pub trait Fetch {
    async fn fetch(
        &mut self,
        requests_tx: mpsc::Sender<NetworkRequest>,
        responses_rx: broadcast::Receiver<ReceivedObject<SyncResponse>>,
    ) -> anyhow::Result<Vec<ReceivedObject<RequestPayload>>>;
    async fn try_fetch_from(
        &mut self,
        requests_tx: mpsc::Sender<NetworkRequest>,
        responses_rx: broadcast::Receiver<ReceivedObject<SyncResponse>>,
        source: &Box<dyn DataProvider + Send + Sync>,
    ) -> anyhow::Result<Vec<ReceivedObject<RequestPayload>>>;
}

#[async_trait]
/// A trait to abscract structures that contains peers. Needed to build and use a requestedObject.
pub trait DataProvider {
    async fn sources(&self) -> Box<dyn Iterator<Item = PeerId> + Send>;
}

/// Fetch will use this to build the request to fetch objects without caring about the nature of the objetcs
pub trait IntoSyncRequest {
    fn into_sync_request(&self) -> SyncRequest;
}

/// A trait to build a requested object from an object and a source: if the object implements Fetch, calling fetch on the requested obejct will call try_fetch_from on the object with the source
pub trait Sourced<S, T>
where
    S: DataProvider,
{
    fn requested_with_source(self, source: S) -> Box<RequestedObject<T>>;
}

impl<S, T> Sourced<S, T> for T
where
    S: DataProvider + Sync + Send + 'static,
{
    fn requested_with_source(self, source: S) -> Box<RequestedObject<T>> {
        Box::new(RequestedObject {
            object: self,
            source: Box::new(source),
        })
    }
}

#[async_trait]
impl DataProvider for PeerId {
    async fn sources(&self) -> Box<dyn Iterator<Item = PeerId> + Send> {
        Box::new(std::iter::once(*self))
    }
}

#[async_trait]
impl<T> DataProvider for Arc<RwLock<T>>
where
    T: ManagePeers + Send + Sync,
{
    async fn sources(&self) -> Box<dyn Iterator<Item = PeerId> + Send> {
        Box::new(
            self.read()
                .await
                .get_broadcast_peers()
                .into_iter()
                .map(|(id, _)| id),
        )
    }
}
