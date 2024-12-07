use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, RwLock};

use crate::{
    network::{Connect, ManagePeers},
    types::network::{NetworkRequest, ReceivedObject, SyncResponse},
};

use super::Fetch;

pub struct Fetcher<R>
where
    R: Connect + Send,
{
    network_tx: mpsc::Sender<NetworkRequest>,
    //The data that need to be fetched, the fetcher doesn't care about the type of the data, it just fetch it and send it back in the router to be dispatched to the right tasks
    commands_rx: mpsc::Receiver<Box<dyn Fetch + Send + Sync>>,
    //Will contain only responses to sync requests
    sync_response_rx: broadcast::Receiver<ReceivedObject<SyncResponse>>,
    //Not sure if really needed
    peers: Arc<RwLock<Box<dyn ManagePeers + Send>>>,
    //PrimaryConnector or WorkerConnector, Only contains senders, can be duplicated. To dispatch the fetched data
    publish_router: R,
}

impl<R> Fetcher<R>
where
    R: Connect + Send,
{
    /// Just for testing for now, fecth tasks cant be blocking, circular buffer of tasks, timeout for each task ?
    pub async fn run(mut self) -> Result<(), anyhow::Error> {
        loop {
            // could be for example for a missing header of id header_id to fetch from a peer p. : header_id.requested_with_source(p)
            let mut missing_data = self
                .commands_rx
                .recv()
                .await
                .ok_or(anyhow::anyhow!("FetcherCommand channel closed"))?;
            match missing_data
                .fetch(self.network_tx.clone(), self.sync_response_rx.resubscribe())
                .await
            {
                Ok(data) => {
                    for data in data {
                        self.publish_router
                            .dispatch(&data.object, data.sender)
                            .await?;
                    }
                }
                _ => {}
            };
        }
    }
}
