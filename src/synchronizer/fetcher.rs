
use std::{future::Future, pin::Pin};

use proc_macros::Spawn;
use tokio::{sync::{broadcast, mpsc}, task::JoinSet};
use tokio_util::sync::CancellationToken;

use crate::{
    network::Connect,
    types::network::{NetworkRequest, ReceivedObject, SyncResponse},
};

use super::Fetch;

const MAX_CONCURENT_FETCH_TASKS: usize = 10;

pub struct Fetcher<R>
where
    R: Connect + Send,
{
    network_tx: mpsc::Sender<NetworkRequest>,
    //The data that need to be fetched, the fetcher doesn't care about the type of the data, it just fetch it and send it back in the router to be dispatched to the right tasks
    commands_rx: mpsc::Receiver<Box<dyn Fetch + Send + Sync>>,
    //Will contain only responses to sync requests
    sync_response_rx: broadcast::Receiver<ReceivedObject<SyncResponse>>,
    //PrimaryConnector or WorkerConnector, Only contains senders, can be duplicated. To dispatch the fetched data
    publish_router: R,
}

impl<R> Fetcher<R>
where
    R: Connect + Send + 'static,
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

    pub fn spawn(
        cancellation_token: CancellationToken,
        network_tx: mpsc::Sender<NetworkRequest>,
        //The data that need to be fetched, the fetcher doesn't care about the type of the data, it just fetch it and send it back in the router to be dispatched to the right tasks
        commands_rx: mpsc::Receiver<Box<dyn Fetch + Send + Sync>>,
        //Will contain only responses to sync requests
        sync_response_rx: broadcast::Receiver<ReceivedObject<SyncResponse>>,
        //PrimaryConnector or WorkerConnector, Only contains senders, can be duplicated. To dispatch the fetched data
        publish_router: R,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let run = Self {
                network_tx,
                commands_rx,
                sync_response_rx,
                publish_router,
            }
            .run();
            let res = cancellation_token.run_until_cancelled(run).await;
            match res {
                Some(res) => {
                    match res {
                        Ok(_) => {
                            tracing::info!("fetcher finished successfully");
                        }
                        Err(e) => {
                            tracing::error!("fetcher finished with an error: {:#?}", e);
                        }
                    };
                    cancellation_token.cancel();
                }
                None => {
                    tracing::info!("fetcher has been cancelled");
                }
            }
        })
    }
}

struct TaskManager<T>
where
    T: Send + Sync + 'static,
{
    max_concurrent_tasks: usize,
    results_tx: mpsc::Sender<T>,
    tasks_rx: mpsc::Receiver<Pin<Box<dyn Future<Output = T> + Send>>>,
}

impl<T> TaskManager<T>
where
    T: Send + Sync + 'static,
{
    // TODO: timeout for each task ?
    pub async fn run(mut self) -> anyhow::Result<()> {
        let mut tasks_number = 0;
        let mut tasks = JoinSet::new();
        loop {
            tokio::select! {
                Some(Ok(next_res)) = tasks.join_next() => {
                    self.results_tx.send(next_res).await?;
                    tasks_number -= 1;
                }
                Some(task) = self.tasks_rx.recv() => {
                    if tasks_number < self.max_concurrent_tasks {
                        tasks.spawn(task);
                        tasks_number += 1;
                    }
                    else {
                        tracing::warn!("TaskManager: too many tasks, queueing");
                    }
                }
                else => break Ok(())
            }
        }
    }
}