use tokio::{
    sync::{broadcast, mpsc},
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;

use crate::{
    network::Connect,
    types::network::{NetworkRequest, ReceivedObject, SyncResponse},
};

use super::Fetch;

pub const MAX_CONCURENT_FETCH_TASKS: usize = 10;

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
    max_concurrent_fetch_tasks: usize,
}

impl<R> Fetcher<R>
where
    R: Connect + Send + 'static,
{
    pub async fn run(mut self) -> Result<(), anyhow::Error> {
        let mut tasks = JoinSet::new();
        let mut tasks_number = 0;
        loop {
            tokio::select! {
                Some(mut command) = self.commands_rx.recv() => {
                    if tasks_number < self.max_concurrent_fetch_tasks {
                        {
                            let network_tx = self.network_tx.clone();
                            let sync_response_rx = self.sync_response_rx.resubscribe();
                            let task = async move {
                                command.try_fetch(network_tx, sync_response_rx).await
                            };
                            tasks.spawn(task);
                            tracing::info!("new fetch task spawned");
                        }
                        tasks_number += 1;
                    }
                    else {
                        tracing::info!("fetcher is busy, queueing the command");
                    }
                }
                Some(res) = tasks.join_next() => {
                    tasks_number -= 1;
                    match res {
                        Ok(Ok(data)) => {
                            tracing::info!("fetch task finished successfully: publishing data");
                            for payload in data {
                                self.publish_router.dispatch(&payload.object, payload.sender).await?;
                            }
                        }
                        Ok(Err(e)) => {
                            tracing::error!("fetch task finished with an error: {:#?}", e);
                        }
                        Err(e) => {
                            tracing::error!("fetch task finished with an error: {:#?}", e);
                        }
                    }
                }
                else => {
                    break Ok(());
                }
            }
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
        max_concurrent_fetch_tasks: usize,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let run = Self {
                network_tx,
                commands_rx,
                sync_response_rx,
                publish_router,
                max_concurrent_fetch_tasks,
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
