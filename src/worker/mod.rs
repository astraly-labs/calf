pub mod batch_broadcaster;
pub mod batch_maker;

use std::time::Duration;

use batch_broadcaster::BatchBroadcaster;
use batch_maker::BatchMaker;
use crossterm::event::{Event, EventStream, KeyCode};
use futures::{FutureExt as _, StreamExt as _};
use futures_timer::Delay;
use futures_util::future::try_join_all;
use tokio::sync::mpsc;

use crate::{
    config::{InstanceConfig, NetworkInfos},
    db,
    types::{
        agents::{BaseAgent, Settings},
        Transaction,
    },
};

#[derive(Debug)]
pub(crate) struct Worker {
    config: InstanceConfig,
    network: NetworkInfos,
    db: db::Db,
}

#[async_trait::async_trait]
impl BaseAgent for Worker {
    const AGENT_NAME: &'static str = "worker";
    type Settings = Settings;

    async fn from_settings(_settings: Self::Settings) -> anyhow::Result<Self> {
        let db = db::Db::new("./db")?;
        let network = NetworkInfos::default();
        let config = InstanceConfig::load_from_file("config.json")?;

        Ok(Self {
            config,
            network,
            db,
        })
    }

    async fn run(mut self) {
        let mut tasks = vec![];
        match self.config {
            InstanceConfig::Worker(worker_config) => {
                let (batches_tx, batches_rx) = mpsc::channel(100);
                let (transactions_tx, transactions_rx) = mpsc::channel(100);

                // Spawn BatchMaker
                let batch_maker = BatchMaker::new(
                    batches_tx,
                    transactions_rx,
                    worker_config.timeout,
                    worker_config.batch_size,
                );
                tasks.push(tokio::spawn(async move { batch_maker.spawn().await }));

                // Spawn BatchBroadcaster
                let batch_broadcaster = BatchBroadcaster::new(batches_rx);
                tasks.push(tokio::spawn(async move { batch_broadcaster.spawn().await }));

                tasks.push(tokio::spawn(async move {
                    let transaction = Transaction { data: vec![1; 100] };

                    let mut reader = EventStream::new();

                    loop {
                        let delay = Delay::new(Duration::from_millis(1_000)).fuse();
                        let event = reader.next().fuse();

                        tokio::select! {
                            _ = delay => { },
                            maybe_event = event => {
                                match maybe_event {
                                    Some(Ok(event)) => {
                                        if event == Event::Key(KeyCode::Char('t').into()) {
                                            transactions_tx.send(transaction.clone()).await.unwrap();
                                            tracing::info!("transaction sent");
                                        }

                                        if event == Event::Key(KeyCode::Esc.into()) {
                                            break;
                                        }
                                    }
                                    Some(Err(e)) => tracing::error!("Transaction Sender Error: {:?}\r", e),
                                    None => break,
                                }
                            }
                        };
                    }
                    #[allow(unreachable_code)]
                    Ok(())
                }));
            }
            _ => unreachable!("Worker agent can only be run as a worker"),
        }
        if let Err(e) = try_join_all(tasks).await {
            tracing::error!("Error in Worker: {:?}", e);
        }
    }
}
