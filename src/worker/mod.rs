pub mod batch_broadcaster;
pub mod batch_maker;
pub mod network;

use batch_broadcaster::BatchBroadcaster;
use batch_maker::BatchMaker;
use clap::{command, Parser};
use crossterm::event::{Event, EventStream, KeyCode};
use derive_more::{AsMut, AsRef, Deref, DerefMut};
use futures::{FutureExt as _, StreamExt as _};
use futures_timer::Delay;
use futures_util::future::try_join_all;
use network::Network as WorkerNetwork;
use std::{path::PathBuf, sync::Arc, time::Duration};
use tokio::sync::mpsc;

use crate::{
    db,
    settings::parser::{InstanceConfig, NetworkInfos},
    types::{
        agents::{BaseAgent, LoadableFromSettings, Settings},
        Transaction,
    },
};

/// CLI arguments for Worker
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct WorkerCli {
    /// Path to the database directory
    #[arg(short, long, default_value = "db")]
    db_path: PathBuf,
}

/// Settings for `Worker`
#[derive(Debug, AsRef, AsMut, Deref, DerefMut)]
pub struct WorkerSettings {
    #[as_ref]
    #[as_mut]
    #[deref]
    #[deref_mut]
    base: Settings,
    /// Database path
    pub db: PathBuf,
}

impl LoadableFromSettings for WorkerSettings {
    fn load() -> anyhow::Result<Self> {
        // Parse command line arguments
        let cli = WorkerCli::parse();

        Ok(Self {
            base: Settings::load()?,
            db: cli.db_path,
        })
    }
}

#[derive(Debug)]
pub(crate) struct Worker {
    config: InstanceConfig,
    keypair: libp2p::identity::Keypair,
    network: NetworkInfos,
    db: Arc<db::Db>,
}

#[async_trait::async_trait]
impl BaseAgent for Worker {
    const AGENT_NAME: &'static str = "worker";
    type Settings = WorkerSettings;

    async fn from_settings(settings: Self::Settings) -> anyhow::Result<Self> {
        let db = Arc::new(db::Db::new(settings.db)?);
        let network = NetworkInfos::default();
        let config = InstanceConfig::load_from_file("config.json")?;
        let keypair = match &config {
            InstanceConfig::Worker(worker_config) => {
                let bytes= hex::decode(&worker_config.keypair)?;
                match libp2p::identity::Keypair::ed25519_from_bytes(bytes) {
                    Ok(keypair) => Ok(keypair),
                    Err(e) => {
                        tracing::error!("failed to decode keypair from worker configuration file");
                        Err(e)
                    }
                }
            },
            _ => unreachable!("Worker agent can only be run as a worker"),
        }?;

        Ok(Self {
            config,
            network,
            db,
            keypair,
        })
    }

    async fn run(mut self) {
        let mut tasks = vec![];
        match self.config {
            InstanceConfig::Worker(worker_config) => {
                let (batches_tx, batches_rx) = mpsc::channel(100);
                let (transactions_tx, transactions_rx) = mpsc::channel(100);
                let (network_tx, network_rx) = mpsc::channel(100);
                let (network_resp_tx, _network_resp_rx) = tokio::sync::oneshot::channel();

                // Spawn BatchMaker
                let batch_maker = BatchMaker::new(
                    batches_tx,
                    transactions_rx,
                    worker_config.timeout,
                    worker_config.batch_size,
                );
                tasks.push(tokio::spawn(async move { batch_maker.spawn().await }));

                // Spawn BatchBroadcaster
                let batch_broadcaster = BatchBroadcaster::new(batches_rx, network_tx);
                tasks.push(tokio::spawn(async move { batch_broadcaster.spawn().await }));

                tasks.push(tokio::spawn(async move {
                    tokio::spawn(transaction_event_listener_task(transactions_tx)).await
                }));

                tasks.push(tokio::spawn(async move {
                    WorkerNetwork::spawn(network_rx, network_resp_tx, self.keypair).await
                }));
            }
            _ => unreachable!("Worker agent can only be run as a worker"),
        }
        if let Err(e) = try_join_all(tasks).await {
            tracing::error!("Error in Worker: {:?}", e);
        }
    }
}

#[tracing::instrument(skip_all)]
async fn transaction_event_listener_task(tx: mpsc::Sender<Transaction>) {
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
                            tx.send(transaction.clone()).await.unwrap();
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
}
