pub mod batch_broadcaster;
pub mod batch_maker;
pub mod network;
pub mod synchronizer;

use anyhow::Context;
use batch_broadcaster::{quorum_waiter_task, BatchBroadcaster};
use batch_maker::BatchMaker;
use clap::{command, Parser};
use crossterm::event::{Event, EventStream, KeyCode};
use derive_more::{AsMut, AsRef, Deref, DerefMut};
use futures::{FutureExt as _, StreamExt as _};
use futures_timer::Delay;
use futures_util::future::try_join_all;
use network::Network as WorkerNetwork;
use std::{path::PathBuf, sync::Arc, time::Duration};
use tokio::sync::{broadcast, mpsc};

use crate::{
    db,
    settings::parser::{InstanceConfig, NetworkInfos, WorkerConfig},
    types::{
        agents::{BaseAgent, LoadableFromSettings, Settings},
        Transaction,
    },
    utils,
};

// ARBITRAIRE !!!
const QUORUM_TRESHOLD: u32 = 3;

/// CLI arguments for Worker
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct WorkerCli {
    /// Path to the database directory
    #[arg(short, long, default_value = "db")]
    db_path: PathBuf,
    /// Path to the keypair file
    #[arg(short, long, default_value = "keypair")]
    keypair_path: PathBuf,
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
    /// Key pair path
    pub keypair: PathBuf,
}

impl LoadableFromSettings for WorkerSettings {
    fn load() -> anyhow::Result<Self> {
        // Parse command line arguments
        let cli = WorkerCli::parse();

        Ok(Self {
            base: Settings::load()?,
            db: cli.db_path,
            keypair: cli.keypair_path,
        })
    }
}

#[derive(Debug)]
pub(crate) struct Worker {
    config: WorkerConfig,
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
        let config = match InstanceConfig::load_from_file("config.json")? {
            InstanceConfig::Worker(worker_config) => worker_config,
            _ => unreachable!("Worker agent can only be run as a worker"),
        };
        let keypair = utils::read_keypair_from_file(&settings.keypair)
            .context("Failed to read keypair from file")?;

        Ok(Self {
            config,
            network,
            db,
            keypair,
        })
    }

    async fn run(mut self) {
        let mut tasks = vec![];
        let (batches_tx, batches_rx) = broadcast::channel(100);
        let (transactions_tx, transactions_rx) = mpsc::channel(100);
        let (network_tx, network_rx) = mpsc::channel(100);
        let (network_resp_tx, _network_resp_rx) = tokio::sync::oneshot::channel();
        let (acknolwedgements_tx, acknolwedgements_rx) = mpsc::channel(100);
        let (digest_tx, _digest_rx) = mpsc::channel(100);
        let quorum_waiter_batches_rx = batches_tx.subscribe();

        // Spawn BatchMaker
        let batch_maker = BatchMaker::new(
            batches_tx,
            transactions_rx,
            self.config.timeout,
            self.config.batch_size,
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

        tasks.push(
            tokio::spawn(async move {
                quorum_waiter_task(quorum_waiter_batches_rx, acknolwedgements_rx, QUORUM_TRESHOLD, digest_tx, Arc::clone(&self.db)).await
        }));

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
