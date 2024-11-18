pub mod batch_acknowledger;
pub mod batch_broadcaster;
pub mod batch_maker;
pub mod network;
pub mod quorum_waiter;
pub mod transaction_event_listener;

use anyhow::Context;
use batch_acknowledger::Batchacknowledger;
use batch_broadcaster::BatchBroadcaster;
use batch_maker::BatchMaker;
use clap::{command, Parser};
use derive_more::{AsMut, AsRef, Deref, DerefMut};
use network::Network as WorkerNetwork;
use quorum_waiter::QuorumWaiter;
use transaction_event_listener::TransactionEventListener;
use std::{path::PathBuf, sync::Arc};
use tokio::sync::{broadcast, mpsc};

use crate::{
    db,
    settings::parser::{InstanceConfig, NetworkInfos, WorkerConfig},
    types::{
        agents::{BaseAgent, LoadableFromSettings, Settings},
        ReceivedAcknowledgment,
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
        let (batches_tx, batches_rx) = broadcast::channel(100);
        let (transactions_tx, transactions_rx) = mpsc::channel(100);
        let (network_tx, network_rx) = mpsc::channel(100);
        let (received_ack_tx, received_ack_rx) = mpsc::channel::<ReceivedAcknowledgment>(100);
        let (received_batches_tx, received_batches_rx) = mpsc::channel(100);
        let (digest_tx, _digest_rx) = mpsc::channel(100);
        let quorum_waiter_batches_rx = batches_tx.subscribe();

        let batchmaker_handle = BatchMaker::spawn(batches_tx,
            transactions_rx,
            self.config.timeout,
            self.config.batch_size);

        let batch_broadcaster_handle = BatchBroadcaster::spawn(batches_rx, network_tx.clone());

        let transaction_event_listener_handle = TransactionEventListener::spawn(transactions_tx);

        let worker_network_hadle = WorkerNetwork::spawn(
            network_rx,
            received_ack_tx,
            received_batches_tx,
            self.keypair,
        );

        let quorum_waiter_handle = QuorumWaiter::spawn(
            quorum_waiter_batches_rx,
            received_ack_rx,
            digest_tx,
            Arc::clone(&self.db),
            QUORUM_TRESHOLD,
            self.config.quorum_timeout.into(),
        );

        let batch_acknoledger_handle = Batchacknowledger::spawn(received_batches_rx, network_tx);

        let res = tokio::try_join!(
            batchmaker_handle,
            batch_broadcaster_handle,
            transaction_event_listener_handle,
            worker_network_hadle,
            quorum_waiter_handle,
            batch_acknowledger_task,
        );

        match res {
            Ok(_) => tracing::info!("Worker exited successfully"),
            Err(e) => tracing::error!("Worker exited with error: {:?}", e),
        }
    }
}

