pub mod batch_acknowledger;
pub mod batch_broadcaster;
pub mod batch_maker;
pub mod network;
pub mod quorum_waiter;
pub mod transaction_event_listener;

use anyhow::Context;
use batch_acknowledger::BatchAcknowledger;
use batch_broadcaster::BatchBroadcaster;
use batch_maker::BatchMaker;
use clap::{command, Parser};
use derive_more::{AsMut, AsRef, Deref, DerefMut};
use network::Network as WorkerNetwork;
use quorum_waiter::QuorumWaiter;
use std::{path::PathBuf, sync::Arc};
use tokio::sync::{broadcast, mpsc};
use tokio_util::sync::CancellationToken;
use transaction_event_listener::TransactionEventListener;

use crate::{
    db,
    settings::parser::{Committee, FileLoader as _},
    types::{
        agents::{BaseAgent, LoadableFromSettings, Settings},
        ReceivedAcknowledgment,
    },
    utils,
};

// ARBITRAIRE !!!
const QUORUM_TRESHOLD: u32 = 3;
const TIMEOUT: u64 = 1000;
const BATCH_SIZE: usize = 10;
const QUORUM_TIMEOUT: u128 = 1000;

/// CLI arguments for Worker
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct WorkerArgs {
    /// Path to the database directory
    #[arg(short, long, default_value = "db")]
    pub db_path: PathBuf,
    /// Path to the keypair file
    #[arg(short, long, default_value = "keypair")]
    pub keypair_path: PathBuf,
    /// Path to the keypair file
    #[arg(short, long, default_value = "validator_keypair")]
    pub validator_keypair_path: PathBuf,
}

#[derive(Debug, AsRef, AsMut, Deref, DerefMut)]
pub struct WorkerSettings {
    #[as_ref]
    #[as_mut]
    #[deref]
    #[deref_mut]
    pub base: Settings,
}

impl LoadableFromSettings for WorkerSettings {
    fn load() -> anyhow::Result<Self> {
        // This won't be called directly anymore, but you might want to keep it
        // for backward compatibility or testing
        let cli = WorkerArgs::parse();
        Ok(Self {
            base: Settings {
                db_path: cli.db_path,
                keypair_path: cli.keypair_path,
                validator_keypair_path: cli.validator_keypair_path,
            },
        })
    }
}

#[derive(Debug)]
pub(crate) struct Worker {
    commitee: Committee,
    keypair: libp2p::identity::Keypair,
    validator_keypair: libp2p::identity::Keypair,
    db: Arc<db::Db>,
}

#[async_trait::async_trait]
impl BaseAgent for Worker {
    const AGENT_NAME: &'static str = "worker";
    type Settings = WorkerSettings;

    async fn from_settings(settings: Self::Settings) -> anyhow::Result<Self> {
        let db = Arc::new(db::Db::new(settings.base.db_path)?);
        let commitee = Committee::load_from_file(".config.json")?;
        let keypair = utils::read_keypair_from_file(&settings.base.keypair_path)
            .context("Failed to read keypair from file")?;
        let validator_keypair =
            utils::read_keypair_from_file(&settings.base.validator_keypair_path)
                .context("Failed to read keypair from file")?;
        Ok(Self {
            commitee,
            db,
            keypair,
            validator_keypair,
        })
    }

    async fn run(mut self) {
        let (batches_tx, batches_rx) = broadcast::channel(100);
        let (transactions_tx, transactions_rx) = mpsc::channel(100);
        let (network_tx, network_rx) = mpsc::channel(100);
        let (received_ack_tx, received_ack_rx) = mpsc::channel::<ReceivedAcknowledgment>(100);
        let (received_batches_tx, received_batches_rx) = mpsc::channel(100);
        let quorum_waiter_batches_rx = batches_tx.subscribe();

        let cancellation_token = CancellationToken::new();

        let batchmaker_handle = BatchMaker::spawn(
            batches_tx,
            transactions_rx,
            TIMEOUT,
            BATCH_SIZE,
            cancellation_token.clone(),
        );

        let batch_broadcaster_handle =
            BatchBroadcaster::spawn(batches_rx, network_tx.clone(), cancellation_token.clone());
        let transaction_event_listener_handle =
            TransactionEventListener::spawn(transactions_tx, cancellation_token.clone());

        let worker_network_hadle = WorkerNetwork::spawn(
            network_rx,
            received_ack_tx,
            received_batches_tx,
            self.keypair,
            self.validator_keypair,
            cancellation_token.clone(),
        );

        let quorum_waiter_handle = QuorumWaiter::spawn(
            quorum_waiter_batches_rx,
            received_ack_rx,
            network_tx.clone(),
            Arc::clone(&self.db),
            QUORUM_TRESHOLD,
            QUORUM_TIMEOUT,
            cancellation_token.clone(),
        );

        let batch_acknowledger_handle =
            BatchAcknowledger::spawn(received_batches_rx, network_tx, cancellation_token.clone());

        let res = tokio::try_join!(
            batchmaker_handle,
            batch_broadcaster_handle,
            transaction_event_listener_handle,
            worker_network_hadle,
            quorum_waiter_handle,
            batch_acknowledger_handle,
        );

        match res {
            Ok(_) => tracing::info!("Worker exited successfully"),
            Err(e) => tracing::error!("Worker exited with error: {:?}", e),
        }
    }
}
