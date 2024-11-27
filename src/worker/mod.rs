pub mod batch_broadcaster;
pub mod batch_maker;
pub mod batch_receiver;
pub mod quorum_waiter;
pub mod transaction_event_listener;

use anyhow::Context;
use batch_broadcaster::BatchBroadcaster;
use batch_maker::BatchMaker;
use batch_receiver::BatchReceiver;
use clap::{command, Parser};
use derive_more::{AsMut, AsRef, Deref, DerefMut};
use quorum_waiter::QuorumWaiter;
use std::{path::PathBuf, sync::Arc};
use tokio::sync::{broadcast, mpsc};
use tokio_util::sync::CancellationToken;
use transaction_event_listener::TransactionEventListener;

use crate::{
    db,
    network::{
        worker::{WorkerConnector, WorkerPeers},
        Network, WorkerNetwork,
    },
    settings::parser::{AuthorityInfo, Committee, FileLoader as _},
    types::{
        agents::{BaseAgent, LoadableFromSettings, Settings},
        Transaction,
    },
    utils,
};

// ARBITRAIRE !!!
const QUORUM_TRESHOLD: u32 = 1;
const TIMEOUT: u64 = 1000;
const BATCH_SIZE: usize = 10;
const QUORUM_TIMEOUT: u128 = 1000;

// Wrapper
pub struct WorkerMetadata {
    pub id: u32,
    pub authority: AuthorityInfo,
}

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
    /// Path to the database directory
    #[arg(short, long)]
    pub id: u32,
    #[arg(long, default_value = "false")]
    pub txs_producer: bool,
}

#[derive(Debug, AsRef, AsMut, Deref, DerefMut)]
pub struct WorkerSettings {
    #[as_ref]
    #[as_mut]
    #[deref]
    #[deref_mut]
    pub base: Settings,
    pub id: u32,
    pub txs_producer: bool,
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
            id: cli.id,
            txs_producer: cli.txs_producer,
        })
    }
}

#[derive(Debug)]
pub(crate) struct Worker {
    id: u32,
    commitee: Committee,
    keypair: libp2p::identity::Keypair,
    validator_keypair: libp2p::identity::Keypair,
    db: Arc<db::Db>,
    txs_producer: bool,
}

#[async_trait::async_trait]
impl BaseAgent for Worker {
    const AGENT_NAME: &'static str = "worker";
    type Settings = WorkerSettings;

    async fn from_settings(settings: Self::Settings) -> anyhow::Result<Self> {
        let db = Arc::new(db::Db::new(settings.base.db_path)?);
        let commitee = match Committee::load_from_file("committee.json") {
            Ok(c) => c,
            Err(e) => {
                tracing::error!("Failed to load committee from file: {:?}", e);
                return Err(e);
            }
        };
        let keypair = utils::read_keypair_from_file(&settings.base.keypair_path)
            .context("Failed to read keypair from file")?;
        let validator_keypair =
            utils::read_keypair_from_file(&settings.base.validator_keypair_path)
                .context("Failed to read keypair from file")?;
        Ok(Self {
            id: settings.id,
            commitee,
            db,
            keypair,
            validator_keypair,
            txs_producer: settings.txs_producer,
        })
    }

    async fn run(mut self) {
        let (batches_tx, batches_rx) = broadcast::channel(100);
        let (transactions_tx, transactions_rx) = mpsc::channel(100);
        let quorum_waiter_batches_rx = batches_tx.subscribe();
        let (network_tx, network_rx) = mpsc::channel(100);
        let (p2p_connector, acks_rx, received_batches_rx) = WorkerConnector::new(100);

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

        let tx_producer_handle =
            tx_producer_task(transactions_tx.clone(), 10, 1000, self.txs_producer);

        let transaction_event_listener_handle =
            TransactionEventListener::spawn(transactions_tx, cancellation_token.clone());

        let peers = WorkerPeers::new(
            self.id,
            hex::encode(self.validator_keypair.public().encode_protobuf()),
        );

        tracing::info!(
            "launched with validator keypair: {}",
            hex::encode(self.validator_keypair.public().encode_protobuf())
        );

        let worker_network_handle = Network::<WorkerNetwork, WorkerConnector, WorkerPeers>::spawn(
            self.commitee,
            p2p_connector,
            self.validator_keypair,
            self.keypair,
            peers,
            network_rx,
            cancellation_token.clone(),
        );

        let quorum_waiter_handle = QuorumWaiter::spawn(
            quorum_waiter_batches_rx,
            acks_rx,
            network_tx.clone(),
            Arc::clone(&self.db),
            QUORUM_TRESHOLD,
            QUORUM_TIMEOUT,
            cancellation_token.clone(),
        );

        let batch_acknowledger_handle = BatchReceiver::spawn(
            received_batches_rx,
            network_tx,
            Arc::clone(&self.db),
            cancellation_token.clone(),
        );

        let res = tokio::try_join!(
            batchmaker_handle,
            batch_broadcaster_handle,
            transaction_event_listener_handle,
            worker_network_handle,
            quorum_waiter_handle,
            batch_acknowledger_handle,
            tx_producer_handle,
        );

        match res {
            Ok(_) => tracing::info!("Worker exited successfully"),
            Err(e) => tracing::error!("Worker exited with error: {:?}", e),
        }
    }
}

fn tx_producer_task(
    txs_tx: mpsc::Sender<Transaction>,
    size: usize,
    delay: u64,
    flag: bool,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        if flag {
            loop {
                let tx = Transaction::random(size);
                txs_tx.send(tx).await.unwrap();
                tokio::time::sleep(tokio::time::Duration::from_millis(delay)).await;
            }
        }
    })
}
