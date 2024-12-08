pub mod dag_processor;
pub mod digests_receiver;
pub mod header_builder;
pub mod header_elector;
pub mod sync_tracker;

use anyhow::Context;
use clap::{command, Parser};
use dag_processor::DagProcessor;
use derive_more::{AsMut, AsRef, Deref, DerefMut};
use digests_receiver::DigestReceiver;
use header_builder::HeaderBuilder;
use header_elector::HeaderElector;
use libp2p::identity::ed25519;
use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::Arc,
};
use sync_tracker::SyncTracker;
use tokio::sync::{mpsc, watch, Mutex, RwLock};
use tokio_util::sync::CancellationToken;

use crate::{
    db,
    network::{
        primary::{PrimaryConnector, PrimaryPeers},
        Network, PrimaryNetwork,
    },
    settings::parser::{Committee, FileLoader as _},
    synchronizer::fetcher::{self, Fetcher},
    types::{
        agents::{BaseAgent, LoadableFromSettings, Settings},
        batch::BatchId,
        certificate::{Certificate, CertificateId},
        Digest, Round,
    },
    utils::{self, CircularBuffer},
    CHANNEL_SIZE,
};

const MAX_DIGESTS_IN_HEADER: usize = 10;

/// CLI arguments for Primary
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct PrimaryArgs {
    /// Path to the database directory
    #[arg(short, long, default_value = "db")]
    pub db_path: PathBuf,
    /// Path to the keypair file
    #[arg(short, long, default_value = "keypair")]
    pub keypair_path: PathBuf,
    /// Path to the validator keypair file
    #[arg(short, long, default_value = "validator_keypair")]
    pub validator_keypair_path: PathBuf,
}

/// Settings for `Primary`
#[derive(Debug, AsRef, AsMut, Deref, DerefMut)]
pub struct PrimarySettings {
    #[as_ref]
    #[as_mut]
    #[deref]
    #[deref_mut]
    pub base: Settings,
}

impl LoadableFromSettings for PrimarySettings {
    fn load() -> anyhow::Result<Self> {
        // This won't be called directly anymore, but you might want to keep it
        // for backward compatibility or testing
        let cli = PrimaryArgs::parse();
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
pub(crate) struct Primary {
    commitee: Committee,
    keypair: ed25519::Keypair,
    validator_keypair: ed25519::Keypair,
    db: Arc<db::Db>,
}

#[async_trait::async_trait]
impl BaseAgent for Primary {
    const AGENT_NAME: &'static str = "worker";
    type Settings = PrimarySettings;

    async fn from_settings(settings: Self::Settings) -> anyhow::Result<Self> {
        let db = Arc::new(db::Db::new(settings.base.db_path)?);
        let commitee = Committee::load_from_file("committee.json")?;
        let keypair = utils::read_keypair_from_file(&settings.base.keypair_path)
            .context("Failed to read keypair from file")?
            .try_into_ed25519()?;
        let validator_keypair =
            utils::read_keypair_from_file(&settings.base.validator_keypair_path)
                .context("Failed to read keypair from file")?
                .try_into_ed25519()?;

        Ok(Self {
            commitee,
            db,
            keypair,
            validator_keypair,
        })
    }

    async fn run(mut self) {
        let (network_tx, network_rx) = mpsc::channel(CHANNEL_SIZE);
        let (round_tx, round_rx) =
            watch::channel::<(Round, HashSet<Certificate>)>((0, HashSet::new()));
        let (connector, digests_rx, header_rx, vote_rx, peers_certificates_rx, sync_responses_rx) =
            PrimaryConnector::new(CHANNEL_SIZE);
        let (certificates_tx, certificates_rx) = mpsc::channel(CHANNEL_SIZE);

        let (orphans_tx, orphans_rx) = mpsc::channel(CHANNEL_SIZE);
        let (missing_headers_tx, missing_headers_rx) = mpsc::channel(CHANNEL_SIZE);
        let (incomplete_headers_tx, incomplete_headers_rx) = mpsc::channel(CHANNEL_SIZE);

        let (orphans_list_tx, orphans_list_rx) = watch::channel(HashSet::<CertificateId>::new());
        let (fetcher_commands_tx, fetcher_commands_rx) = mpsc::channel(CHANNEL_SIZE);

        let (sync_reset_trigger_tx, sync_reset_trigger_rx) = mpsc::channel(CHANNEL_SIZE);

        let digests_buffer = Arc::new(Mutex::new(CircularBuffer::<BatchId>::new(
            MAX_DIGESTS_IN_HEADER,
        )));
        let cancellation_token = CancellationToken::new();

        let peers = Arc::new(RwLock::new(PrimaryPeers {
            authority_pubkey: hex::encode(self.validator_keypair.public().to_bytes()),
            workers: vec![],
            primaries: HashMap::new(),
            established: HashMap::new(),
        }));

        tracing::info!(
            "launched with validator keypair: {}",
            hex::encode(self.validator_keypair.public().to_bytes())
        );

        let digests_receiver_handle = DigestReceiver::spawn(
            cancellation_token.clone(),
            digests_rx,
            digests_buffer.clone(),
            self.db.clone(),
        );

        let header_builder_handle = HeaderBuilder::spawn(
            cancellation_token.clone(),
            network_tx.clone(),
            certificates_tx,
            self.keypair.clone(),
            self.db.clone(),
            round_rx.clone(),
            vote_rx,
            digests_buffer.clone(),
            self.commitee.clone(),
        );

        let header_elector_handle = HeaderElector::spawn(
            cancellation_token.clone(),
            network_tx.clone(),
            header_rx,
            round_rx.clone(),
            self.validator_keypair.clone(),
            self.db.clone(),
            self.commitee.clone(),
            incomplete_headers_tx,
        );

        let network_handle = Network::<PrimaryNetwork, PrimaryConnector, PrimaryPeers>::spawn(
            self.commitee.clone(),
            connector.clone(),
            self.keypair.clone(),
            self.keypair.clone(),
            peers.clone(),
            network_rx,
            cancellation_token.clone(),
        );

        let dag_processor_handle = DagProcessor::spawn(
            cancellation_token.clone(),
            peers_certificates_rx.resubscribe(),
            certificates_rx,
            orphans_tx,
            missing_headers_tx,
            orphans_list_rx,
            round_tx,
            self.commitee.clone(),
            self.db.clone(),
            sync_reset_trigger_tx,
        );

        let sync_tracker_handle = SyncTracker::spawn(
            cancellation_token.clone(),
            peers_certificates_rx.resubscribe(),
            orphans_rx,
            orphans_list_tx,
            fetcher_commands_tx,
            sync_reset_trigger_rx,
            incomplete_headers_rx,
            missing_headers_rx,
        );

        let fetcher_handle = Fetcher::spawn(
            cancellation_token.clone(),
            network_tx.clone(),
            fetcher_commands_rx,
            sync_responses_rx,
            Box::new(connector.clone()),
        );

        let res = tokio::try_join!(
            fetcher_handle,
            sync_tracker_handle,
            network_handle,
            digests_receiver_handle,
            header_builder_handle,
            header_elector_handle,
            dag_processor_handle,
        );
        match res {
            Ok(_) => tracing::info!("Primary exited successfully"),
            Err(e) => tracing::error!("Primary exited with error: {:?}", e),
        }
    }
}
