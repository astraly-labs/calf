pub mod header_builder;
pub mod header_processor;
pub mod vote_aggregator;

use anyhow::Context;
use clap::{command, Parser};
use derive_more::{AsMut, AsRef, Deref, DerefMut};
use header_builder::HeaderBuilder;
use header_processor::HeaderProcessor;
use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};
use tokio::sync::{broadcast, mpsc};
use tokio_util::sync::CancellationToken;
use vote_aggregator::VoteAggregator;

use crate::{
    db,
    settings::parser::{Committee, FileLoader as _},
    types::agents::{BaseAgent, LoadableFromSettings, Settings},
    utils,
};

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
    keypair: libp2p::identity::Keypair,
    db: Arc<db::Db>,
}

#[async_trait::async_trait]
impl BaseAgent for Primary {
    const AGENT_NAME: &'static str = "worker";
    type Settings = PrimarySettings;

    async fn from_settings(settings: Self::Settings) -> anyhow::Result<Self> {
        let db = Arc::new(db::Db::new(settings.base.db_path)?);
        let commitee = Committee::load_from_file(".config.json")?;
        let keypair = utils::read_keypair_from_file(&settings.base.keypair_path)
            .context("Failed to read keypair from file")?;

        Ok(Self {
            commitee,
            db,
            keypair,
        })
    }

    async fn run(mut self) {
        let (network_tx, network_rx) = mpsc::channel(100);
        let (digest_tx, digest_rx) = broadcast::channel(100);
        let (header_tx, header_rx) = broadcast::channel(100);
        let (votes_tx, votes_rx) = broadcast::channel(100);

        let dag = Arc::new(Mutex::new(Default::default()));

        // let network_handle = Ne::spawn(
        //     network_rx,
        //     self.keypair.clone(),
        //     digest_tx,
        //     header_tx.clone(),
        //     votes_tx,
        //     self.commitee.clone(),
        // );

        let cancellation_token = CancellationToken::new();

        let header_builder = HeaderBuilder::spawn(
            self.keypair.clone(),
            digest_rx,
            network_tx.clone(),
            cancellation_token.clone(),
            self.db.clone(),
        );

        let header_processor = HeaderProcessor::spawn(
            self.commitee.clone(),
            header_rx,
            network_tx.clone(),
            cancellation_token.clone(),
            self.db.clone(),
            self.keypair.clone(),
        );

        let vote_aggregator = VoteAggregator::spawn(
            self.commitee,
            votes_rx,
            network_tx,
            header_tx.subscribe(),
            cancellation_token,
            self.db,
            dag,
            self.keypair,
        );

        let res = tokio::try_join!(
            //network_handle,
            header_builder,
            header_processor,
            vote_aggregator
        );
        match res {
            Ok(_) => tracing::info!("Primary exited successfully"),
            Err(e) => tracing::error!("Primary exited with error: {:?}", e),
        }
    }
}
