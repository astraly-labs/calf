pub mod network;

use anyhow::Context;
use clap::{command, Parser};
use derive_more::{AsMut, AsRef, Deref, DerefMut};
use network::Network as PrimaryNetwork;
use std::{path::PathBuf, sync::Arc};
use tokio::sync::{broadcast, mpsc};

use crate::{
    db,
    settings::parser::{FileLoader as _, InstanceConfig, PrimaryConfig},
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
            },
        })
    }
}

#[derive(Debug)]
pub(crate) struct Primary {
    config: PrimaryConfig,
    keypair: libp2p::identity::Keypair,
    db: Arc<db::Db>,
}

#[async_trait::async_trait]
impl BaseAgent for Primary {
    const AGENT_NAME: &'static str = "worker";
    type Settings = PrimarySettings;

    async fn from_settings(settings: Self::Settings) -> anyhow::Result<Self> {
        let db = Arc::new(db::Db::new(settings.base.db_path)?);
        let config = match InstanceConfig::load_from_file("config.json")? {
            InstanceConfig::Primary(worker_config) => worker_config,
            _ => unreachable!("Primary agent can only be run as a worker"),
        };
        let keypair = utils::read_keypair_from_file(&settings.base.keypair_path)
            .context("Failed to read keypair from file")?;

        Ok(Self {
            config,
            db,
            keypair,
        })
    }

    async fn run(mut self) {
        let (network_tx, network_rx) = mpsc::channel(100);
        let network_handle = PrimaryNetwork::spawn(network_rx, self.keypair);

        let res = tokio::try_join!(network_handle);
        match res {
            Ok(_) => tracing::info!("Primary exited successfully"),
            Err(e) => tracing::error!("Primary exited with error: {:?}", e),
        }
    }
}
