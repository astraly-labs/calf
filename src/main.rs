use clap::{Args, Parser, Subcommand};
use primary::PrimaryArgs;
use types::agents::{agent_main, LoadableFromSettings, Settings};
use worker::{Worker, WorkerArgs};

pub mod db;
pub mod primary;
pub mod settings;
pub mod types;
pub mod utils;
pub mod worker;

#[macro_export]
macro_rules! safe_send {
    ($sender:expr, $message:expr, $error_msg:expr) => {
        if let Err(err) = $sender.send($message).await {
            tracing::error!("{}: Failed to send message. Reason: {}", $error_msg, err);
        }
    };
}

// Empty settings for now
impl AsRef<Settings> for Settings {
    fn as_ref(&self) -> &Settings {
        self
    }
}

impl LoadableFromSettings for Settings {
    fn load() -> anyhow::Result<Self> {
        Ok(Settings {})
    }
}

// ... (your existing module declarations)

/// Node management CLI
#[derive(Debug, Parser)]
#[clap(name = "node-cli", version)]
pub struct App {
    #[clap(flatten)]
    global_opts: GlobalOpts,

    #[clap(subcommand)]
    command: Command,
}

#[derive(Debug, Args)]
struct GlobalOpts {
    /// Enable debug logging
    #[clap(long, global = true)]
    debug: bool,

    /// Log level
    #[clap(long, global = true, default_value = "info")]
    log_level: String,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run a node
    Run {
        #[clap(subcommand)]
        mode: NodeMode,
    },
    /// Generate an ed25519 key pair
    GenerateKeyPair {
        /// Path where to save the key pair
        #[clap(short, long, default_value = "keypair")]
        keypair_path: String,
    },
}

#[derive(Debug, Subcommand)]
enum NodeMode {
    /// Run as primary node
    Primary(PrimaryArgs),

    /// Run as worker node
    Worker(WorkerArgs),
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let app = App::parse();

    // Setup logging based on global options
    let subscriber = tracing_subscriber::fmt()
        .compact()
        .with_file(true)
        .with_line_number(true)
        .with_thread_ids(true)
        .with_target(false)
        .with_max_level(if app.global_opts.debug {
            tracing::Level::DEBUG
        } else {
            tracing::Level::INFO
        })
        .finish();

    tracing::subscriber::set_global_default(subscriber)?;

    match app.command {
        Command::Run { mode } => match mode {
            NodeMode::Primary(args) => {
                tracing::info!("✨ Starting primary node...");
                // Add primary node initialization here
            }
            NodeMode::Worker(args) => {
                tracing::info!("✨ Starting worker node...");
                agent_main::<Worker>().await?;
            }
        },
        Command::GenerateKeyPair { keypair_path } => {
            utils::generate_keypair_and_write_to_file(&keypair_path)?;
        }
    }

    Ok(())
}
