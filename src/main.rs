use async_channel::{bounded, Sender};
use config::{InstanceConfig, NetworkInfos};
use futures_util::future::try_join_all;
use tokio::task::JoinHandle;
use types::{
    agents::{agent_main, BaseAgent, LoadableFromSettings, Settings},
    Transaction,
};
use worker::{batch_broadcaster::BatchBroadcaster, batch_maker::BatchMaker};

pub mod cli;
pub mod config;
pub mod db;
pub mod dispatcher;
pub mod primary;
pub mod types;
pub mod worker;

#[derive(Debug)]
struct TxSenderAgent {
    transactions_tx: Sender<Transaction>,
}

#[derive(Debug)]
struct Worker {
    config: InstanceConfig,
    network: NetworkInfos,
    db: db::Db,
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

#[async_trait::async_trait]
impl BaseAgent for TxSenderAgent {
    const AGENT_NAME: &'static str = "transaction_sender";
    type Settings = Settings;

    async fn from_settings(_settings: Self::Settings) -> anyhow::Result<Self> {
        let (transactions_tx, _) = bounded(100);
        Ok(Self { transactions_tx })
    }

    async fn run(self) {
        let mut handles: Vec<JoinHandle<anyhow::Result<()>>> = Vec::new();

        handles.push(tokio::spawn(async move {
            let transaction = Transaction { data: vec![1; 100] };

            loop {
                if crossterm::event::poll(std::time::Duration::from_millis(100))? {
                    if let crossterm::event::Event::Key(key) = crossterm::event::read()? {
                        match key.code {
                            crossterm::event::KeyCode::Char('t') => {
                                self.transactions_tx.send(transaction.clone()).await?;
                                tracing::info!("transaction sent");
                            }
                            _ => {}
                        }
                    }
                }
            }
            #[allow(unreachable_code)]
            Ok(())
        }));

        if let Err(e) = try_join_all(handles).await {
            tracing::error!("Error in TxSenderAgent: {:?}", e);
        }
    }
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
                let (batches_tx, batches_rx) = bounded(worker_config.batch_size);
                let (transactions_tx, transactions_rx) = bounded(worker_config.batch_size);

                // Spawn BatchMaker
                let mut batch_maker = BatchMaker::new(
                    batches_tx,
                    transactions_rx,
                    worker_config.timeout,
                    worker_config.batch_size,
                );
                tasks.push(tokio::spawn(async move { batch_maker.run_forever().await }));

                // Spawn BatchBroadcaster
                let mut batch_broadcaster = BatchBroadcaster::new(batches_rx);
                tasks.push(tokio::spawn(async move {
                    batch_broadcaster.run_forever().await
                }));

                tasks.push(tokio::spawn(async move {
                    let transaction = Transaction { data: vec![1; 100] };

                    loop {
                        if crossterm::event::poll(std::time::Duration::from_millis(100))? {
                            if let crossterm::event::Event::Key(key) = crossterm::event::read()? {
                                match key.code {
                                    crossterm::event::KeyCode::Char('t') => {
                                        transactions_tx.send(transaction.clone()).await?;
                                        tracing::info!("transaction sent");
                                    }
                                    _ => {}
                                }
                            }
                        }
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    agent_main::<Worker>().await?;

    Ok(())
}
