use async_channel::{bounded, Sender};
use config::{InstanceConfig, NetworkInfos};
use tokio::task::JoinSet;
use types::services::{Service, ServiceGroup};
use worker::Worker;
use types::Transaction;

pub mod cli;
pub mod config;
pub mod db;
pub mod dispatcher;
pub mod primary;
pub mod types;
pub mod worker;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    // TODO: passer les chemins en arguments
    let db_path = "./db";
    let network_infos_path = "network.json";
    let instance_config_path = "config.json";

    let db = db::Db::new(db_path)?;

    // let network = match NetworkInfos::load_from_file(network_infos_path) {
    //     Ok(infos) => infos,
    //     Err(e) => {
    //         tracing::error!(
    //             "failed to parse network infos from {}: {}",
    //             network_infos_path,
    //             e.to_string()
    //         );
    //         return Err(e);
    //     }
    // };
    let network = NetworkInfos::default();

    let config = match InstanceConfig::load_from_file(instance_config_path) {
        Ok(infos) => infos,
        Err(e) => {
            tracing::error!(
                "failed to parse instance config from {}: {}",
                instance_config_path,
                e.to_string()
            );
            return Err(e);
        }
    };

    let (transactions_tx, transactions_rx) = bounded(100);

    let mut service_group = ServiceGroup::default();
    service_group.push(TransactionSender {
        transactions_tx,
    });

    match config {
        InstanceConfig::Worker(config) => match Worker::new(config, network, db, transactions_rx) {
            Ok(worker) => service_group.push(worker),
            Err(e) => {
                tracing::error!("failed to create worker: {}", e.to_string());
                return Err(e.into());
            }
        },
        InstanceConfig::Primary(_config) => todo!(),
    };

    service_group.start_and_drive_to_end().await?;

    Ok(())

}

#[derive(Debug, Clone)]
struct TransactionSender {
    transactions_tx: Sender<Transaction>,
}


#[async_trait::async_trait]
impl Service for TransactionSender {
    async fn start(&mut self, join_set: &mut JoinSet<anyhow::Result<()>>) -> anyhow::Result<()> {
        let transactions_tx = self.transactions_tx.clone();
        join_set.spawn(async move {
            let mut service = TransactionSender {
                transactions_tx,
            };
            service.run_forever().await?;
            Ok(())
        });
        Ok(())
    }
}

impl TransactionSender {
    async fn run_forever(&mut self) -> anyhow::Result<()> {
        let transaction = Transaction {
            data: vec![1; 100]
        };
    
        loop {
            if crossterm::event::poll(std::time::Duration::from_millis(100)).unwrap() {
                if let crossterm::event::Event::Key(key) = crossterm::event::read().unwrap() {
                    match key.code {
                        crossterm::event::KeyCode::Char('t') => {
                            self.transactions_tx.send(transaction.clone()).await.unwrap();
                            tracing::info!("transaction sent");
                        },
                        _ => {}
                    }
                }
            }
        }
    }
}
