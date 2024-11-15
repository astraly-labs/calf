use config::{InstanceConfig, NetworkInfos, PrimaryInfo, WorkerConfig};
use worker::Worker;

pub mod cli;
pub mod config;
pub mod db;
pub mod dispatcher;
pub mod primary;
pub mod types;
pub mod worker;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let config = InstanceConfig::Worker(WorkerConfig {
        keypair: "0xKeypair".into(),
        pubkey: "0xPubkey".into(),
        address: "127.0.0.1:327".into(),
        timeout: 1000,
        batch_size: 1000,
        validator_pubkey: "0xValidatorPubkey".into(),
        id: 0,
        primary: PrimaryInfo {
            id: 0,
            pubkey: "0xPrimaryPubkey".into(),
            address: "0.0.0.0:1986".into(),
        },
    });

    config.write_to_file("config.json".into())?;

    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();
    let network_infos_path = "network.json";
    let instance_config_path = "config.json";

    let network = match NetworkInfos::load_from_file(network_infos_path) {
        Ok(infos) => infos,
        Err(e) => {
            tracing::error!(
                "failed to parse network infos from {}: {}",
                network_infos_path,
                e.to_string()
            );
            return Err(e);
        }
    };

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

    match config {
        InstanceConfig::Worker(config) => match Worker::new(config, network, None) {
            Ok(worker) => match worker.spawn().await {
                Ok(_) => Ok(()),
                Err(e) => {
                    tracing::error!("failed to spawn worker: {}", e.to_string());
                    return Err(e.into());
                }
            },
            Err(e) => {
                tracing::error!("failed to create worker: {}", e.to_string());
                return Err(e.into());
            }
        },
        InstanceConfig::Primary(_config) => todo!(),
    }
}

#[cfg(test)]
mod test {
    pub fn _test_e2e() {
        todo!()
    }
}
