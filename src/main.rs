use config::{InstanceConfig, NetworkInfos};

pub mod cli;
pub mod config;
//pub mod db;
pub mod dispatcher;
pub mod primary;
pub mod types;
pub mod worker;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();
    let network_infos_path = "network.json";
    let instance_config_path = "config.json";

    let _network = match NetworkInfos::load_from_file(network_infos_path) {
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

    let _config = match InstanceConfig::load_from_file(instance_config_path) {
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

    Ok(())

    // match config {
    //     InstanceConfig::Worker(config) => match Worker::new(config, network, None) {
    //         Ok(worker) => match worker.spawn().await {
    //             Ok(_) => Ok(()),
    //             Err(e) => {
    //                 tracing::error!("failed to spawn worker: {}", e.to_string());
    //                 return Err(e.into());
    //             }
    //         },
    //         Err(e) => {
    //             tracing::error!("failed to create worker: {}", e.to_string());
    //             return Err(e.into());
    //         }
    //     },
    //     InstanceConfig::Primary(_config) => todo!(),
    // }
}

#[cfg(test)]
mod test {
    pub fn _test_e2e() {
        todo!()
    }
}
