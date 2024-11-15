use clap::Parser;
use dispatcher::Dispatcher;
use worker::{BatchMakerConfig, Worker, WorkerConfig};

pub mod dispatcher;
pub mod primary;
pub mod types;
pub mod worker;
pub mod cli;
pub mod config;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {

    config.load_from_file("network.json".to_string()).unwrap();

    let args = cli::Args::parse();

    let (transactions_tx, transactions_rx) = tokio::sync::mpsc::channel::<Vec<u8>>(200);
    let batch_cfg_1 = BatchMakerConfig {
        batch_size: 1000,
        batch_timeout: std::time::Duration::from_millis(5000),
        transactions_rx
    };

    let (trs_tx, trs_rx) = tokio::sync::mpsc::channel::<Vec<u8>>(200);
    let batch_cfg_2 = BatchMakerConfig {
        batch_size: 1000,
        batch_timeout: std::time::Duration::from_millis(5000),
        transactions_rx : trs_rx,
    };

    let dispatcher = Dispatcher::spawn("127.0.0.1:7878".to_string(), vec![transactions_tx, trs_tx]);
    let worker1 = Worker::spawn(WorkerConfig {id : 1}, batch_cfg_1);
    let worker2 = Worker::spawn(WorkerConfig {id : 2}, batch_cfg_2);

    tokio::try_join!(
        dispatcher,
        worker1,
        worker2,
    ).unwrap();

}


#[cfg(test)]
mod test {
    pub fn test_e2e() {
        // let tx_mock: [u8] = [0,1,2,3,4,4,5,6,7,6,7,8,9,10,22,1,0,2];

        //send this tx to dispatch
        //ensure its sent to worker 
    }
}