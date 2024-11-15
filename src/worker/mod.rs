use std::{os::unix::net::SocketAddr, time::Duration};
use blake3::{self, Hash};
use tokio::task::JoinHandle;

use crate::types::{Digest, Transaction};

pub struct WorkerConfig {
    pub id: u32,
}

pub struct Worker {
    id: u16,
    channel_from_dispatcher: tokio::sync::mpsc::Receiver<Transaction>,
    channel_to_primary: tokio::sync::mpsc::Sender<Digest>,
    current_batch : Vec<Transaction>,
    //TODO: maybe add worker adress
    other_workers: Vec<SocketAddr>
}

impl Worker {
    pub fn spawn(worker_cfg: WorkerConfig, batch_maker_cfg: BatchMakerConfig) -> JoinHandle<()>{
        tokio::spawn(async move {tracing::info!("Worker {:?} Created", worker_cfg.id);
            let (batches_tx, mut batches_rx) = tokio::sync::mpsc::channel(20);
            tracing::info!("spawning batcher");
            tokio::spawn(async move {batch_maker_task(worker_cfg.id, batch_maker_cfg, batches_tx).await;});
            tracing::info!("Batcher Created");
            
            loop {
                match batches_rx.recv().await {
                    Some(txs) => {
                        let batch = txs.iter().flat_map(|txs| txs).copied().collect::<Vec<u8>>();
                        let digest = blake3::hash(batch.as_slice());
                        // send digest to primary
                        tracing::info!("Worker {:?} : Digest {:?}", worker_cfg.id, digest);
                        // Worker::send_digest(digest);
                        // broadcast batches
                        tracing::info!("Worker {:?} :Batch {:?}", worker_cfg.id, batch);
                        // Worker::broadcast_batch(batch);
                    },
                    None => todo!(),
                }
            }
        })
    }

    pub fn send_digest(hash : Hash) {
        todo!()
    }

    pub fn broadcast_batch(batch : Vec<u8>) {
        todo!()
    }
}

pub struct BatchMakerConfig {
    pub batch_size: usize,
    pub batch_timeout: std::time::Duration,
    pub transactions_rx: tokio::sync::mpsc::Receiver<Vec<u8>>
}

async fn batch_maker_task(id: u32, mut config: BatchMakerConfig, batches_tx: tokio::sync::mpsc::Sender<Vec<Vec<u8>>>) {
    let mut current_batch: Vec<Vec<u8>> = vec![];
    let timer = tokio::time::sleep(config.batch_timeout);
    tokio::pin!(timer);
    loop {
        tokio::select! {
            Some(transaction) = config.transactions_rx.recv() => {
                println!("Batch Maker {:?} received : {:?}", id, transaction);
                current_batch.push(transaction);
                let batch_size = current_batch.iter().flat_map(|tx| tx).count();
                if batch_size >= config.batch_size {
                    println!("SENDING BCOZ 2 BIG");
                    batches_tx.send(std::mem::take(&mut current_batch)).await.unwrap();
                    timer.as_mut().reset(tokio::time::Instant::now() + config.batch_timeout);
                }
            },
            () = &mut timer => {
                if !current_batch.is_empty() {
                    println!("SENDING BCOZ 2 LONG");
                    batches_tx.send(std::mem::take(&mut current_batch)).await.unwrap();
                    timer.as_mut().reset(tokio::time::Instant::now() + config.batch_timeout);
                }
            }
        }
    }
}

#[cfg(test)]
mod test {

    #[test]
    fn test_digest(){
        todo!()
    }

    #[test]
    fn test_batch(){
        todo!()
    }

    #[test]
    fn test_send_batch(){
        todo!()
    }

    #[test]
    fn test_broadcast(){
        todo!()
    }

    #[test]
    fn test_batch_maker(){
        todo!()
    }
}