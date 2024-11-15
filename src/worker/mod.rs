use std::{os::unix::net::SocketAddr, time::Duration};
use blake3::{self, Hash};

use crate::types::{Digest, Transaction};

pub struct WorkerConfig {
    
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
    pub async fn spawn(_worker_cfg: WorkerConfig, batch_maker_cfg: BatchMakerConfig) {
        let (batches_tx, mut batches_rx) = tokio::sync::mpsc::channel(20);
        tokio::spawn(async move {batch_maker_task(batch_maker_cfg, batches_tx).await;});
        
        loop {
            match batches_rx.recv().await {
                Some(txs) => {
                    let digest = blake3::hash(txs.iter().flat_map(|txs| txs.as_bytes()).collect::<Vec<u8>>().as_slice());
                    // send digest to primary
                    // broadcast batches
                },
                None => todo!(),
            }
        }
    }

    pub fn make_batches() {
        todo!()
    }

    pub fn produce_digest() {
        
        hash = blake3::hash(input);
    }

    pub fn send_digest() {
        todo!()
    }

    pub fn broadcast() {
        todo!()
    }
}

struct BatchMakerConfig {
    batch_size: usize,
    batch_timeout: std::time::Duration,
    transactions_rx: tokio::sync::mpsc::Receiver<Transaction>
}

async fn batch_maker_task(mut config: BatchMakerConfig, batches_tx: tokio::sync::mpsc::Sender<Vec<Transaction>>) {
    let mut current_batch: Vec<Transaction> = vec![];
    let timer = tokio::time::sleep(config.batch_timeout);
    tokio::pin!(timer);
    loop {
        tokio::select! {
            Some(transaction) = config.transactions_rx.recv() => {
                current_batch.push(transaction);
                let batch_size = current_batch.iter().flat_map(|tx| tx.as_bytes()).count();
                if batch_size >= config.batch_size {
                    batches_tx.send(std::mem::take(&mut current_batch)).await;
                    timer.as_mut().reset(tokio::time::Instant::now() + config.batch_timeout);
                }
            },
            () = &mut timer => {
                if !current_batch.is_empty() {
                    batches_tx.send(std::mem::take(&mut current_batch)).await;
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