use std::sync::Arc;
use tokio::task::JoinHandle;

use crate::{
    db::Db, safe_send, types::{ReceivedAcknoledgement, TxBatch}
};

struct WaitingBatch {
    ack_number: u32,
    batch: TxBatch,
    digest: blake3::Hash,
    timestamp: tokio::time::Instant,
}

impl WaitingBatch {
    fn new(batch: TxBatch) -> Self {
        let digest = blake3::hash(&bincode::serialize(&batch).expect("batch hash failed"));
        Self {
            ack_number: 0,
            batch,
            digest,
            timestamp: tokio::time::Instant::now(),
        }
    }
}

pub struct QuorumWaiter {
    batches_rx: tokio::sync::broadcast::Receiver<TxBatch>,
    acknolwedgements_rx: tokio::sync::mpsc::Receiver<ReceivedAcknoledgement>,
    quorum_threshold: u32,
    digest_tx: tokio::sync::mpsc::Sender<blake3::Hash>,
    db: Arc<Db>,
    quorum_timeout: u128,
}

impl QuorumWaiter {
    #[must_use]
    pub fn spawn(
        batches_rx: tokio::sync::broadcast::Receiver<TxBatch>,
        acknolwedgements_rx: tokio::sync::mpsc::Receiver<ReceivedAcknoledgement>,
        digest_tx: tokio::sync::mpsc::Sender<blake3::Hash>,
        db: Arc<Db>,
        quorum_threshold: u32,
        quorum_timeout: u128,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            Self {
                batches_rx,
                acknolwedgements_rx,
                quorum_threshold,
                digest_tx,
                db,
                quorum_timeout,
            }
            .run()
            .await;
        })
    }

    pub async fn run(mut self) {
        let mut batches = vec![];
        loop {
            tokio::select! {
                Ok(batch) = self.batches_rx.recv() => {
                    let waiting_batch = WaitingBatch::new(batch);
                    if batches.iter().any(|elm: &WaitingBatch| {
                        elm.digest.as_bytes() == waiting_batch.digest.as_bytes()
                    }) {
                        tracing::warn!("Received duplicate batch");
                    }
                    else {
                        batches.push(waiting_batch);
                        tracing::info!("Received new batch");
                    }
                    let now = tokio::time::Instant::now();
                    //perfectible ? rayon ? une "liste de timers" ?
                    for i in 0..batches.len() {
                        if now.duration_since(batches[i].timestamp).as_millis() > self.quorum_timeout {
                            tracing::warn!("Batch timed out: {:?}", batches[i].digest);
                            batches.remove(i);
                        }
                    }
                },
                Some(ack) = self.acknolwedgements_rx.recv() => {
                    let ack = ack.acknoledgement;
                    match batches.iter().position(|b| b.digest.as_bytes() == ack.as_slice()) {
                        Some(batch_index) => {
                            let batch = &mut batches[batch_index];
                            batch.ack_number += 1;
                            if batch.ack_number >= self.quorum_threshold {
                                tracing::info!("Batch is now confirmed: {:?}", batch.digest);
                                safe_send!(self.digest_tx, batch.digest, "failed to send digest from quorum waiter");
                                match self.db.insert(crate::db::Column::Batches, &batch.digest.to_string(), &batch.batch) {
                                    Ok(_) => {
                                        tracing::info!("Batch inserted in DB");
                                    },
                                    Err(e) => {
                                        tracing::error!("Failed to insert batch in DB: {:?}", e);
                                    }
                                }
                                batches.remove(batch_index);
                            }
                        },
                        _ => {
                        }
                    };
                }
            }
        }
    }
}
