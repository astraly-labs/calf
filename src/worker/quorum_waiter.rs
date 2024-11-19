use libp2p::PeerId;
use std::sync::Arc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::{
    db::Db,
    types::{NetworkRequest, ReceivedAcknowledgment, RequestPayload, TxBatch},
};

struct WaitingBatch {
    ack_number: u32,
    batch: TxBatch,
    digest: blake3::Hash,
    timestamp: tokio::time::Instant,
}

impl WaitingBatch {
    fn new(batch: TxBatch) -> anyhow::Result<Self> {
        let digest = blake3::hash(&bincode::serialize(&batch)?);
        Ok(Self {
            ack_number: 0,
            batch,
            digest,
            timestamp: tokio::time::Instant::now(),
        })
    }
}

pub struct QuorumWaiter {
    batches_rx: tokio::sync::broadcast::Receiver<TxBatch>,
    acknowledgments_rx: tokio::sync::mpsc::Receiver<ReceivedAcknowledgment>,
    quorum_threshold: u32,
    digest_tx: tokio::sync::mpsc::Sender<NetworkRequest>,
    db: Arc<Db>,
    quorum_timeout: u128,
}

impl QuorumWaiter {
    #[must_use]
    pub fn spawn(
        batches_rx: tokio::sync::broadcast::Receiver<TxBatch>,
        acknowledgments_rx: tokio::sync::mpsc::Receiver<ReceivedAcknowledgment>,
        digest_tx: tokio::sync::mpsc::Sender<NetworkRequest>,
        db: Arc<Db>,
        quorum_threshold: u32,
        quorum_timeout: u128,
        cancellation_token: CancellationToken,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let res = cancellation_token
                .run_until_cancelled(
                    Self {
                        batches_rx,
                        acknowledgments_rx,
                        quorum_threshold,
                        digest_tx,
                        db,
                        quorum_timeout,
                    }
                    .run(),
                )
                .await;

            match res {
                Some(res) => {
                    match res {
                        Ok(_) => {
                            tracing::info!("Quorum Waiter finnished successfully");
                        }
                        Err(e) => {
                            tracing::error!("Quorum Waiter finished with error: {:?}", e);
                        }
                    };
                    cancellation_token.cancel();
                }
                None => {
                    tracing::info!("Quorum Waiter cancelled");
                }
            }
        })
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        let mut batches = vec![];
        loop {
            tokio::select! {
                Ok(batch) = self.batches_rx.recv() => {
                    let waiting_batch = match WaitingBatch::new(batch) {
                        Ok(waiting_batch) => waiting_batch,
                        Err(e) => {
                            tracing::error!("Failed to create waiting batch: {:?}", e);
                            continue;
                        }
                    };
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
                Some(ack) = self.acknowledgments_rx.recv() => {
                    let ack = ack.acknoledgement;
                    match batches.iter().position(|b| b.digest.as_bytes() == ack.as_slice()) {
                        Some(batch_index) => {
                            let batch = &mut batches[batch_index];
                            batch.ack_number += 1;
                            if batch.ack_number >= self.quorum_threshold {
                                tracing::info!("Batch is now confirmed: {:?}", batch.digest);
                                let primary_id = PeerId::from_bytes(&[0;32])?; // TODO: get primary id
                                self.digest_tx.send(NetworkRequest::SendTo(primary_id, RequestPayload::Digest(*batch.digest.as_bytes()))).await?;
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
