use std::sync::Arc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::{
    db::Db,
    types::{ReceivedAcknowledgment, TxBatch},
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
    digest_tx: tokio::sync::mpsc::Sender<blake3::Hash>,
    db: Arc<Db>,
    quorum_timeout: u128,
}

impl QuorumWaiter {
    #[must_use]
    pub fn spawn(
        batches_rx: tokio::sync::broadcast::Receiver<TxBatch>,
        acknowledgments_rx: tokio::sync::mpsc::Receiver<ReceivedAcknowledgment>,
        digest_tx: tokio::sync::mpsc::Sender<blake3::Hash>,
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
                                self.digest_tx.send(batch.digest).await?;
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
                else => {
                    tracing::error!("all senders dropped");
                    break Err(anyhow::anyhow!("all senders dropped"));
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::{sync::Arc, time::Duration};
    use crate::{db::Db, types::{ReceivedAcknowledgment, TxBatch}};
    use super::QuorumWaiter;

    type QuorumWaiterFixture = (
        tokio::sync::broadcast::Sender<TxBatch>,
        tokio::sync::mpsc::Sender<ReceivedAcknowledgment>,
        tokio::sync::mpsc::Receiver<blake3::Hash>,
        tokio_util::sync::CancellationToken,
        Arc<Db>,
        tokio::task::JoinHandle<()>
    );

    fn lauch_quorum_waiter(db_path: &str) -> QuorumWaiterFixture {
        let (batches_tx, batches_rx) = tokio::sync::broadcast::channel(10);
        let (acknowledgments_tx, acknowledgments_rx) = tokio::sync::mpsc::channel(10);
        let (digest_tx, digest_rx) = tokio::sync::mpsc::channel(10);
        let db = Db::new(db_path.into()).expect("failed to open db");
        let quorum_threshold = 2;
        let quorum_timeout = 1000;
        let cancellation_token = tokio_util::sync::CancellationToken::new();
        let cancellation_token_clone = cancellation_token.clone();
        let db_waiter = Arc::new(db);
        let db_test = db_waiter.clone();
        let handle = QuorumWaiter::spawn(
            batches_rx,
            acknowledgments_rx,
            digest_tx,
            db_waiter,
            quorum_threshold,
            quorum_timeout,
            cancellation_token_clone,
        );
        (batches_tx, acknowledgments_tx, digest_rx, cancellation_token, db_test, handle)
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn test_cancelled() {
        let (_, _, _, cancellation_token, _, handle) = lauch_quorum_waiter("/tmp/test_db_0");
        cancellation_token.cancel();
        handle.await.expect("failed to await handle");
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn test_quorum_received() {
        let (batches_tx, acknowledgments_tx, mut digest_rx, _, _, _) = lauch_quorum_waiter("/tmp/test_db_1");

        let batch = TxBatch::default();
        let digest = blake3::hash(&bincode::serialize(&batch).expect("failed to serialize batch"));
        let ack = ReceivedAcknowledgment {
            acknoledgement: digest.as_bytes().to_vec(),
            sender: libp2p::PeerId::random(),
        };

        batches_tx.send(batch).expect("failed to send batch");
        for _ in 0..3 {
            acknowledgments_tx.send(ack.clone()).await.expect("failed to send ack");
        }
        let res = tokio::time::timeout(Duration::from_millis(10), digest_rx.recv()).await;
        assert!(res.unwrap().unwrap().as_bytes() == digest.as_bytes());
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn test_batch_forgotten_after_quorum_quorum_received() {
        let (batches_tx, acknowledgments_tx, mut digest_rx, _, _, _) = lauch_quorum_waiter("/tmp/test_db_1");

        let batch = TxBatch::default();
        let digest = blake3::hash(&bincode::serialize(&batch).expect("failed to serialize batch"));
        let ack = ReceivedAcknowledgment {
            acknoledgement: digest.as_bytes().to_vec(),
            sender: libp2p::PeerId::random(),
        };

        batches_tx.send(batch).expect("failed to send batch");
        for _ in 0..3 {
            acknowledgments_tx.send(ack.clone()).await.expect("failed to send ack");
        }
        let res = tokio::time::timeout(Duration::from_millis(10), digest_rx.recv()).await;
        assert!(res.unwrap().unwrap().as_bytes() == digest.as_bytes());
        for _ in 0..3 {
            acknowledgments_tx.send(ack.clone()).await.expect("failed to send ack");
        }
        let res = tokio::time::timeout(Duration::from_millis(100), digest_rx.recv()).await;
        assert!(res.is_err());
    }

}
