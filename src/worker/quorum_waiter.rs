use libp2p::PeerId;
use proc_macros::Spawn;
use std::{collections::HashSet, sync::Arc};
use tokio_util::sync::CancellationToken;

use crate::{
    db::Db,
    types::{
        batch::Batch,
        network::{NetworkRequest, ReceivedObject, RequestPayload},
        traits::Hash,
        transaction::Transaction,
        Acknowledgment, Digest,
    },
};

#[derive(Debug)]
struct WaitingBatch {
    acknowledgers: HashSet<PeerId>,
    batch: Batch<Transaction>,
    digest: Digest,
    timestamp: tokio::time::Instant,
}

impl WaitingBatch {
    fn new(batch: Batch<Transaction>) -> anyhow::Result<Self> {
        Ok(Self {
            acknowledgers: HashSet::new(),
            digest: batch.digest(),
            batch,
            timestamp: tokio::time::Instant::now(),
        })
    }
}

#[derive(Spawn)]
pub(crate) struct QuorumWaiter {
    batches_rx: tokio::sync::broadcast::Receiver<Batch<Transaction>>,
    acknowledgments_rx: tokio::sync::mpsc::Receiver<ReceivedObject<Acknowledgment>>,
    quorum_threshold: u32,
    network_tx: tokio::sync::mpsc::Sender<NetworkRequest>,
    db: Arc<Db>,
    quorum_timeout: u128,
}

impl QuorumWaiter {
    pub async fn run(mut self) -> anyhow::Result<()> {
        let mut batches = vec![];
        loop {
            tokio::select! {
                Ok(batch) = self.batches_rx.recv() => {
                    tracing::info!("ℹ️ received a new batch");
                    let waiting_batch = match WaitingBatch::new(batch) {
                        Ok(waiting_batch) => waiting_batch,
                        Err(_) => {
                            continue;
                        }
                    };
                    if !batches.iter().any(|elm: &WaitingBatch| {
                        elm.digest == waiting_batch.digest
                    }) {
                        batches.push(waiting_batch);
                    }
                    let now = tokio::time::Instant::now();
                    batches.retain(|batch| now.duration_since(batch.timestamp).as_millis() < self.quorum_timeout);
                },
                Some(ack) = self.acknowledgments_rx.recv() => {
                    let (ack, sender) = (ack.object, ack.sender);
                    if let Some(batch_index) = batches.iter().position(|b| ack.verify(&b.digest)) {
                        tracing::info!("✅ received an acknowledgment");
                        let batch = &mut batches[batch_index];
                            if !batch.acknowledgers.insert(sender) {
                                tracing::warn!("Duplicate acknowledgment from peer: {}", sender);
                            }
                            if batch.acknowledgers.len() as u32 >= self.quorum_threshold {
                                tracing::info!("sending a digest to the primary: {}", hex::encode(batch.digest));
                                self.network_tx.send(NetworkRequest::SendToPrimary(RequestPayload::Digest(batch.digest))).await?;
                                tracing::info!("{:?} batches remaining 1", batches.len());
                                let _ = self.insert_batch_in_db(batches.remove(batch_index));
                                tracing::info!("{:?} batches remaining", batches.len());
                            }
                    };
                }
                else => {
                    tracing::error!("all senders dropped: quorum waiter");
                    break Err(anyhow::anyhow!("all senders dropped"));
                }
            }
        }
    }

    fn insert_batch_in_db(&mut self, batch: WaitingBatch) -> anyhow::Result<()> {
        match self.db.insert(
            crate::db::Column::Batches,
            &hex::encode(batch.digest),
            &batch.batch,
        ) {
            Ok(_) => {
                tracing::info!("💾 batch inserted in DB");
                Ok(())
            }
            Err(e) => {
                tracing::error!("Failed to insert batch in DB: {:?}", e);
                Err(e.into())
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::QuorumWaiter;
    use crate::{
        db::Db,
        types::{
            batch::Batch,
            network::{NetworkRequest, ReceivedObject, RequestPayload},
            traits::{Hash, Random},
            transaction::Transaction,
            Acknowledgment,
        },
    };
    use libp2p::PeerId;
    use std::{sync::Arc, time::Duration};

    type QuorumWaiterFixture = (
        tokio::sync::broadcast::Sender<Batch<Transaction>>,
        tokio::sync::mpsc::Sender<ReceivedObject<Acknowledgment>>,
        tokio::sync::mpsc::Receiver<NetworkRequest>,
        tokio_util::sync::CancellationToken,
        Arc<Db>,
        tokio::task::JoinHandle<()>,
    );

    fn lauch_quorum_waiter(db_path: &str) -> QuorumWaiterFixture {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .with_test_writer()
            .try_init();
        let (batches_tx, batches_rx) = tokio::sync::broadcast::channel(100);
        let (acknowledgments_tx, acknowledgments_rx) = tokio::sync::mpsc::channel(100);
        let (network_tx, network_rx) = tokio::sync::mpsc::channel(100);
        let db = Db::new(db_path.into()).expect("failed to open db");
        let quorum_threshold = 2;
        let quorum_timeout = 1000;
        let cancellation_token = tokio_util::sync::CancellationToken::new();
        let cancellation_token_clone = cancellation_token.clone();
        let db_waiter = Arc::new(db);
        let db_test = db_waiter.clone();
        let handle = QuorumWaiter::spawn(
            cancellation_token_clone,
            batches_rx,
            acknowledgments_rx,
            quorum_threshold,
            network_tx,
            db_waiter,
            quorum_timeout,
        );
        (
            batches_tx,
            acknowledgments_tx,
            network_rx,
            cancellation_token,
            db_test,
            handle,
        )
    }

    #[rstest::rstest]
    #[tokio::test(start_paused = true)]
    async fn test_cancelled() {
        let (_, _, _, cancellation_token, _, handle) = lauch_quorum_waiter("/tmp/test_db_0");
        cancellation_token.cancel();
        handle.await.expect("failed to await handle");
    }

    #[rstest::rstest]
    #[tokio::test(start_paused = true)]
    async fn test_quorum_received() {
        let (batches_tx, acknowledgments_tx, mut digest_rx, token, _, handle) =
            lauch_quorum_waiter("/tmp/test_db_1");

        let batch = Batch::random(10);
        batches_tx
            .send(batch.clone())
            .expect("failed to send batch");
        tokio::time::sleep(Duration::from_millis(1)).await;
        for _ in 0..3 {
            let ack = Acknowledgment::from(&batch);
            acknowledgments_tx
                .send(ReceivedObject::new(ack, PeerId::random()))
                .await
                .expect("failed to send ack");
        }
        let res = tokio::time::timeout(Duration::from_millis(10), digest_rx.recv()).await;
        assert!(
            res.unwrap().unwrap()
                == NetworkRequest::SendToPrimary(RequestPayload::Digest(batch.digest()))
        );
        token.cancel();
        handle.await.expect("failed to await handle");
    }

    #[rstest::rstest]
    #[tokio::test(start_paused = true)]
    async fn test_batch_forgotten_after_quorum_received() {
        let (batches_tx, acknowledgments_tx, mut digest_rx, token, _, handle) =
            lauch_quorum_waiter("/tmp/test_db_2");

        let batch = Batch::random(10);
        let digest = batch.digest();

        batches_tx
            .send(batch.clone())
            .expect("failed to send batch");
        tokio::time::sleep(Duration::from_millis(1)).await;
        for _ in 0..3 {
            let ack = Acknowledgment::from(&batch);
            acknowledgments_tx
                .send(ReceivedObject::new(ack, PeerId::random()))
                .await
                .expect("failed to send ack");
        }
        let res = tokio::time::timeout(Duration::from_millis(10), digest_rx.recv()).await;
        assert!(
            res.unwrap().unwrap() == NetworkRequest::SendToPrimary(RequestPayload::Digest(digest))
        );

        for _ in 0..3 {
            let ack = Acknowledgment::from(&batch);
            acknowledgments_tx
                .send(ReceivedObject::new(ack, PeerId::random()))
                .await
                .expect("failed to send ack");
        }
        let res = tokio::time::timeout(Duration::from_millis(10), digest_rx.recv()).await;

        assert!(res.is_err());
        token.cancel();
        handle.await.expect("failed to await handle");
    }

    #[rstest::rstest]
    #[tokio::test(start_paused = true)]
    async fn test_multiple_batches_single_quorum() {
        let (batches_tx, acknowledgments_tx, mut digest_rx, token, _, handle) =
            lauch_quorum_waiter("/tmp/test_db_4");

        let batches = vec![Batch::random(10); 30];
        let digest = batches[9].digest();
        for batch in batches {
            batches_tx.send(batch).expect("failed to send batch");
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        for _ in 0..3 {
            let ack = Acknowledgment::from_digest(&digest);
            acknowledgments_tx
                .send(ReceivedObject::new(ack, PeerId::random()))
                .await
                .expect("failed to send acknowledgment");
        }

        let res = tokio::time::timeout(Duration::from_millis(10), digest_rx.recv())
            .await
            .unwrap()
            .unwrap();

        assert!(res == NetworkRequest::SendToPrimary(RequestPayload::Digest(digest)));
        let res = tokio::time::timeout(Duration::from_millis(10), digest_rx.recv()).await;

        assert!(res.is_err());
        token.cancel();
        handle.await.expect("failed to await handle");
    }

    #[rstest::rstest]
    #[tokio::test(start_paused = true)]
    async fn test_multiple_batches_multiple_quorum() {
        let (batches_tx, acknowledgments_tx, mut digest_rx, token, _, handle) =
            lauch_quorum_waiter("/tmp/test_db_5");

        let batches = (0..10)
            .into_iter()
            .map(|_| Batch::random(10))
            .collect::<Vec<Batch<Transaction>>>();
        for batch in &batches {
            batches_tx
                .send(batch.clone())
                .expect("failed to send batch");
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        tracing::info!("all batches sent");
        for batch in &batches {
            for _ in 0..3 {
                let ack = Acknowledgment::from(batch);
                acknowledgments_tx
                    .send(ReceivedObject::new(ack, PeerId::random()))
                    .await
                    .expect("failed to send acknowledgment");
            }
        }
        for _ in batches {
            let res = tokio::time::timeout(Duration::from_millis(10), digest_rx.recv()).await;
            assert!(res.is_ok());
        }
        token.cancel();
        handle.await.expect("failed to await handle");
    }

    #[rstest::rstest]
    #[tokio::test(start_paused = true)]
    async fn test_duplicates_acknowledgements() {
        let (batches_tx, acknowledgments_tx, mut digest_rx, token, _, handle) =
            lauch_quorum_waiter("/tmp/test_db_6");

        let batch = Batch::random(10);
        let ack = ReceivedObject::new(Acknowledgment::from(&batch), PeerId::random());
        batches_tx.send(batch).expect("failed to send batch");
        tokio::time::sleep(Duration::from_millis(1)).await;
        for _ in 0..10 {
            acknowledgments_tx
                .send(ack.clone())
                .await
                .expect("failed to send ack");
        }
        let res = tokio::time::timeout(Duration::from_millis(10), digest_rx.recv()).await;
        assert!(res.is_err());
        token.cancel();
        handle.await.expect("failed to await handle");
    }
}
