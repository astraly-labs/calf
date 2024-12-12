use std::sync::Arc;

use proc_macros::Spawn;
use tokio::sync::{broadcast, Mutex};
use tokio_util::sync::CancellationToken;

use crate::{
    db::{self, Db},
    types::{batch::BatchId, network::ReceivedObject, traits::AsHex},
    utils::CircularBuffer,
};

#[derive(Spawn)]
pub(crate) struct DigestReceiver {
    pub digest_rx: broadcast::Receiver<ReceivedObject<BatchId>>,
    pub buffer: Arc<Mutex<CircularBuffer<BatchId>>>,
    pub db: Arc<Db>,
}

impl DigestReceiver {
    pub async fn run(mut self) -> anyhow::Result<()> {
        loop {
            let digest = self.digest_rx.recv().await?;
            self.db.insert(
                db::Column::Digests,
                &digest.object.0.as_hex_string(),
                &digest.object,
            )?;
            self.buffer.lock().await.push(digest.object.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::DigestReceiver;
    use crate::{
        db::{Column, Db},
        types::{
            batch::{Batch, BatchId},
            network::ReceivedObject,
            traits::{AsHex, Hash, Random},
            transaction::Transaction,
        },
        utils::CircularBuffer,
    };
    use libp2p::PeerId;
    use rstest::rstest;
    use std::{sync::Arc, time::Duration};
    use tokio::sync::{broadcast, Mutex};
    use tokio_util::sync::CancellationToken;

    const CHANNEL_CAPACITY: usize = 1000;
    const BUFFER_CAPACITY: usize = 10;
    const RANDOM_BATCH_SIZE: usize = 10;

    type DigestReceiverFixture = (
        broadcast::Sender<ReceivedObject<BatchId>>,
        Arc<Mutex<CircularBuffer<BatchId>>>,
        Arc<Db>,
        CancellationToken,
    );

    fn launch_digest_receiver(db_path: &str) -> DigestReceiverFixture {
        let (digest_tx, digest_rx) = broadcast::channel(CHANNEL_CAPACITY);
        let buffer = Arc::new(Mutex::new(CircularBuffer::new(BUFFER_CAPACITY)));
        let db = Arc::new(Db::new(db_path.into()).unwrap());
        let buffer_clone = buffer.clone();
        let db_clone = db.clone();
        let token = CancellationToken::new();
        let token_clone = token.clone();
        let _ = tokio::spawn(async move {
            DigestReceiver::spawn(token_clone, digest_rx, buffer_clone, db_clone)
                .await
                .unwrap();
        });
        (digest_tx, buffer, db, token)
    }

    fn random_digests(count: usize) -> Vec<BatchId> {
        (0..count)
            .map(|_| {
                Batch::<Transaction>::random(RANDOM_BATCH_SIZE)
                    .digest()
                    .into()
            })
            .collect()
    }

    fn check_storage_for_digests(db: &Db, digests: &[BatchId]) {
        for digest in digests {
            let stored_digest: BatchId = db
                .get(Column::Digests, &digest.0.as_hex_string())
                .unwrap()
                .unwrap();
            assert_eq!(stored_digest, *digest);
        }
    }

    #[tokio::test]
    #[rstest]
    async fn test_single_digest_received() {
        let (digest_tx, buffer, db, _) =
            launch_digest_receiver("/tmp/test_single_digest_received_db");
        let digests = random_digests(1);
        {
            for digest in digests.clone() {
                digest_tx
                    .send(ReceivedObject::new(digest, PeerId::random()))
                    .unwrap();
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
            let mut buffer = buffer.lock().await;
            let drained = buffer.drain();
            assert_eq!(drained.len(), 1);
            assert_eq!(drained[0], digests[0]);
            check_storage_for_digests(&db, &digests);
        }
        {
            let digests = random_digests(1);
            for digest in digests.clone() {
                digest_tx
                    .send(ReceivedObject::new(digest, PeerId::random()))
                    .unwrap();
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
            let mut buffer = buffer.lock().await;
            let drained = buffer.drain();
            assert_eq!(drained.len(), 1);
            assert_eq!(drained[0], digests[0]);
            check_storage_for_digests(&db, &digests);
        }
    }

    #[tokio::test]
    #[rstest]
    async fn test_multiple_under_capacity_digests_received() {
        let (digest_tx, buffer, db, _) =
            launch_digest_receiver("/tmp/test_multiple_under_capacity_digests_received_db");
        let digests = random_digests(10);
        {
            for digest in digests.clone() {
                digest_tx
                    .send(ReceivedObject::new(digest, PeerId::random()))
                    .unwrap();
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
            let mut buffer = buffer.lock().await;
            let drained = buffer.drain();
            assert_eq!(drained.len(), 10);
            assert_eq!(drained, digests);
            check_storage_for_digests(&db, &digests);
        }
        {
            let digests = random_digests(10);
            for digest in digests.clone() {
                digest_tx
                    .send(ReceivedObject::new(digest, PeerId::random()))
                    .unwrap();
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
            let mut buffer = buffer.lock().await;
            let drained = buffer.drain();
            assert_eq!(drained.len(), 10);
            assert_eq!(drained, digests);
            check_storage_for_digests(&db, &digests);
        }
    }

    #[tokio::test]
    #[rstest]
    async fn test_multiple_over_capacity_digests_received() {
        let (digest_tx, buffer, db, _) =
            launch_digest_receiver("/tmp/test_multiple_over_capacity_digests_received_db");
        let digests = random_digests(20);
        {
            for digest in digests.clone() {
                digest_tx
                    .send(ReceivedObject::new(digest, PeerId::random()))
                    .unwrap();
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
            let mut buffer = buffer.lock().await;
            let drained = buffer.drain();
            assert_eq!(drained.len(), 10);
            assert_eq!(drained, digests[10..].to_vec());
            check_storage_for_digests(&db, &digests);
        }
        {
            let digests = random_digests(20);
            for digest in digests.clone() {
                digest_tx
                    .send(ReceivedObject::new(digest, PeerId::random()))
                    .unwrap();
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
            let mut buffer = buffer.lock().await;
            let drained = buffer.drain();
            assert_eq!(drained.len(), 10);
            assert_eq!(drained, digests[10..].to_vec());
            check_storage_for_digests(&db, &digests);
        }
    }

    #[tokio::test]
    #[rstest]
    async fn test_multiple_very_over_capacity_digests_received() {
        let (digest_tx, buffer, db, _) =
            launch_digest_receiver("/tmp/test_multiple_very_over_capacity_digests_received_db");
        let digests = random_digests(100);
        {
            for digest in digests.clone() {
                digest_tx
                    .send(ReceivedObject::new(digest, PeerId::random()))
                    .unwrap();
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
            let mut buffer = buffer.lock().await;
            let drained = buffer.drain();
            assert_eq!(drained.len(), 10);
            assert_eq!(drained, digests[90..].to_vec());
            check_storage_for_digests(&db, &digests);
        }
        {
            let digests = random_digests(100);
            for digest in digests.clone() {
                digest_tx
                    .send(ReceivedObject::new(digest, PeerId::random()))
                    .unwrap();
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
            let mut buffer = buffer.lock().await;
            let drained = buffer.drain();
            assert_eq!(drained.len(), 10);
            assert_eq!(drained, digests[90..].to_vec());
            check_storage_for_digests(&db, &digests);
        }
    }
}
