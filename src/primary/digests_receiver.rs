use std::sync::Arc;

use proc_macros::Spawn;
use tokio::sync::{broadcast, Mutex};
use tokio_util::sync::CancellationToken;

use crate::{
    db::{self, Db},
    types::{
        batch::BatchId,
        network::{ObjectSource, ReceivedObject},
        traits::AsHex,
    },
    utils::CircularBuffer,
};

#[derive(Spawn)]
pub(crate) struct DigestReceiver {
    pub digest_rx: broadcast::Receiver<ReceivedObject<(BatchId, ObjectSource)>>,
    pub buffer: Arc<Mutex<CircularBuffer<BatchId>>>,
    pub db: Arc<Db>,
}

impl DigestReceiver {
    pub async fn run(mut self) -> anyhow::Result<()> {
        loop {
            let digest = self.digest_rx.recv().await?;
            self.db.insert(
                db::Column::Digests,
                &digest.object.0 .0.as_hex_string(),
                &digest.object.0,
            )?;
            // Dont create a header with other nodes batches
            if digest.object.1 == ObjectSource::SameNode {
                self.buffer.lock().await.push(digest.object.0.clone());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        primary::test_utils::fixtures::{
            check_storage_for_digests, launch_digest_receiver, random_digests,
        },
        types::network::{ObjectSource::SameNode, ReceivedObject},
    };
    use libp2p::PeerId;
    use rstest::rstest;
    use std::time::Duration;

    #[tokio::test]
    #[rstest]
    async fn test_single_digest_received() {
        let (digest_tx, buffer, db, _) =
            launch_digest_receiver("/tmp/test_single_digest_received_db");
        let digests = random_digests(1);
        {
            for digest in digests.clone() {
                digest_tx
                    .send(ReceivedObject::new((digest, SameNode), PeerId::random()))
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
                    .send(ReceivedObject::new((digest, SameNode), PeerId::random()))
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
                    .send(ReceivedObject::new((digest, SameNode), PeerId::random()))
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
                    .send(ReceivedObject::new((digest, SameNode), PeerId::random()))
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
                    .send(ReceivedObject::new((digest, SameNode), PeerId::random()))
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
                    .send(ReceivedObject::new((digest, SameNode), PeerId::random()))
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
                    .send(ReceivedObject::new((digest, SameNode), PeerId::random()))
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
                    .send(ReceivedObject::new((digest, SameNode), PeerId::random()))
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
