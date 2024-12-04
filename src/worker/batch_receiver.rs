use std::sync::Arc;

use proc_macros::Spawn;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::{
    db::{self, Db},
    types::{
        batch::Batch,
        network::{NetworkRequest, ReceivedObject, RequestPayload},
        traits::Hash,
        transaction::Transaction,
        Acknowledgment,
    },
};

#[derive(Spawn)]
pub(crate) struct BatchReceiver {
    batches_rx: mpsc::Receiver<ReceivedObject<Batch<Transaction>>>,
    requests_tx: mpsc::Sender<NetworkRequest>,
    db: Arc<Db>,
}

impl BatchReceiver {
    pub async fn run(mut self) -> anyhow::Result<()> {
        while let Some(batch) = self.batches_rx.recv().await {
            let digest = batch.object.digest();
            tracing::info!("Received batch from {}", batch.sender);
            self.requests_tx
                .send(NetworkRequest::SendTo(
                    batch.sender,
                    RequestPayload::Acknowledgment(Acknowledgment::from_digest(&digest)),
                ))
                .await?;
            self.requests_tx
                .send(NetworkRequest::SendToPrimary(RequestPayload::Digest(
                    digest,
                )))
                .await?;
            self.db
                .insert(db::Column::Batches, &hex::encode(digest), &batch.object)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use super::BatchReceiver;
    use crate::types::{
        batch::Batch,
        network::{NetworkRequest, ReceivedObject, RequestPayload},
        traits::Random,
        transaction::Transaction,
        Acknowledgment,
    };
    use libp2p::PeerId;

    type BatchReceiverFixture = (
        tokio::sync::mpsc::Sender<ReceivedObject<Batch<Transaction>>>,
        tokio::sync::mpsc::Receiver<NetworkRequest>,
        tokio::task::JoinHandle<()>,
        tokio_util::sync::CancellationToken,
    );

    fn launch_batch_receiver(db_path: &str) -> BatchReceiverFixture {
        let (batches_tx, batches_rx) = tokio::sync::mpsc::channel(100);
        let (requests_tx, requests_rx) = tokio::sync::mpsc::channel(100);
        let db = Arc::new(crate::db::Db::new(db_path.into()).expect("failed to open db"));
        let token = tokio_util::sync::CancellationToken::new();
        let handle = BatchReceiver::spawn(token.clone(), batches_rx, requests_tx, db);
        (batches_tx, requests_rx, handle, token)
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn test_single_batch_acknowledgement() {
        let (batches_tx, mut requests_rx, _, _) = launch_batch_receiver("/tmp/test_db_7");
        let batch = ReceivedObject::new(Batch::random(30), PeerId::random());
        let expected_request = NetworkRequest::SendTo(
            batch.sender,
            RequestPayload::Acknowledgment(Acknowledgment::from(&batch.object)),
        );
        batches_tx.send(batch).await.expect("failed to send batch");
        let res = requests_rx.recv().await.expect("failed to receive request");
        assert_eq!(res, expected_request);
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn test_cancelled() {
        let (_, _, handle, token) = launch_batch_receiver("/tmp/test_db_8");
        token.cancel();
        handle.await.expect("failed to await handle");
    }
}
