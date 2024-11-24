use std::sync::Arc;

use tokio::{sync::mpsc, task::JoinHandle};
use tokio_util::sync::CancellationToken;

use crate::{
    db::{self, Db},
    types::{Hash, NetworkRequest, ReceivedBatch, RequestPayload},
};

pub struct BatchReceiver {
    batches_rx: mpsc::Receiver<ReceivedBatch>,
    requests_tx: mpsc::Sender<NetworkRequest>,
    db: Arc<Db>,
}

impl BatchReceiver {
    #[must_use]
    pub fn spawn(
        batches_rx: mpsc::Receiver<ReceivedBatch>,
        requests_tx: mpsc::Sender<NetworkRequest>,
        db: Arc<Db>,
        cancellation_token: CancellationToken,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let res = cancellation_token
                .run_until_cancelled(
                    Self {
                        batches_rx,
                        requests_tx,
                        db,
                    }
                    .run(),
                )
                .await;

            match res {
                Some(res) => {
                    match res {
                        Ok(_) => {
                            tracing::info!("Batch Acknowledger finnished successfully");
                        }
                        Err(e) => {
                            tracing::error!("Batch Acknowledger finished with error: {:?}", e);
                        }
                    };
                    cancellation_token.cancel();
                }
                None => {
                    tracing::info!("Batch Acknowledger cancelled");
                }
            }
        })
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        while let Some(batch) = self.batches_rx.recv().await {
            tracing::info!("Received batch from {}", batch.sender);
            let digest = match batch.batch.digest() {
                Ok(digest) => digest,
                Err(e) => {
                    tracing::warn!("Failed to compute digest for a received batch: {:?}", e);
                    continue;
                }
            };
            self.requests_tx
                .send(NetworkRequest::SendTo(
                    batch.sender,
                    RequestPayload::Acknowledgment(digest),
                ))
                .await?;
            self.requests_tx
                .send(NetworkRequest::SendToPrimary(RequestPayload::Digest(
                    digest,
                )))
                .await?;
            self.db
                .insert(db::Column::Batches, &hex::encode(digest), &batch.batch)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use super::BatchReceiver;
    use crate::types::{Hash, NetworkRequest, ReceivedBatch, RequestPayload, TxBatch};
    use libp2p::PeerId;

    type BatchReceiverFixture = (
        tokio::sync::mpsc::Sender<ReceivedBatch>,
        tokio::sync::mpsc::Receiver<NetworkRequest>,
        tokio::task::JoinHandle<()>,
        tokio_util::sync::CancellationToken,
    );

    fn launch_batch_receiver(db_path: &str) -> BatchReceiverFixture {
        let (batches_tx, batches_rx) = tokio::sync::mpsc::channel(100);
        let (requests_tx, requests_rx) = tokio::sync::mpsc::channel(100);
        let db = Arc::new(crate::db::Db::new(db_path.into()).expect("failed to open db"));
        let token = tokio_util::sync::CancellationToken::new();
        let handle = BatchReceiver::spawn(batches_rx, requests_tx, db, token.clone());
        (batches_tx, requests_rx, handle, token)
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn test_single_batch_acknowledgement() {
        let (batches_tx, mut requests_rx, _, _) = launch_batch_receiver("/tmp/test_db_7");

        let batch = ReceivedBatch {
            sender: PeerId::random(),
            batch: TxBatch::default(),
        };

        let expected_request = NetworkRequest::SendTo(
            batch.sender,
            RequestPayload::Acknowledgment(batch.batch.digest().expect("failed to compute digest")),
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
