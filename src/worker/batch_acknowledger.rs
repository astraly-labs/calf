use tokio::{sync::mpsc, task::JoinHandle};
use tokio_util::sync::CancellationToken;

use crate::types::{NetworkRequest, ReceivedBatch, RequestPayload};

pub struct BatchAcknowledger {
    batches_rx: mpsc::Receiver<ReceivedBatch>,
    resquests_tx: mpsc::Sender<NetworkRequest>,
}

impl BatchAcknowledger {
    #[must_use]
    pub fn spawn(
        batches_rx: mpsc::Receiver<ReceivedBatch>,
        resquests_tx: mpsc::Sender<NetworkRequest>,
        cancellation_token: CancellationToken,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let res = cancellation_token
                .run_until_cancelled(
                    Self {
                        batches_rx,
                        resquests_tx,
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
            let digest = blake3::hash(&bincode::serialize(&batch.batch)?);
            self.resquests_tx
                .send(NetworkRequest::SendTo(
                    batch.sender,
                    RequestPayload::Acknoledgment(digest.as_bytes().to_vec()),
                ))
                .await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::thread::JoinHandle;

    use libp2p::PeerId;
    use rstest::fixture;

    use crate::types::{NetworkRequest, ReceivedBatch, RequestPayload, TxBatch};

    use super::BatchAcknowledger;


    type BatchAcknowledgerFixture = (
        tokio::sync::mpsc::Sender<ReceivedBatch>,
        tokio::sync::mpsc::Receiver<NetworkRequest>,
        tokio::task::JoinHandle<()>,
        tokio_util::sync::CancellationToken,
    );

    fn launch_batch_maker() -> BatchAcknowledgerFixture {
        let (batches_tx, batches_rx) = tokio::sync::mpsc::channel(100);
        let (requests_tx, requests_rx) = tokio::sync::mpsc::channel(100);
        let token = tokio_util::sync::CancellationToken::new();
        let handle = BatchAcknowledger::spawn(batches_rx, requests_tx, token.clone());
        (batches_tx, requests_rx, handle, token)
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn test_single_batch_acknowledgement() {
        let (batches_tx, mut requests_rx, _, _) = launch_batch_maker();

        let batch = ReceivedBatch {
            sender: PeerId::random(),
            batch: TxBatch::default(),
        };

        let expected_request = NetworkRequest::SendTo(batch.sender, RequestPayload::Acknoledgment(
            blake3::hash(&bincode::serialize(&batch.batch).expect("failed to serialize batch")).as_bytes().to_vec(),
        ));
        batches_tx.send(batch).await.expect("failed to send batch");
        let res = requests_rx.recv().await.expect("failed to receive request");
        assert_eq!(res, expected_request);
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn test_cancelled() {
        let (_, _, handle, token) = launch_batch_maker();
        token.cancel();
        handle.await.expect("failed to await handle");
    }

}