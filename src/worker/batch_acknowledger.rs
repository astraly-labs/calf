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
