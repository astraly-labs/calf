use std::time::{SystemTime, UNIX_EPOCH};

use tokio::{
    sync::{broadcast, mpsc},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

use crate::types::{BlockHeader, Digest, NetworkRequest, RequestPayload};

const MAX_DIGESTS_IN_HEADER: usize = 10;

pub(crate) struct HeaderBuilder {
    digest_rx: broadcast::Receiver<Digest>,
    network_tx: mpsc::Sender<NetworkRequest>,
}

impl HeaderBuilder {
    #[must_use]
    pub fn spawn(
        digest_rx: broadcast::Receiver<Digest>,
        network_tx: mpsc::Sender<NetworkRequest>,
        cancellation_token: CancellationToken,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let res = cancellation_token
                .run_until_cancelled(
                    Self {
                        digest_rx,
                        network_tx,
                    }
                    .run(),
                )
                .await;

            match res {
                Some(res) => {
                    match res {
                        Ok(_) => {
                            tracing::info!("HeaderBuilder finnished");
                        }
                        Err(e) => {
                            tracing::error!("HeaderBuilder finished with Error: {:?}", e);
                        }
                    };
                    cancellation_token.cancel();
                }
                None => {
                    tracing::info!("HeaderBuilder cancelled");
                }
            };
        })
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        let mut batch = vec![];
        while let Ok(digest) = self.digest_rx.recv().await {
            batch.push(digest);

            if batch.len() >= MAX_DIGESTS_IN_HEADER {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("Failed to measure time")
                    .as_millis();
                let header = BlockHeader {
                    parents_hashes: vec![],
                    timestamp_ms: now,
                    digests: batch.clone(),
                };

                tracing::info!("🤖 Broadcasting header {:?}", header.clone());
                self.network_tx
                    .send(NetworkRequest::Broadcast(RequestPayload::Header(header)))
                    .await?;
                batch.clear();
            }
        }
        Ok(())
    }
}
