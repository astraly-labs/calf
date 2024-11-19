use std::time::{SystemTime, UNIX_EPOCH};

use libp2p::identity::PublicKey;
use tokio::{
    sync::{broadcast, mpsc},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

use crate::types::{BlockHeader, Digest, NetworkRequest, RequestPayload};

const MAX_DIGESTS_IN_HEADER: u64 = 10;
const HEADER_PRODUCTION_INTERVAL_IN_SECS: u64 = 5;

pub(crate) struct HeaderBuilder {
    digest_rx: broadcast::Receiver<Digest>,
    network_tx: mpsc::Sender<NetworkRequest>,
    authority_key: PublicKey,
}

impl HeaderBuilder {
    #[must_use]
    pub fn spawn(
        authority_key: PublicKey,
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
                        authority_key,
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
        let mut timer = tokio::time::interval(tokio::time::Duration::from_secs(
            HEADER_PRODUCTION_INTERVAL_IN_SECS,
        ));

        loop {
            tokio::select! {
                _ = timer.tick() => {
                    if !batch.is_empty() {
                        let block_header = self.build_current_header(&mut batch);
                        self.broadcast_header(block_header).await?;
                    }
                }
                Ok(digest) = self.digest_rx.recv() => {
                    batch.push(digest);
                    if batch.len() >= MAX_DIGESTS_IN_HEADER as usize {
                        let now = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .expect("Failed to measure time")
                            .as_millis();
                        let header = BlockHeader {
                            author: self.authority_key.encode_protobuf(),
                            parents_hashes: vec![],
                            timestamp_ms: now,
                            digests: batch.clone(),
                        };

                        tracing::info!("🤖 [Batch] Broadcasting header {:?}", header.clone());
                        self.network_tx
                            .send(NetworkRequest::Broadcast(RequestPayload::Header(header)))
                            .await?;
                        batch.clear();
                    }
                }
            }
        }
    }

    pub fn build_current_header(&self, batch: &mut Vec<Digest>) -> BlockHeader {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Failed to measure time")
            .as_millis();
        let header = BlockHeader {
            author: self.authority_key.encode_protobuf(),
            parents_hashes: vec![],
            timestamp_ms: now,
            digests: batch.clone(),
        };

        tracing::info!("🤖 [Timer] Broadcasting header {:?}", header.clone());

        batch.clear();

        header
    }

    pub async fn broadcast_header(&self, header: BlockHeader) -> anyhow::Result<()> {
        self.network_tx
            .send(NetworkRequest::Broadcast(RequestPayload::Header(header)))
            .await?;
        Ok(())
    }
}
