use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use libp2p::identity::Keypair;
use tokio::{
    sync::{broadcast, mpsc},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

use crate::{
    db::{self, Db},
    types::{
        signing::sign_with_keypair, BlockHeader, Digest, NetworkRequest, RequestPayload, Round,
        SignedBlockHeader,
    },
};

const MAX_DIGESTS_IN_HEADER: u64 = 10;
const HEADER_PRODUCTION_INTERVAL_IN_SECS: u64 = 5;

pub(crate) struct HeaderBuilder {
    digest_rx: broadcast::Receiver<Digest>,
    network_tx: mpsc::Sender<NetworkRequest>,
    local_keypair: Keypair,
    db: Arc<Db>,
    round_rx: tokio::sync::watch::Receiver<Round>,
}

impl HeaderBuilder {
    #[must_use]
    pub fn spawn(
        local_keypair: Keypair,
        digest_rx: broadcast::Receiver<Digest>,
        network_tx: mpsc::Sender<NetworkRequest>,
        cancellation_token: CancellationToken,
        db: Arc<Db>,
        round_rx: tokio::sync::watch::Receiver<Round>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let res = cancellation_token
                .run_until_cancelled(
                    Self {
                        digest_rx,
                        network_tx,
                        local_keypair,
                        db,
                        round_rx,
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
                        let current_round = *self.round_rx.borrow();
                        let block_header = self.build_current_header(&mut batch, current_round);
                        let signed_header = sign_with_keypair(&self.local_keypair, block_header)?;
                        tracing::info!("broadcasting header for round {current_round}");
                        self.broadcast_header(signed_header).await?;

                        batch.clear();
                    }
                }
                Ok(digest) = self.digest_rx.recv() => {
                    batch.push(digest);
                    self.db.insert(db::Column::Digests, &hex::encode(digest), true)?;
                    if batch.len() >= MAX_DIGESTS_IN_HEADER as usize {
                        let current_round = *self.round_rx.borrow();
                        let block_header = self.build_current_header(&mut batch, current_round);
                        let signed_header = sign_with_keypair(&self.local_keypair, block_header)?;
                        tracing::info!("broadcasting header for round {current_round}");
                        self.broadcast_header(signed_header).await?;
                        batch.clear();
                    }
                }
            }
        }
    }

    /// Builds the block header given a batch of digests
    /// NOTE: `timestamp_ms` field is set to the current timestamp
    pub fn build_current_header(
        &self,
        batch: &mut Vec<Digest>,
        current_round: Round,
    ) -> BlockHeader {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Failed to measure time")
            .as_millis();

        let peer_id = self.local_keypair.public().to_peer_id();

        BlockHeader {
            round: current_round,
            author: peer_id,
            timestamp_ms: now,
            digests: batch.clone(),
        }
    }

    /// Broadcasts the block header to the other primaries
    pub async fn broadcast_header(&self, header: SignedBlockHeader) -> anyhow::Result<()> {
        tracing::info!(
            "🤖 [Batch] Broadcasting header, signature: {}",
            hex::encode(header.signature.clone())
        );
        self.network_tx
            .send(NetworkRequest::Broadcast(RequestPayload::Header(header)))
            .await?;
        Ok(())
    }
}
