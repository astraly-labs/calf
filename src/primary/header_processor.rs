use std::sync::Arc;

use anyhow::Context;
use libp2p::identity::Keypair;
use tokio::{
    sync::{broadcast, mpsc},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

use crate::{
    db::{Column, Db},
    settings::parser::Committee,
    types::{BlockHeader, NetworkRequest, RequestPayload, Round, SignedBlockHeader, Vote},
};

pub(crate) struct HeaderProcessor {
    header_rx: broadcast::Receiver<SignedBlockHeader>,
    network_tx: mpsc::Sender<NetworkRequest>,
    _commitee: Committee,
    db: Arc<Db>,
    local_keypair: Keypair,
    round_rx: tokio::sync::watch::Receiver<Round>,
}

impl HeaderProcessor {
    #[must_use]
    pub fn spawn(
        _commitee: Committee,
        header_rx: broadcast::Receiver<SignedBlockHeader>,
        network_tx: mpsc::Sender<NetworkRequest>,
        cancellation_token: CancellationToken,
        db: Arc<Db>,
        local_keypair: Keypair,
        round_rx: tokio::sync::watch::Receiver<Round>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let res = cancellation_token
                .run_until_cancelled(
                    Self {
                        _commitee,
                        header_rx,
                        network_tx,
                        db,
                        local_keypair,
                        round_rx,
                    }
                    .run(),
                )
                .await;

            match res {
                Some(res) => {
                    match res {
                        Ok(_) => {
                            tracing::info!("HeaderProcessor finnished");
                        }
                        Err(e) => {
                            tracing::error!("HeaderProcessor finished with Error: {:?}", e);
                        }
                    };
                    cancellation_token.cancel();
                }
                None => {
                    tracing::info!("HeaderProcessor cancelled");
                }
            };
        })
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        loop {
            tokio::select! {
                Ok(header) = self.header_rx.recv() => {
                    tracing::info!("🔨 Processing header...");
                    if !self.has_seen_header_for_round(header.value.round)? && self.has_digests_in_db(&header.value)? {
                        // 1. Send back a vote for it
                        self.broadcast_vote(&header).await.context("Failed to broadcast vote")?;
                        // 2. Store it in db
                        self.db.insert(Column::Headers, &header.value.round.to_string(), header).context("Failed to insert header in db")?;
                    }
                }
            }
        }
    }

    /// Checks in db if we have a header stored for a given round
    pub fn has_seen_header_for_round(&self, round: Round) -> anyhow::Result<bool> {
        let maybe_header = self
            .db
            .get::<BlockHeader>(Column::Headers, &round.to_string())?;
        Ok(maybe_header.is_some())
    }

    fn has_digests_in_db(&self, header: &BlockHeader) -> anyhow::Result<bool> {
        Ok(header.digests.iter().all(|digest| {
            self.db
                .get::<bool>(Column::Digests, &hex::encode(digest))
                .unwrap_or(Some(false))
                .unwrap_or(false)
        }))
    }

    /// Broadcasts a vote for the given block header to the other primaries
    pub async fn broadcast_vote(&self, header: &SignedBlockHeader) -> anyhow::Result<()> {
        tracing::info!(
            "🤖 [Batch] Broadcasting vote for header of signature {}",
            hex::encode(header.signature.clone())
        );
        self.network_tx
            .send(NetworkRequest::Broadcast(RequestPayload::Vote(Vote::new(
                self.local_keypair.public().to_peer_id(),
                header.value.clone(),
            ))))
            .await?;
        Ok(())
    }
}
