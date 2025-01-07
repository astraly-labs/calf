use std::{collections::HashSet, sync::Arc, time::Duration};

use libp2p::identity::ed25519::Keypair;
use proc_macros::Spawn;
use tokio::{
    sync::{broadcast, mpsc, watch, Mutex},
    time::timeout,
};
use tokio_util::sync::CancellationToken;

use crate::{
    db::Db,
    settings::parser::Committee,
    types::{
        batch::BatchId,
        block_header::BlockHeader,
        certificate::Certificate,
        network::{NetworkRequest, ReceivedObject, RequestPayload},
        sync::SyncStatus,
        traits::Hash,
        vote::Vote,
        Round,
    },
    utils::CircularBuffer,
};

const QUORUM_TIMEOUT: u64 = 1000;

#[derive(Spawn)]
pub(crate) struct HeaderBuilder {
    network_tx: mpsc::Sender<NetworkRequest>,
    certificate_tx: mpsc::Sender<Certificate>,
    keypair: Keypair,
    _db: Arc<Db>,
    header_trigger_rx: watch::Receiver<(Round, HashSet<Certificate>)>,
    votes_rx: broadcast::Receiver<ReceivedObject<Vote>>,
    digests_buffer: Arc<Mutex<CircularBuffer<BatchId>>>,
    committee: Committee,
    sync_status_rx: watch::Receiver<SyncStatus>,
}

impl HeaderBuilder {
    pub async fn run(mut self) -> anyhow::Result<()> {
        let mut cancellation_token = CancellationToken::new();
        loop {
            self.header_trigger_rx.changed().await?;

            if *self.sync_status_rx.borrow() == SyncStatus::Incomplete {
                tracing::info!("🔨 Syncing, unable t build a header, waiting for sync to finish");
                loop {
                    if *self.sync_status_rx.borrow() == SyncStatus::Complete {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(1000)).await;
                }
            }

            cancellation_token.cancel();
            let (round, certificates) = self.header_trigger_rx.borrow().clone();
            tracing::info!("🔨 Building Header for round {}", round);
            tokio::time::sleep(Duration::from_secs(1)).await;
            let digests = self.digests_buffer.lock().await.drain();
            let header = BlockHeader::new(
                self.keypair.public().to_bytes(),
                digests,
                certificates.into_iter().map(|elm| elm.id()).collect(),
                round,
            );
            cancellation_token = CancellationToken::new();

            // wait for quorum, build certificate, broadcast certificate. If the quorum is not reached before th enext round, the process will be cancelled
            {
                let network_tx = self.network_tx.clone();
                let certificate_tx = self.certificate_tx.clone();
                let mut votes_rx = self.votes_rx.resubscribe();
                let quorum_threshold = self.committee.quorum_threshold();
                let keypair = self.keypair.clone();
                let token = cancellation_token.clone();

                tokio::spawn(async move {
                    let _ = token
                        .run_until_cancelled(tokio::spawn(async move {
                            loop {
                                let _ = broadcast_header(header.clone(), &network_tx).await;
                                let votes = timeout(
                                    Duration::from_millis(QUORUM_TIMEOUT),
                                    wait_for_quorum(
                                        &header,
                                        quorum_threshold as usize,
                                        &mut votes_rx,
                                        &keypair,
                                    ),
                                )
                                .await;

                                match votes {
                                    Ok(Ok(votes)) => {
                                        let certificate = Certificate::derived(
                                            round,
                                            keypair.public().to_bytes(),
                                            votes,
                                            &header,
                                        );
                                        if let Ok(certificate) = certificate {
                                            let _ = broadcast_certificate(
                                                certificate,
                                                &network_tx,
                                                &certificate_tx,
                                            )
                                            .await;
                                        } else {
                                            tracing::warn!("Error building certificate");
                                        }
                                        break;
                                    }
                                    Ok(Err(e)) => {
                                        tracing::warn!("Error waiting for quorum: {:?}", e);
                                        break;
                                    }
                                    Err(_) => {
                                        tracing::warn!("Quorum not reached in time");
                                    }
                                }
                            }
                        }))
                        .await;
                });
            }
        }
    }
}

pub async fn wait_for_quorum(
    waiting_header: &BlockHeader,
    threshold: usize,
    votes_rx: &mut broadcast::Receiver<ReceivedObject<Vote>>,
    keypair: &Keypair,
) -> anyhow::Result<Vec<Vote>> {
    tracing::info!("⏳ Waiting quorum for header... threshold: {}", threshold);
    let header_hash = waiting_header.digest();
    let mut votes = vec![];
    let my_vote = Vote::from_header(waiting_header.clone(), keypair)?;
    votes.push(my_vote);
    loop {
        let vote = votes_rx.recv().await?;
        tracing::info!(
            "📡 received new vote from {}",
            hex::encode(vote.object.authority)
        );
        // vote: signed hash of the header
        if vote.object.verify(&header_hash)? {
            votes.push(vote.object);
            tracing::info!("👍 vote accepted");
        }
        // we vote for ourself
        if votes.len() >= threshold {
            break;
        }
    }
    tracing::info!("✅ Quorum reached for header");
    Ok(votes)
}

async fn broadcast_certificate(
    certificate: Certificate,
    network_tx: &mpsc::Sender<NetworkRequest>,
    certificate_tx: &mpsc::Sender<Certificate>,
) -> anyhow::Result<()> {
    certificate_tx.send(certificate.clone()).await?;
    network_tx
        .send(NetworkRequest::BroadcastCounterparts(
            RequestPayload::Certificate(certificate),
        ))
        .await?;

    tracing::info!("🤖 Broadcasting Certificate...");
    Ok(())
}

async fn broadcast_header(
    header: BlockHeader,
    network_tx: &mpsc::Sender<NetworkRequest>,
) -> anyhow::Result<()> {
    tracing::info!(
        "🤖 Broadcasting Header {} for round {}",
        hex::encode(header.digest()),
        header.round
    );
    network_tx
        .send(NetworkRequest::BroadcastCounterparts(
            RequestPayload::Header(header),
        ))
        .await?;
    Ok(())
}
