use std::sync::Arc;

use libp2p::identity::ed25519::Keypair;
use proc_macros::Spawn;
use tokio::sync::{broadcast, mpsc, watch, Mutex};
use tokio_util::sync::CancellationToken;

use crate::{
    db::Db,
    settings::parser::Committee,
    types::{
        block_header::BlockHeader, certificate::Certificate, Digest, Hash, NetworkRequest,
        ReceivedObject, RequestPayload, Round, Vote,
    },
    utils::CircularBuffer,
};

#[derive(Spawn)]
pub(crate) struct HeaderBuilder {
    network_tx: mpsc::Sender<NetworkRequest>,
    certificate_tx: mpsc::Sender<Certificate>,
    keypair: Keypair,
    _db: Arc<Db>,
    header_trigger_rx: watch::Receiver<(Round, Vec<Certificate>)>,
    votes_rx: broadcast::Receiver<ReceivedObject<Vote>>,
    digests_buffer: Arc<Mutex<CircularBuffer<Digest>>>,
    committee: Committee,
}

impl HeaderBuilder {
    pub async fn run(mut self) -> anyhow::Result<()> {
        let mut cancellation_token = CancellationToken::new();
        loop {
            let _trigger = self.header_trigger_rx.changed().await?;
            cancellation_token.cancel();
            let (round, certificates) = self.header_trigger_rx.borrow().clone();
            let digests = self.digests_buffer.lock().await.drain();
            tracing::info!("🔨 Building Header for round {}", round);
            let header = BlockHeader::new(
                self.keypair.public().to_bytes(),
                digests,
                certificates,
                round,
            );
            self.network_tx
                .send(NetworkRequest::Broadcast(RequestPayload::Header(
                    header.clone(),
                )))
                .await?;
            tracing::info!("🤖 Broadcasting Header {}", hex::encode(header.digest()?));
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
                            let votes =
                                wait_for_quorum(&header, quorum_threshold as usize, &mut votes_rx)
                                    .await
                                    .unwrap();
                            let certificate =
                                Certificate::new(round, keypair.public().to_bytes(), votes, header);
                            certificate_tx.send(certificate.clone()).await.unwrap();
                            tracing::info!("🤖 Broadcasting Certificate...");
                            network_tx
                                .send(NetworkRequest::Broadcast(RequestPayload::Certificate(
                                    certificate,
                                )))
                                .await
                                .unwrap();
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
) -> anyhow::Result<Vec<Vote>> {
    tracing::info!("⏳ Waiting quorum for header...");
    let header_hash = waiting_header.digest()?;
    let mut votes = vec![];
    loop {
        let vote = votes_rx.recv().await?;
        // vote: signed hash of the header
        if vote.object.verify(&header_hash)? {
            votes.push(vote.object);
        }
        if votes.len() >= threshold {
            break;
        }
    }
    tracing::info!("✅ Quorum reached for header");
    Ok(votes)
}
