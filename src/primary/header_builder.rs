use std::sync::Arc;

use libp2p::identity::ed25519::Keypair;
use proc_macros::Spawn;
use tokio::sync::{broadcast, mpsc, watch, Mutex};
use tokio_util::sync::CancellationToken;

use crate::{
    db::Db,
    settings::parser::Committee,
    types::{
        BlockHeader, Certificate, Digest, Hash, NetworkRequest, ReceivedObject, RequestPayload,
        Round, Vote,
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
        loop {
            let _trigger = self.header_trigger_rx.changed().await?;
            let (round, certificates) = self.header_trigger_rx.borrow().clone();
            let digests = self.digests_buffer.lock().await.drain();
            let header = BlockHeader::new(
                self.keypair.public().to_bytes(),
                digests,
                certificates,
                round,
            );
            tracing::info!("🤖 Broadcasting Header {}", hex::encode(header.digest()?));
            self.network_tx
                .send(NetworkRequest::Broadcast(RequestPayload::Header(
                    header.clone(),
                )))
                .await?;
            // timeout ?
            let votes = wait_for_quorum(
                &header,
                self.committee.quorum_threshold() as usize,
                &mut self.votes_rx,
            )
            .await?;
            let certificate = Certificate::new(votes, header, self.keypair.public().to_bytes());
            self.certificate_tx.send(certificate.clone()).await?;
            tracing::info!("🤖 Broadcasting Certificate...");
            self.network_tx
                .send(NetworkRequest::Broadcast(RequestPayload::Certificate(
                    certificate,
                )))
                .await?;
        }
    }
}

pub async fn wait_for_quorum(
    waiting_header: &BlockHeader,
    threshold: usize,
    votes_rx: &mut broadcast::Receiver<ReceivedObject<Vote>>,
) -> anyhow::Result<Vec<Vote>> {
    tracing::info!("⏳ Waiting for quorum for header...");
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
