use std::sync::Arc;

use libp2p::identity::ed25519::Keypair;
use tokio::{
    sync::{broadcast, mpsc, watch, Mutex},
    task::JoinHandle,
};
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
    #[must_use]
    pub fn spawn(
        keypair: Keypair,
        network_tx: mpsc::Sender<NetworkRequest>,
        certificate_tx: mpsc::Sender<Certificate>,
        cancellation_token: CancellationToken,
        _db: Arc<Db>,
        header_trigger_rx: watch::Receiver<(Round, Vec<Certificate>)>,
        votes_rx: broadcast::Receiver<ReceivedObject<Vote>>,
        digests_buffer: Arc<Mutex<CircularBuffer<Digest>>>,
        committee: Committee,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let res = cancellation_token
                .run_until_cancelled(
                    Self {
                        network_tx,
                        certificate_tx,
                        keypair,
                        _db,
                        header_trigger_rx,
                        votes_rx,
                        digests_buffer,
                        committee,
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
