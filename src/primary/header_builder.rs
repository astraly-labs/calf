use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use libp2p::{identity::ed25519::Keypair, tls::certificate};
use tokio::{
    sync::{broadcast, mpsc, watch, Mutex},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

use crate::{
    db::{self, Db},
    settings::parser::Committee,
    types::{
        signing::sign_with_keypair, BlockHeader, Certificate, CircularBuffer, Digest, Hash,
        NetworkRequest, RequestPayload, Round, SignedBlockHeader, Vote,
    },
};

const MAX_DIGESTS_IN_HEADER: u64 = 10;
const HEADER_PRODUCTION_INTERVAL_IN_SECS: u64 = 5;

pub(crate) struct HeaderBuilder {
    network_tx: mpsc::Sender<NetworkRequest>,
    certificate_tx: mpsc::Sender<Certificate>,
    keypair: Keypair,
    db: Arc<Db>,
    header_trigger_rx: watch::Receiver<(Round, Vec<Certificate>)>,
    votes_rx: broadcast::Receiver<Vote>,
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
        db: Arc<Db>,
        header_trigger_rx: watch::Receiver<(Round, Vec<Certificate>)>,
        votes_rx: broadcast::Receiver<Vote>,
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
                        db,
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
    votes_rx: &mut broadcast::Receiver<Vote>,
) -> anyhow::Result<Vec<Vote>> {
    tracing::info!("⏳ Waiting for quorum for header...");
    let header_hash = waiting_header.digest()?;
    let mut votes = vec![];
    loop {
        let vote = votes_rx.recv().await?;
        // vote: signed hash of the header
        if vote.verify(&header_hash)? {
            votes.push(vote);
        }
        if votes.len() >= threshold {
            break;
        }
    }
    tracing::info!("✅ Quorum reached for header");
    Ok(votes)
}
