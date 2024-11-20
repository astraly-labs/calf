use std::sync::{Arc, Mutex};

use libp2p::{identity::Keypair, PeerId};
use tokio::{
    sync::{broadcast, mpsc},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

use crate::{
    db::Db,
    settings::parser::Committee,
    types::{Certificate, Dag, NetworkRequest, SignedBlockHeader, Vote},
};

const QUORUM_TIMEOUT: u128 = 1000;

struct WaitingHeader {
    ack_number: u32,
    votes: Vec<PeerId>,
    header: SignedBlockHeader,
    timestamp: tokio::time::Instant,
}

impl WaitingHeader {
    fn new(header: SignedBlockHeader) -> anyhow::Result<Self> {
        Ok(Self {
            ack_number: 0,
            votes: vec![],
            header,
            timestamp: tokio::time::Instant::now(),
        })
    }
}

pub(crate) struct VoteAggregator {
    votes_rx: broadcast::Receiver<Vote>,
    network_tx: mpsc::Sender<NetworkRequest>,
    header_rx: broadcast::Receiver<SignedBlockHeader>,
    commitee: Committee,
    db: Arc<Db>,
    dag: Arc<Mutex<Dag>>,
    local_keypair: Keypair,
}

impl VoteAggregator {
    #[must_use]
    pub fn spawn(
        commitee: Committee,
        votes_rx: broadcast::Receiver<Vote>,
        network_tx: mpsc::Sender<NetworkRequest>,
        header_rx: broadcast::Receiver<SignedBlockHeader>,
        cancellation_token: CancellationToken,
        db: Arc<Db>,
        dag: Arc<Mutex<Dag>>,
        local_keypair: Keypair,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let res = cancellation_token
                .run_until_cancelled(
                    Self {
                        commitee,
                        votes_rx,
                        network_tx,
                        header_rx,
                        db,
                        dag,
                        local_keypair,
                    }
                    .run(),
                )
                .await;

            match res {
                Some(res) => {
                    match res {
                        Ok(_) => {
                            tracing::info!("VoteAggregator finnished");
                        }
                        Err(e) => {
                            tracing::error!("VoteAggregator finished with Error: {:?}", e);
                        }
                    };
                    cancellation_token.cancel();
                }
                None => {
                    tracing::info!("VoteAggregator cancelled");
                }
            };
        })
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        let mut waiting_headers = vec![];
        loop {
            tokio::select! {
                Ok(header) = self.header_rx.recv() => {
                    let waiting_header = match WaitingHeader::new(header) {
                        Ok(waiting_header) => waiting_header,
                        Err(e) => {
                            tracing::error!("Failed to create waiting header: {:?}", e);
                            continue;
                        }
                    };
                    if waiting_headers.iter().any(|elm: &WaitingHeader| {
                       elm.header.value == waiting_header.header.value
                    }) {
                        tracing::warn!("Received duplicate header");
                    }
                    else {
                        waiting_headers.push(waiting_header);
                        tracing::info!("Received new header");
                    }
                    let now = tokio::time::Instant::now();
                    //perfectible ? rayon ? une "liste de timers" ?
                    for i in 0..waiting_headers.len() {
                        if now.duration_since(waiting_headers[i].timestamp).as_millis() > QUORUM_TIMEOUT {
                            tracing::warn!("Header timed out: {:?}", waiting_headers[i].header);
                            waiting_headers.remove(i);
                        }
                    }
            },
            Ok(vote) = self.votes_rx.recv() => {
                // Check if we have a corresponding header waiting for votes
                match waiting_headers.iter().position(|b| b.header.value == *vote.header()) {
                    Some(header_index) => {
                        let waiting_header = &mut waiting_headers[header_index];
                        waiting_header.votes.push(*vote.peer_id());
                        waiting_header.ack_number += 1;
                        if waiting_header.ack_number >= self.commitee.quorum_threshold().try_into()? {
                            tracing::info!("✅ Quorum Reached for round : {:?}", waiting_header.header.value.round);
                            let signed_authorities = waiting_header.votes.clone();
                            let header_value = waiting_header.header.value.clone();
                            let round_number = header_value.round;

                            // Create certificate
                            let certificate = Certificate::new(signed_authorities, header_value);

                            // Add it to my local DAG
                            self.dag.lock().unwrap().insert(round_number, (self.local_keypair.public().to_peer_id(),certificate));

                            tracing::info!("💚 DAG Updated");
                            waiting_headers.remove(header_index);
                        }
                    },
                    _ => {
                    }
                };
            }
            }
        }
    }
}
