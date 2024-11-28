use crate::{
    db::Db,
    settings::parser::Committee,
    types::{Certificate, Dag, NetworkRequest, Round, SignedBlockHeader, Vote},
};
use libp2p::{identity::Keypair, PeerId};
use std::sync::{Arc, Mutex};
use tokio::{
    sync::{broadcast, mpsc, watch},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

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
    round_tx: watch::Sender<Round>,
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
        round_tx: watch::Sender<Round>,
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
                        round_tx,
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
                    // let now = tokio::time::Instant::now();
                    // //perfectible ? rayon ? une "liste de timers" ? //TODO: reparer: out of bounds
                    // for i in 0..waiting_headers.len() {
                    //     if now.duration_since(waiting_headers[i].timestamp).as_millis() > QUORUM_TIMEOUT {
                    //         tracing::warn!("Header timed out: {:?}", waiting_headers[i].header);
                    //         waiting_headers.remove(i);
                    //     }
                    // }
            },
            Ok(vote) = self.votes_rx.recv() => {
                    // Check if we have a corresponding header waiting for votes
                    if let Some(header_index) = waiting_headers.iter().position(|b| b.header.value == *vote.header()) {
                        let waiting_header = &mut waiting_headers[header_index];
                        waiting_header.votes.push(*vote.peer_id());
                        waiting_header.ack_number += 1;
                        if waiting_header.ack_number >= self.commitee.quorum_threshold().try_into()? {
                            tracing::info!("✅ Quorum Reached for round : {}", waiting_header.header.value.round);
                            // Create certificate
                            // TODO: must depends on the round r-1 certificates ?
                            let certificate = Certificate::new(waiting_header.votes.clone(), waiting_header.header.value.clone());
                            // Add it to my local DAG
                            self.dag.lock().unwrap().insert(waiting_header.header.value.round, (self.local_keypair.public().to_peer_id(), certificate));
                            tracing::info!("💚 DAG Updated: {:?}", self.dag.lock().unwrap());
                            // TODO: Broadcast the certificate
                            tracing::info!("round updated --> {}", waiting_header.header.value.round + 1);
                            self.round_tx.send(waiting_header.header.value.round + 1)?;
                            waiting_headers.remove(header_index);
                        }
                    };
                }
            }
        }
    }
}
