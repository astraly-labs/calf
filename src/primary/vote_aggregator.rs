use std::sync::Arc;

use tokio::{
    sync::{broadcast, mpsc},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

use crate::{
    db::Db,
    settings::parser::Committee,
    types::{NetworkRequest, SignedBlockHeader},
};

const QUORUM_TIMEOUT: u128 = 1000;

struct WaitingHeader {
    ack_number: u32,
    header: SignedBlockHeader,
    timestamp: tokio::time::Instant,
}

impl WaitingHeader {
    fn new(header: SignedBlockHeader) -> anyhow::Result<Self> {
        Ok(Self {
            ack_number: 0,
            header,
            timestamp: tokio::time::Instant::now(),
        })
    }
}

pub(crate) struct VoteAggregator {
    votes_rx: broadcast::Receiver<SignedBlockHeader>,
    network_tx: mpsc::Sender<NetworkRequest>,
    header_rx: broadcast::Receiver<SignedBlockHeader>,
    commitee: Committee,
    db: Arc<Db>,
}

impl VoteAggregator {
    #[must_use]
    pub fn spawn(
        commitee: Committee,
        votes_rx: broadcast::Receiver<SignedBlockHeader>,
        network_tx: mpsc::Sender<NetworkRequest>,
        header_rx: broadcast::Receiver<SignedBlockHeader>,
        cancellation_token: CancellationToken,
        db: Arc<Db>,
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
        let mut votes = vec![];
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
                    if votes.iter().any(|elm: &WaitingHeader| {
                       elm.header.value == waiting_header.header.value
                    }) {
                        tracing::warn!("Received duplicate header");
                    }
                    else {
                        votes.push(waiting_header);
                        tracing::info!("Received new header");
                    }
                    let now = tokio::time::Instant::now();
                    //perfectible ? rayon ? une "liste de timers" ?
                    for i in 0..votes.len() {
                        if now.duration_since(votes[i].timestamp).as_millis() > QUORUM_TIMEOUT {
                            tracing::warn!("Header timed out: {:?}", votes[i].header);
                            votes.remove(i);
                        }
                    }
            },
                Ok(vote) = self.votes_rx.recv() => {
                    match votes.iter().position(|b| b.header.value == vote.value) {
                        Some(header_index) => {
                            let waiting_header = &mut votes[header_index];
                            waiting_header.ack_number += 1;
                            if waiting_header.ack_number >= self.commitee.quorum_threshold().try_into()? {
                                tracing::info!("Header is now confirmed: {:?}", waiting_header.header);
                                votes.remove(header_index);
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
