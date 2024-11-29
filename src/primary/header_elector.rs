use std::{collections::HashSet, sync::Arc};

use libp2p::identity::ed25519::Keypair;
use tokio::{
    sync::{broadcast, mpsc, watch},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

use crate::{
    db::{self, Db},
    types::{BlockHeader, Certificate, Digest, NetworkRequest, ReceivedObject, Round, Vote},
};

pub(crate) struct HeaderElector {
    network_tx: mpsc::Sender<NetworkRequest>,
    headers_rx: broadcast::Receiver<ReceivedObject<BlockHeader>>,
    round_rx: watch::Receiver<(Round, Vec<Certificate>)>,
    validator_keypair: Keypair,
    db: Arc<Db>,
}

impl HeaderElector {
    #[must_use]
    pub fn spawn(
        cancellation_token: CancellationToken,
        validator_keypair: Keypair,
        db: Arc<Db>,
        headers_rx: broadcast::Receiver<ReceivedObject<BlockHeader>>,
        network_tx: mpsc::Sender<NetworkRequest>,
        round_rx: watch::Receiver<(Round, Vec<Certificate>)>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let res = cancellation_token
                .run_until_cancelled(
                    Self {
                        network_tx,
                        validator_keypair,
                        db,
                        headers_rx,
                        round_rx,
                    }
                    .run(),
                )
                .await;

            match res {
                Some(res) => {
                    match res {
                        Ok(_) => {
                            tracing::info!("HeaderElector finnished");
                        }
                        Err(e) => {
                            tracing::error!("HeaderElector finished with Error: {:?}", e);
                        }
                    };
                    cancellation_token.cancel();
                }
                None => {
                    tracing::info!("HeaderElector cancelled");
                }
            };
        })
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        let mut previous_round = 0;
        // Authorities that have produced header for the current round
        let mut round_authors = HashSet::new();
        loop {
            let header = self.headers_rx.recv().await?;
            tracing::info!(
                "📡 received new block header from {}",
                hex::encode(header.object.author)
            );
            // Check if all batch digests are available in the database, all certificates are valid, the header is from the current round and if the header author has not already produced a header for the current round
            let (round, certificates) = self.round_rx.borrow().clone();
            if previous_round != round {
                round_authors.clear();
                previous_round = round;
            }
            if round_authors.insert(header.object.author)
                && header.object.round == round
                && header.object.digests.iter().all(|digest| {
                    self.db
                        .get(db::Column::Digests, &hex::encode(digest))
                        .is_ok_and(|d: Option<Digest>| d.is_some())
                })
                && header
                    .object
                    .certificates
                    .iter()
                    .all(|cert| verify_certificate(cert, &certificates).is_ok_and(|v: bool| v))
            {
                // Send back a vote to the header author
                self.network_tx
                    .send(NetworkRequest::SendTo(
                        header.sender,
                        crate::types::RequestPayload::Vote(Vote::from_header(
                            header.object,
                            &self.validator_keypair,
                        )?),
                    ))
                    .await?;
                tracing::info!("✨ header accepted, vote sent");
            }
        }
    }
}

// TODO: Implement this function
fn verify_certificate(
    certificate: &Certificate,
    previous_round_certificates: &Vec<Certificate>,
) -> anyhow::Result<bool> {
    Ok(true)
}
