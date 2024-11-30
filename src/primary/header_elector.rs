use std::{collections::HashSet, sync::Arc};

use libp2p::identity::ed25519::Keypair;
use proc_macros::Spawn;
use tokio::sync::{broadcast, mpsc, watch};
use tokio_util::sync::CancellationToken;

use crate::{
    db::{self, Db},
    settings::parser::Committee,
    types::{
        block_header::BlockHeader, certificate::Certificate, Digest, NetworkRequest,
        ReceivedObject, Round, Vote,
    },
};

#[derive(Spawn)]
pub(crate) struct HeaderElector {
    network_tx: mpsc::Sender<NetworkRequest>,
    headers_rx: broadcast::Receiver<ReceivedObject<BlockHeader>>,
    round_rx: watch::Receiver<(Round, Vec<Certificate>)>,
    validator_keypair: Keypair,
    db: Arc<Db>,
    committee: Committee,
}

impl HeaderElector {
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
            let (round, certificates) = self.round_rx.borrow().clone();
            if previous_round != round {
                round_authors.clear();
                previous_round = round;
            }
            // Check if all batch digests are available in the database, all certificates are valid, the header is from the current round and if the header author has not already produced a header for the current round
            if round_authors.insert(header.object.author)
                && header.object.round == round
                && header.object.digests.iter().all(|digest| {
                    self.db
                        .get(db::Column::Digests, &hex::encode(digest))
                        .is_ok_and(|d: Option<Digest>| d.is_some())
                })
                && verify_certificates(
                    &header.object.certificates,
                    &certificates,
                    self.committee.quorum_threshold(),
                )
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

// check if all certifcates are available in the DAG (previous round) and if the quorum threshold is met
fn verify_certificates(
    certificates: &Vec<Certificate>,
    previous_round_certificates: &Vec<Certificate>,
    quorum_threshold: u32,
) -> bool {
    certificates
        .iter()
        .filter(|cert| previous_round_certificates.contains(cert))
        .collect::<HashSet<_>>()
        .len()
        >= quorum_threshold as usize
}
