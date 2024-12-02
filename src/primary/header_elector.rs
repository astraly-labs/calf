use std::{collections::HashSet, sync::Arc};

use libp2p::identity::ed25519::Keypair;
use proc_macros::Spawn;
use tokio::sync::{broadcast, mpsc, watch};
use tokio_util::sync::CancellationToken;

use crate::{
    db::{self, Db},
    settings::parser::Committee,
    types::{
        block_header::BlockHeader, certificate::Certificate, Digest, NetworkRequest, PublicKey,
        ReceivedObject, Round, Vote,
    },
};

#[derive(Spawn)]
pub(crate) struct HeaderElector {
    network_tx: mpsc::Sender<NetworkRequest>,
    headers_rx: broadcast::Receiver<ReceivedObject<BlockHeader>>,
    round_rx: watch::Receiver<(Round, HashSet<Certificate>)>,
    validator_keypair: Keypair,
    db: Arc<Db>,
    committee: Committee,
}

impl HeaderElector {
    pub async fn run(mut self) -> anyhow::Result<()> {
        let mut previous_round = 0;
        // Authorities that have produced header for the current round
        let mut round_authors = HashSet::<PublicKey>::new();
        loop {
            let header = self.headers_rx.recv().await?;
            tracing::info!(
                "📡 received new block header from {}, round: {}",
                hex::encode(header.object.author),
                header.object.round
            );
            let (round, certificates) = self.round_rx.borrow().clone();
            if previous_round != round {
                round_authors.clear();
                previous_round = round;
            }
            // Check if all batch digests are available in the database, all certificates are valid, the header is from the current round and if the header author has not already produced a header for the current round
            if header.object.round == round
                && header
                    .object
                    .verify_parents(certificates, self.committee.quorum_threshold())
                && header_data_in_storage(&header.object, &self.db)
            //&& round_authors.insert(header.object.author) // TODO: if the header produced by a validator is the same as the previous one, it will not be rejected
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
            } else {
                tracing::info!("❌ header rejected");
            }
        }
    }
}

fn header_data_in_storage(header: &BlockHeader, db: &Arc<Db>) -> bool {
    header.digests.iter().all(|digest| {
        db.get(db::Column::Digests, &hex::encode(digest))
            .is_ok_and(|d: Option<Digest>| d.is_some())
    }) || header.digests.is_empty()
}
