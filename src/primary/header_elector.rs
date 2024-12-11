use std::{collections::HashSet, sync::Arc};

use libp2p::{identity::ed25519::Keypair, PeerId};
use proc_macros::Spawn;
use tokio::sync::{broadcast, mpsc, watch};
use tokio_util::sync::CancellationToken;

use crate::{
    db::{self, Db},
    settings::parser::Committee,
    types::{
        batch::BatchId,
        block_header::BlockHeader,
        certificate::{Certificate, CertificateId},
        network::{NetworkRequest, ReceivedObject, RequestPayload},
        traits::AsHex,
        vote::Vote,
        Round,
    },
};

use super::sync_tracker::IncompleteHeader;

#[derive(Spawn)]
pub(crate) struct HeaderElector {
    network_tx: mpsc::Sender<NetworkRequest>,
    headers_rx: broadcast::Receiver<ReceivedObject<BlockHeader>>,
    round_rx: watch::Receiver<(Round, HashSet<Certificate>)>,
    validator_keypair: Keypair,
    db: Arc<Db>,
    committee: Committee,
    incomplete_headers_tx: mpsc::Sender<ReceivedObject<IncompleteHeader>>,
}

impl HeaderElector {
    pub async fn run(mut self) -> anyhow::Result<()> {
        loop {
            let header = self.headers_rx.recv().await?;
            tracing::info!(
                "📡 received new block header from {}, round: {}",
                hex::encode(header.object.author),
                header.object.round
            );
            let (round, certificates) = self.round_rx.borrow().clone();
            let potential_parents_ids: HashSet<CertificateId> =
                certificates.iter().map(|elm| elm.id()).collect();
            // Check if all batch digests are available in the database, all certificates are valid, the header is from the current round and if the header author has not already produced a header for the current round
            if header.object.round != round {
                tracing::info!(
                    "🚫 header from a different round, expected: {}, received: {}. rejected",
                    round,
                    header.object.round
                );
                continue;
            }
            match process_header(header.object.clone(), header.sender, &self.db) {
                Ok(()) => {
                    tracing::info!("✅ header approved");
                }
                Err(incomplete_header) => {
                    tracing::info!("🚫 header incomplete, sending to the sync tracker");
                    self.incomplete_headers_tx
                        .send(ReceivedObject::new(incomplete_header, header.sender))
                        .await?;
                    continue;
                }
            }

            match header
                .object
                .verify_parents(potential_parents_ids, self.committee.quorum_threshold())
            {
                Ok(()) => {
                    tracing::info!("✅ header parents verified");
                    // Send back a vote to the header author
                    self.network_tx
                        .send(NetworkRequest::SendTo(
                            header.sender,
                            RequestPayload::Vote(Vote::from_header(
                                header.object.clone(),
                                &self.validator_keypair,
                            )?),
                        ))
                        .await?;
                    tracing::info!("✨ header accepted, vote sent");
                    self.db.insert(
                        db::Column::Headers,
                        &header.object.id().as_hex_string(),
                        header.object,
                    )?;
                }
                Err(e) => {
                    tracing::info!("🚫 header parents verification failed: {}", e);
                    continue;
                }
            }
        }
    }
}

fn process_header(
    header: BlockHeader,
    sender: PeerId,
    db: &Arc<Db>,
) -> Result<(), IncompleteHeader> {
    let missing_batches: HashSet<BatchId> = header
        .digests
        .iter()
        .filter(
            |digest| match db.get::<BatchId>(db::Column::Digests, &digest.0.as_hex_string()) {
                Ok(Some(_)) => true,
                _ => false,
            },
        )
        .cloned()
        .collect();

    let missing_certificates: HashSet<CertificateId> = header
        .certificates_ids
        .iter()
        .filter(|certificate| {
            match db.get::<CertificateId>(db::Column::Certificates, &certificate.0.as_hex_string())
            {
                Ok(Some(_)) => true,
                _ => false,
            }
        })
        .cloned()
        .collect();

    if missing_certificates.is_empty() && missing_batches.is_empty() {
        Ok(())
    } else {
        Err(IncompleteHeader {
            missing_certificates,
            missing_batches,
            header,
            sender,
        })
    }
}
