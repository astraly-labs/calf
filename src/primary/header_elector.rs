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
        sync::IncompleteHeader,
        traits::AsHex,
        vote::Vote,
        Round,
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
    _incomplete_headers_tx: mpsc::Sender<ReceivedObject<IncompleteHeader>>,
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
                Ok(_) => {
                    tracing::info!("✅ header approved");
                }
                Err(_) => {
                    tracing::info!("🚫 header incomplete: rejected");
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
        .filter(|digest| {
            !matches!(
                db.get::<BatchId>(db::Column::Digests, &digest.0.as_hex_string()),
                Ok(Some(_))
            )
        })
        .cloned()
        .collect();

    let missing_certificates: HashSet<CertificateId> = header
        .certificates_ids
        .iter()
        .filter(|certificate| {
            !matches!(
                db.get::<CertificateId>(db::Column::Certificates, &certificate.0.as_hex_string()),
                Ok(Some(_))
            )
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

#[cfg(test)]
mod test {
    use std::{collections::HashSet, sync::Arc, time::Duration};

    use libp2p::{
        identity::ed25519::{self, Keypair},
        PeerId,
    };
    use tempfile::tempdir;
    use tokio::{
        sync::{broadcast, mpsc, watch},
        time::timeout,
    };
    use tokio_util::sync::CancellationToken;

    use crate::{
        db::{Column, Db},
        primary::test_utils::fixtures::{random_digests, CHANNEL_CAPACITY},
        settings::parser::Committee,
        types::{
            block_header::BlockHeader,
            certificate::{Certificate, CertificateId},
            network::{NetworkRequest, ReceivedObject, RequestPayload},
            sync::IncompleteHeader,
            traits::{AsHex, Hash},
            Round,
        },
    };

    use super::HeaderElector;

    type HeaderElectorFixutre = (
        //To send headers to the elector
        broadcast::Sender<ReceivedObject<BlockHeader>>,
        //To send the current round and certificates to the elector
        watch::Sender<(Round, HashSet<Certificate>)>,
        //To receive incomplete headers from the elector
        mpsc::Receiver<ReceivedObject<IncompleteHeader>>,
        //To receive requests from the elector
        mpsc::Receiver<NetworkRequest>,
        Arc<Db>,
        CancellationToken,
        tempfile::TempDir,
    );

    async fn launch_header_elector() -> HeaderElectorFixutre {
        let (headers_tx, headers_rx) = broadcast::channel(CHANNEL_CAPACITY);
        let (round_tx, round_rx) = watch::channel((0, HashSet::new()));
        let (incomplete_headers_tx, incomplete_headers_rx) = mpsc::channel(CHANNEL_CAPACITY);
        let (network_tx, network_rx) = mpsc::channel(CHANNEL_CAPACITY);

        // Create a temporary directory for the test database
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.db");

        let db = Arc::new(Db::new(db_path).unwrap());

        let validator_keypair = ed25519::Keypair::generate();
        let token = CancellationToken::new();
        let db_clone = db.clone();
        let token_clone = token.clone();

        let committee = Committee::new_test();

        tokio::spawn(async move {
            HeaderElector::spawn(
                token_clone,
                network_tx,
                headers_rx,
                round_rx,
                validator_keypair,
                db_clone,
                committee,
                incomplete_headers_tx,
            )
            .await
            .unwrap()
        });

        (
            headers_tx,
            round_tx,
            incomplete_headers_rx,
            network_rx,
            db,
            token,
            temp_dir,
        )
    }

    fn set_header_storage_in_db(header: &BlockHeader, db: &Db) {
        for digest in &header.digests {
            db.insert(Column::Digests, &digest.0.as_hex_string(), digest)
                .unwrap();
        }
    }

    fn set_certificates_in_db(certificates: &[Certificate], db: &Db) {
        for cert in certificates {
            db.insert(Column::Certificates, &cert.id_as_hex(), cert)
                .unwrap();
        }
    }

    fn random_header(certificates_ids: &[CertificateId], round: Round) -> BlockHeader {
        let digests = random_digests(10);
        let author_pubkey = Keypair::generate().public().to_bytes();
        BlockHeader::new(author_pubkey, digests, certificates_ids.into(), round)
    }

    fn publish_round_state(
        tx: &watch::Sender<(Round, HashSet<Certificate>)>,
        round: Round,
        references: &[Certificate],
    ) {
        let certificates = references.iter().cloned().collect();
        tx.send((round, certificates)).unwrap();
    }

    async fn send_header_check_vote(
        header: BlockHeader,
        headers_tx: broadcast::Sender<ReceivedObject<BlockHeader>>,
        mut network_rx: mpsc::Receiver<NetworkRequest>,
    ) {
        let header_hash = header.digest();
        let peer_id = PeerId::random();
        headers_tx
            .send(ReceivedObject::new(header, peer_id))
            .unwrap();
        match timeout(Duration::from_millis(10), network_rx.recv()).await {
            Ok(Some(NetworkRequest::SendTo(sender, RequestPayload::Vote(vote)))) => {
                assert_eq!(sender, peer_id);
                let vote_status = vote.verify(&header_hash);
                assert!(vote_status.is_ok());
            }
            _ => assert!(false),
        }
    }

    #[tokio::test]
    async fn test_first_round_valid_header_digests_stored() {
        let (headers_tx, round_state_tx, _incomplete_headers_rx, network_rx, db, _, _temp_dir) =
            launch_header_elector().await;
        let genesis = Certificate::genesis([0; 32]);
        let header = random_header(&[genesis.id()], 1);
        set_header_storage_in_db(&header, &db);
        set_certificates_in_db(&[genesis.clone()], &db);
        publish_round_state(&round_state_tx, 1, &[genesis]);
        send_header_check_vote(header, headers_tx, network_rx).await;
    }
}
