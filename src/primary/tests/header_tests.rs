use std::{collections::HashSet, sync::Arc, time::Duration};

use libp2p::{identity::ed25519::Keypair, PeerId};
use tokio::{
    sync::{broadcast, mpsc, watch},
    time::sleep,
};
use tokio_util::sync::CancellationToken;

use crate::{
    db::Db,
    primary::header_builder::{wait_for_quorum, HeaderBuilder},
    settings::parser::Committee,
    types::{
        batch::BatchId,
        block_header::BlockHeader,
        certificate::Certificate,
        network::{NetworkRequest, ReceivedObject},
        sync::SyncStatus,
        traits::AsBytes,
        signing::Signable,
        vote::Vote,
        Round,
    },
    utils::CircularBuffer,
};

// test helper functions
impl BlockHeader {
    #[cfg(test)]
    pub fn new_test() -> Self {
        let peer_id = PeerId::random();
        let mut author = [0u8; 32];
        let peer_bytes = peer_id.to_bytes();
        let hash = blake3::hash(&peer_bytes);
        author.copy_from_slice(hash.as_bytes());
        
        Self {
            round: 1,
            author,
            timestamp_ms: chrono::Utc::now().timestamp_millis() as u128,
            certificates_ids: vec![],
            digests: vec![],
        }
    }
}

impl Vote {
    #[cfg(test)]
    pub fn new_test() -> Self {
        let keypair = Keypair::generate();
        let authority = keypair.public().to_bytes();
        let header = BlockHeader::new_test();
        let signature = header.sign(&keypair).unwrap();
        
        Self {
            authority,
            signature,
        }
    }

    #[cfg(test)]
    pub fn new_test_invalid() -> Self {
        let keypair = Keypair::generate();
        let authority = keypair.public().to_bytes();
        
        Self {
            authority,
            signature: vec![0; 32],
        }
    }
}

impl Db {
    #[cfg(test)]
    pub async fn new_in_memory() -> anyhow::Result<Self> {
        let temp_dir = tempfile::tempdir()?;
        Self::new(temp_dir.path().to_path_buf())
    }
}

impl BatchId {
    #[cfg(test)]
    pub fn test_digest(value: u8) -> Self {
        let mut digest = [0u8; 32];
        digest.fill(value);
        Self(digest)
    }
}

#[tokio::test]
async fn test_header_builder_initialization() {
    let (network_tx, _) = mpsc::channel(100);
    let (certificate_tx, _) = mpsc::channel(100);
    let keypair = Keypair::generate();
    let db = Arc::new(Db::new_in_memory().await.unwrap());
    let (header_trigger_tx, header_trigger_rx) = watch::channel((0, HashSet::new()));
    let (votes_tx, votes_rx) = broadcast::channel(100);
    let digests_buffer = Arc::new(tokio::sync::Mutex::new(CircularBuffer::new(100)));
    let committee = Committee::new_test();
    let (sync_status_tx, sync_status_rx) = watch::channel(SyncStatus::Complete);

    let handle = HeaderBuilder::spawn(
        CancellationToken::new(),
        network_tx,
        certificate_tx,
        keypair,
        db,
        header_trigger_rx,
        votes_rx,
        digests_buffer,
        committee,
        sync_status_rx,
    );

    handle.abort();
}

#[tokio::test]
async fn test_wait_for_quorum() {
    let keypair = Keypair::generate();
    let (votes_tx, mut votes_rx) = broadcast::channel(100);
    let header = BlockHeader::new_test();
    let threshold = 2;

    // Create votes before spawning the task
    let votes: Vec<_> = (0..threshold)
        .map(|_| Vote::from_header(header.clone(), &keypair).unwrap())
        .collect();

    // send votes
    tokio::spawn(async move {
        for vote in votes {
            let received = ReceivedObject {
                object: vote,
                sender: PeerId::random(),
            };
            votes_tx.send(received).unwrap();
            sleep(Duration::from_millis(100)).await;
        }
    });

    // Wait for quorum
    let result = wait_for_quorum(&header, threshold, &mut votes_rx, &keypair).await;
    assert!(result.is_ok());
    let votes = result.unwrap();
    assert_eq!(votes.len(), threshold);
}

#[tokio::test]
async fn test_header_builder_sync_status() {
    let (network_tx, _) = mpsc::channel(100);
    let (certificate_tx, _) = mpsc::channel(100);
    let keypair = Keypair::generate();
    let db = Arc::new(Db::new_in_memory().await.unwrap());
    let (header_trigger_tx, header_trigger_rx) = watch::channel((0, HashSet::new()));
    let (votes_tx, votes_rx) = broadcast::channel(100);
    let digests_buffer = Arc::new(tokio::sync::Mutex::new(CircularBuffer::new(100)));
    let committee = Committee::new_test();
    let (sync_status_tx, sync_status_rx) = watch::channel(SyncStatus::Incomplete);

    // Spawn header builder - it returns a JoinHandle
    let handle = HeaderBuilder::spawn(
        CancellationToken::new(),
        network_tx,
        certificate_tx,
        keypair,
        db,
        header_trigger_rx,
        votes_rx,
        digests_buffer,
        committee,
        sync_status_rx,
    );

    // Test that header builder waits for sync to complete
    sleep(Duration::from_millis(100)).await;
    sync_status_tx.send(SyncStatus::Complete).unwrap();
    
    handle.abort();
}

#[tokio::test]
async fn test_header_builder_with_empty_digests() {
    let (network_tx, _) = mpsc::channel(100);
    let (certificate_tx, _) = mpsc::channel(100);
    let keypair = Keypair::generate();
    let db = Arc::new(Db::new_in_memory().await.unwrap());
    let (header_trigger_tx, header_trigger_rx) = watch::channel((0, HashSet::new()));
    let (votes_tx, votes_rx) = broadcast::channel(100);
    let digests_buffer = Arc::new(tokio::sync::Mutex::new(CircularBuffer::new(100)));
    let committee = Committee::new_test();
    let (sync_status_tx, sync_status_rx) = watch::channel(SyncStatus::Complete);

    let handle = HeaderBuilder::spawn(
        CancellationToken::new(),
        network_tx,
        certificate_tx,
        keypair,
        db,
        header_trigger_rx,
        votes_rx,
        digests_buffer,
        committee,
        sync_status_rx,
    );

    // trigger header building with empty digests
    header_trigger_tx.send((1, HashSet::new())).unwrap();
    sleep(Duration::from_millis(100)).await;

    handle.abort();
}

#[tokio::test]
async fn test_header_builder_multiple_rounds() {
    let (network_tx, _) = mpsc::channel(100);
    let (certificate_tx, mut cert_rx) = mpsc::channel(100);
    let keypair = Keypair::generate();
    let db = Arc::new(Db::new_in_memory().await.unwrap());
    let (header_trigger_tx, header_trigger_rx) = watch::channel((0, HashSet::new()));
    let (votes_tx, votes_rx) = broadcast::channel(100);
    let digests_buffer = Arc::new(tokio::sync::Mutex::new(CircularBuffer::new(100)));
    let committee = Committee::new_test();
    let (sync_status_tx, sync_status_rx) = watch::channel(SyncStatus::Complete);

    let handle = HeaderBuilder::spawn(
        CancellationToken::new(),
        network_tx,
        certificate_tx,
        keypair,
        db,
        header_trigger_rx,
        votes_rx,
        digests_buffer,
        committee,
        sync_status_rx,
    );

    // trigger multiple rounds
    for round in 1..=3 {
        let mut certs = HashSet::new();
        certs.insert(Certificate::genesis([round as u8; 32]));
        header_trigger_tx.send((round, certs)).unwrap();
        sleep(Duration::from_millis(100)).await;
    }

    handle.abort();
}

#[tokio::test]
async fn test_header_builder_quorum_timeout() {
    let (network_tx, _) = mpsc::channel(100);
    let (certificate_tx, _) = mpsc::channel(100);
    let keypair = Keypair::generate();
    let db = Arc::new(Db::new_in_memory().await.unwrap());
    let (header_trigger_tx, header_trigger_rx) = watch::channel((0, HashSet::new()));
    let (votes_tx, votes_rx) = broadcast::channel(100);
    let digests_buffer = Arc::new(tokio::sync::Mutex::new(CircularBuffer::new(100)));
    let committee = Committee::new_test();
    let (sync_status_tx, sync_status_rx) = watch::channel(SyncStatus::Complete);

    let handle = HeaderBuilder::spawn(
        CancellationToken::new(),
        network_tx,
        certificate_tx,
        keypair,
        db,
        header_trigger_rx,
        votes_rx,
        digests_buffer,
        committee,
        sync_status_rx,
    );

    // trigger header building but don't send any votes
    let mut certs = HashSet::new();
    certs.insert(Certificate::genesis([0; 32]));
    header_trigger_tx.send((1, certs)).unwrap();

    sleep(Duration::from_millis(110)).await;
    handle.abort();
}

#[tokio::test]
async fn test_header_builder_invalid_votes() {
    let (network_tx, _) = mpsc::channel(100);
    let (certificate_tx, _) = mpsc::channel(100);
    let keypair = Keypair::generate();
    let db = Arc::new(Db::new_in_memory().await.unwrap());
    let (header_trigger_tx, header_trigger_rx) = watch::channel((0, HashSet::new()));
    let (votes_tx, votes_rx) = broadcast::channel(100);
    let digests_buffer = Arc::new(tokio::sync::Mutex::new(CircularBuffer::new(100)));
    let committee = Committee::new_test();
    let (sync_status_tx, sync_status_rx) = watch::channel(SyncStatus::Complete);

    let handle = HeaderBuilder::spawn(
        CancellationToken::new(),
        network_tx,
        certificate_tx,
        keypair,
        db,
        header_trigger_rx,
        votes_rx,
        digests_buffer,
        committee,
        sync_status_rx,
    );

    // send invalid votes
    let invalid_vote = Vote::new_test_invalid();
    votes_tx.send(ReceivedObject {
        object: invalid_vote,
        sender: PeerId::random(),
    }).unwrap();

    sleep(Duration::from_millis(10)).await;
    handle.abort();
}

#[tokio::test]
async fn test_header_builder_digest_buffer() {
    let (network_tx, _) = mpsc::channel(100);
    let (certificate_tx, _) = mpsc::channel(100);
    let keypair = Keypair::generate();
    let db = Arc::new(Db::new_in_memory().await.unwrap());
    let (header_trigger_tx, header_trigger_rx) = watch::channel((0, HashSet::new()));
    let (votes_tx, votes_rx) = broadcast::channel(100);
    let digests_buffer = Arc::new(tokio::sync::Mutex::new(CircularBuffer::new(2))); // Small buffer
    let committee = Committee::new_test();
    let (sync_status_tx, sync_status_rx) = watch::channel(SyncStatus::Complete);

    let handle = HeaderBuilder::spawn(
        CancellationToken::new(),
        network_tx,
        certificate_tx,
        keypair,
        db,
        header_trigger_rx,
        votes_rx,
        digests_buffer.clone(),
        committee,
        sync_status_rx,
    );

    // Add digests to buffer
    {
        let mut buffer = digests_buffer.lock().await;
        buffer.push(BatchId::test_digest(1));
        buffer.push(BatchId::test_digest(2));
        buffer.push(BatchId::test_digest(3));
    }

    // Verify buffer state
    {
        let mut buffer = digests_buffer.lock().await;
        let contents = buffer.drain();
        assert_eq!(contents.len(), 2);
        assert_eq!(contents[0], BatchId::test_digest(2));
        assert_eq!(contents[1], BatchId::test_digest(3));
    }

    handle.abort();
} 