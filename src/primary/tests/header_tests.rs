use std::{collections::HashSet, sync::Arc, time::Duration};

use libp2p::identity::ed25519::Keypair;
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
        vote::Vote,
        Round,
    },
    utils::CircularBuffer,
};

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

    let _header_builder = HeaderBuilder {
        network_tx,
        certificate_tx,
        keypair,
        _db: db,
        header_trigger_rx,
        votes_rx,
        digests_buffer,
        committee,
        sync_status_rx,
    };
}

#[tokio::test]
async fn test_wait_for_quorum() {
    let keypair = Keypair::generate();
    let (votes_tx, mut votes_rx) = broadcast::channel(100);
    let header = BlockHeader::new_test();
    let threshold = 2;

    // send votes
    tokio::spawn(async move {
        for i in 0..threshold {
            let vote = Vote::new_test();
            let received = ReceivedObject {
                object: vote,
                peer_id: format!("peer_{}", i),
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

    let header_builder = HeaderBuilder {
        network_tx,
        certificate_tx,
        keypair,
        _db: db,
        header_trigger_rx,
        votes_rx,
        digests_buffer,
        committee,
        sync_status_rx,
    };

    // run header builder
    let handle = tokio::spawn(async move {
        header_builder.run().await.unwrap();
    });

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

    let header_builder = HeaderBuilder {
        network_tx,
        certificate_tx,
        keypair,
        _db: db,
        header_trigger_rx,
        votes_rx,
        digests_buffer,
        committee,
        sync_status_rx,
    };

    // run header builder
    let handle = tokio::spawn(async move {
        header_builder.run().await.unwrap();
    });

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

    let header_builder = HeaderBuilder {
        network_tx,
        certificate_tx,
        keypair,
        _db: db,
        header_trigger_rx,
        votes_rx,
        digests_buffer,
        committee,
        sync_status_rx,
    };

    // run header builder
    let handle = tokio::spawn(async move {
        header_builder.run().await.unwrap();
    });

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

    let header_builder = HeaderBuilder {
        network_tx,
        certificate_tx,
        keypair,
        _db: db,
        header_trigger_rx,
        votes_rx,
        digests_buffer,
        committee,
        sync_status_rx,
    };

    // run header builder
    let handle = tokio::spawn(async move {
        header_builder.run().await.unwrap();
    });

    // trigger header building but don't send any votes
    let mut certs = HashSet::new();
    certs.insert(Certificate::genesis([0; 32]));
    header_trigger_tx.send((1, certs)).unwrap();

    // wait for timeout duration
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

    let header_builder = HeaderBuilder {
        network_tx,
        certificate_tx,
        keypair,
        _db: db,
        header_trigger_rx,
        votes_rx,
        digests_buffer,
        committee,
        sync_status_rx,
    };

    // run header builder
    let handle = tokio::spawn(async move {
        header_builder.run().await.unwrap();
    });

    // send invalid votes
    let invalid_vote = Vote::new_test_invalid();
    votes_tx.send(ReceivedObject::new(invalid_vote, "peer1".to_string())).unwrap();

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

    let header_builder = HeaderBuilder {
        network_tx,
        certificate_tx,
        keypair,
        _db: db,
        header_trigger_rx,
        votes_rx,
        digests_buffer: digests_buffer.clone(),
        committee,
        sync_status_rx,
    };

    // Run header builder in background
    let handle = tokio::spawn(async move {
        header_builder.run().await.unwrap();
    });

    // Add digests to buffer
    {
        let mut buffer = digests_buffer.lock().await;
        buffer.push(BatchId::new([1; 32]));
        buffer.push(BatchId::new([2; 32]));
        // This should overwrite the first digest
        buffer.push(BatchId::new([3; 32]));
    }

    // Verify buffer state
    {
        let buffer = digests_buffer.lock().await;
        assert_eq!(buffer.len(), 2);
        assert_eq!(buffer.as_slice()[0], BatchId::new([2; 32]));
        assert_eq!(buffer.as_slice()[1], BatchId::new([3; 32]));
    }

    handle.abort();
} 