use crate::{
    network::Connect,
    synchronizer::{
        fetcher::Fetcher,
        traits::{DataProvider, IntoSyncRequest},
        RequestedObject,
    },
    types::{
        batch::{Batch, BatchId},
        network::{NetworkRequest, ReceivedObject, RequestPayload, SyncData, SyncRequest, SyncResponse},
        traits::{AsBytes, Hash, Random},
        transaction::Transaction,
        Digest,
    },
};
use async_trait::async_trait;
use libp2p::PeerId;
use std::{collections::HashSet, sync::Arc};
use tokio::sync::{broadcast, mpsc};
use tokio_util::sync::CancellationToken;

struct MockConnector;

#[async_trait]
impl Connect for MockConnector {
    async fn dispatch(&self, _request: &RequestPayload, _peer_id: PeerId) -> anyhow::Result<()> {
        Ok(())
    }
}

#[async_trait]
impl Connect for Arc<MockConnector> {
    async fn dispatch(&self, request: &RequestPayload, peer_id: PeerId) -> anyhow::Result<()> {
        self.as_ref().dispatch(request, peer_id).await
    }
}

#[derive(Clone)]
struct MockData {
    data: Vec<u8>,
}

impl AsBytes for MockData {
    fn bytes(&self) -> Vec<u8> {
        self.data.clone()
    }
}

impl IntoSyncRequest for MockData {
    fn into_sync_request(&self) -> SyncRequest {
        let digest = blake3::hash(&self.bytes()).into();
        SyncRequest::Batches(vec![digest])
    }
}

#[derive(Clone)]
struct MockDataProvider {
    peers: Vec<PeerId>,
}

#[async_trait]
impl DataProvider for MockDataProvider {
    async fn sources(&self) -> Box<dyn Iterator<Item = PeerId> + Send> {
        Box::new(self.peers.clone().into_iter())
    }
}

#[tokio::test]
async fn test_synchronizer_invalid_response_data() {
    let (network_tx, mut network_rx) = mpsc::channel(100);
    let (commands_tx, commands_rx) = mpsc::channel(100);
    let (sync_tx, sync_rx) = broadcast::channel(100);
    
    let mock_data = MockData {
        data: vec![1, 2, 3],
    };
    
    // Create multiple peers for retry mechanism
    let peer_id = PeerId::random();
    let peer_id2 = PeerId::random();
    let provider = Box::new(MockDataProvider {
        peers: vec![peer_id, peer_id2],
    });
    
    let requested_object = RequestedObject {
        object: mock_data.clone(),
        source: provider,
    };
    
    let token = CancellationToken::new();
    let handle = Fetcher::spawn(
        token.clone(),
        network_tx,
        commands_rx,
        sync_rx,
        Arc::new(MockConnector),
        10,
    );
    
    // Send the request through the commands channel
    let request = Box::new(requested_object);
    commands_tx.send(request).await.unwrap();
    
    // Verify initial request is sent with timeout
    let initial_request = tokio::time::timeout(
        tokio::time::Duration::from_secs(5),
        network_rx.recv()
    ).await.expect("Timed out waiting for initial request")
    .expect("Channel closed unexpectedly");

    match initial_request {
        NetworkRequest::SendTo(pid, RequestPayload::SyncRequest(sync_req)) => {
            assert_eq!(pid, peer_id);
            let expected_digest = blake3::hash(&mock_data.bytes()).into();
            assert_eq!(sync_req, SyncRequest::Batches(vec![expected_digest]));
            
            // Send invalid response - completely different batch with different digest
            let different_data = MockData {
                data: vec![4, 5, 6], // Different data
            };
            let tx = Transaction::random(32);
            let invalid_batch = Batch::new(vec![tx]);
            let sync_data = SyncData::Batches(vec![invalid_batch]);
            
            // Use the digest from the different data to ensure mismatch
            let request_id = different_data.into_sync_request().digest();
            let response = SyncResponse::Success(request_id, sync_data);
            
            // Send the invalid response
            sync_tx.send(ReceivedObject {
                object: response,
                sender: pid,
            }).unwrap();
        }
        _ => panic!("Expected initial SendTo request with SyncRequest payload"),
    }

    // Give some time for the invalid response to be processed
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Verify retry request is sent with timeout
    let retry_request = tokio::time::timeout(
        tokio::time::Duration::from_secs(5),
        network_rx.recv()
    ).await.expect("Timed out waiting for retry request")
    .expect("Channel closed unexpectedly");

    match retry_request {
        NetworkRequest::SendTo(pid, RequestPayload::SyncRequest(_)) => {
            assert_eq!(pid, peer_id2, "Retry should use the second peer");
            
            // Send valid response from second peer to complete the test
            let tx = Transaction::random(32);
            let batch = Batch::new(vec![tx]);
            let sync_data = SyncData::Batches(vec![batch]);
            let request_id = mock_data.into_sync_request().digest();
            let response = SyncResponse::Success(request_id, sync_data);
            sync_tx.send(ReceivedObject {
                object: response,
                sender: peer_id2,
            }).unwrap();
        }
        _ => panic!("Expected retry request with SyncRequest payload"),
    }

    // Verify no more requests are sent
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    assert!(network_rx.try_recv().is_err(), "No more requests should be sent after successful response");
    
    // Clean shutdown
    drop(commands_tx);
    token.cancel();
    let _ = handle.await;
}

#[tokio::test]
async fn test_synchronizer_multiple_peers() {
    let (network_tx, mut network_rx) = mpsc::channel(100);
    let (commands_tx, commands_rx) = mpsc::channel(100);
    let (sync_tx, sync_rx) = broadcast::channel(100);
    
    let mock_data = MockData {
        data: vec![1, 2, 3],
    };
    
    let provider = Box::new(MockDataProvider {
        peers: vec![PeerId::random(), PeerId::random(), PeerId::random()],
    });
    
    let requested_object = RequestedObject {
        object: mock_data.clone(),
        source: provider.clone(),
    };
    
    let _handle = Fetcher::spawn(
        CancellationToken::new(),
        network_tx,
        commands_rx,
        sync_rx,
        Arc::new(MockConnector),
        10,
    );
    
    // Send the request through the commands channel
    let request = Box::new(requested_object);
    commands_tx.send(request).await.unwrap();
    
    // Drop commands_tx to signal no more commands
    drop(commands_tx);
    
    // verify first request is sent
    let request = network_rx.recv().await.unwrap();
    let matched_peer_id = match request {
        NetworkRequest::SendTo(pid, RequestPayload::SyncRequest(sync_req)) => {
            let mut peer_ids = provider.sources().await;
            let expected_peer = peer_ids.next().unwrap();
            assert_eq!(pid, expected_peer);
            let expected_digest = blake3::hash(&mock_data.bytes()).into();
            assert_eq!(sync_req, SyncRequest::Batches(vec![expected_digest]));
            pid
        }
        _ => panic!("Expected SendTo request with SyncRequest payload"),
    };
    
    // Send successful response
    let tx = Transaction::random(32);
    let batch = Batch::new(vec![tx]);
    let sync_data = SyncData::Batches(vec![batch]);
    let request_id = mock_data.into_sync_request().digest();
    let response = SyncResponse::Success(request_id, sync_data);
    let received = ReceivedObject {
        object: response,
        sender: matched_peer_id,
    };
    sync_tx.send(received).unwrap();
    
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
}

#[tokio::test]
async fn test_fetcher_multiple_objects() {
    let (network_tx, mut network_rx) = mpsc::channel(100);
    let (commands_tx, commands_rx) = mpsc::channel(100);
    let (sync_tx, sync_rx) = broadcast::channel(100);
    
    let mock_data1 = MockData {
        data: vec![1, 2, 3],
    };
    let mock_data2 = MockData {
        data: vec![4, 5, 6],
    };
    
    let mock_provider = Box::new(MockDataProvider {
        peers: vec![PeerId::random(), PeerId::random(), PeerId::random()],
    });
    
    let requested_object1 = RequestedObject {
        object: mock_data1,
        source: mock_provider.clone(),
    };
    let requested_object2 = RequestedObject {
        object: mock_data2,
        source: mock_provider,
    };
    
    let _handle = Fetcher::spawn(
        CancellationToken::new(),
        network_tx,
        commands_rx,
        sync_rx,
        Arc::new(MockConnector),
        10,
    );

    // Send requests through the commands channel
    commands_tx.send(Box::new(requested_object1)).await.unwrap();
    commands_tx.send(Box::new(requested_object2)).await.unwrap();
    
    // Drop commands_tx to signal no more commands
    drop(commands_tx);
    
    // Verify requests are sent
    let request1 = network_rx.recv().await.unwrap();
    let request2 = network_rx.recv().await.unwrap();
    
    assert!(matches!(request1, NetworkRequest::SendTo(_, _)));
    assert!(matches!(request2, NetworkRequest::SendTo(_, _)));
}

#[tokio::test]
async fn test_synchronizer_concurrent_requests() {
    let (network_tx, mut network_rx) = mpsc::channel(100);
    let (commands_tx, commands_rx) = mpsc::channel(100);
    let (sync_tx, sync_rx) = broadcast::channel(100);
    
    let mock_data1 = MockData {
        data: vec![1, 2, 3],
    };
    let mock_data2 = MockData {
        data: vec![4, 5, 6],
    };
    let mock_data3 = MockData {
        data: vec![7, 8, 9],
    };
    
    let provider1 = Box::new(MockDataProvider {
        peers: vec![PeerId::random(), PeerId::random(), PeerId::random()],
    });
    let provider2 = Box::new(MockDataProvider {
        peers: vec![PeerId::random(), PeerId::random(), PeerId::random()],
    });
    let provider3 = Box::new(MockDataProvider {
        peers: vec![PeerId::random(), PeerId::random(), PeerId::random()],
    });
    
    let _handle = Fetcher::spawn(
        CancellationToken::new(),
        network_tx,
        commands_rx,
        sync_rx,
        Arc::new(MockConnector),
        10,
    );

    // Send requests through the commands channel
    commands_tx.send(Box::new(RequestedObject {
        object: mock_data1.clone(),
        source: provider1,
    })).await.unwrap();
    commands_tx.send(Box::new(RequestedObject {
        object: mock_data2.clone(),
        source: provider2,
    })).await.unwrap();
    commands_tx.send(Box::new(RequestedObject {
        object: mock_data3.clone(),
        source: provider3,
    })).await.unwrap();
    
    // Drop commands_tx to signal no more commands
    drop(commands_tx);
    
    let mut received_requests = HashSet::new();
    for _ in 0..3 {
        if let NetworkRequest::SendTo(peer_id, _) = network_rx.recv().await.unwrap() {
            received_requests.insert(peer_id);
        }
    }
    
    assert_eq!(received_requests.len(), 3);
}

#[tokio::test]
async fn test_synchronizer_retry_on_failure() {
    let (network_tx, mut network_rx) = mpsc::channel(100);
    let (commands_tx, commands_rx) = mpsc::channel(100);
    let (sync_tx, sync_rx) = broadcast::channel(100);
    
    let mock_data = MockData {
        data: vec![1, 2, 3],
    };
    
    let provider = Box::new(MockDataProvider {
        peers: vec![PeerId::random(), PeerId::random()],
    });
    
    let requested_object = RequestedObject {
        object: mock_data.clone(),
        source: provider,
    };
    
    let _handle = Fetcher::spawn(
        CancellationToken::new(),
        network_tx,
        commands_rx,
        sync_rx,
        Arc::new(MockConnector),
        10,
    );

    // Send request through the commands channel
    commands_tx.send(Box::new(requested_object)).await.unwrap();
    
    // Drop commands_tx to signal no more commands
    drop(commands_tx);
    
    let request1 = network_rx.recv().await.unwrap();
    match request1 {
        NetworkRequest::SendTo(peer_id, _) => {
            let response = SyncResponse::Failure(mock_data.into_sync_request().digest());
            sync_tx.send(ReceivedObject {
                object: response,
                sender: peer_id,
            }).unwrap();
        }
        _ => panic!("Expected SendTo request"),
    }
    
    let request2 = network_rx.recv().await.unwrap();
    match request2 {
        NetworkRequest::SendTo(peer_id1, _) => {
            match request1 {
                NetworkRequest::SendTo(peer_id2, _) => assert_ne!(peer_id1, peer_id2),
                _ => panic!("Expected SendTo request"),
            }
        }
        _ => panic!("Expected SendTo request"),
    }
} 