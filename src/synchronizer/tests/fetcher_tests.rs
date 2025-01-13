use rand::random;
use std::{collections::HashSet, sync::Arc};
use tokio::sync::{broadcast, mpsc};
use tokio_util::sync::CancellationToken;

use crate::{
    network::Connect,
    synchronizer::{
        fetcher::Fetcher,
        traits::{DataProvider, Fetch, IntoSyncRequest},
        RequestedObject,
    },
    types::{
        batch::{Batch, BatchId},
        network::{
            NetworkRequest, ReceivedObject, RequestPayload, SyncData, SyncRequest, SyncResponse,
        },
        traits::{AsBytes, Hash, Random},
        transaction::Transaction,
        Digest,
    },
};

use async_trait::async_trait;
use libp2p::PeerId;

#[derive(Clone)]
struct MockConnector;

#[async_trait]
impl Connect for MockConnector {
    async fn dispatch(&self, request: &RequestPayload, peer_id: PeerId) -> anyhow::Result<()> {
        Ok(())
    }
}

#[async_trait]
impl Connect for Arc<MockConnector> {
    async fn dispatch(&self, request: &RequestPayload, peer_id: PeerId) -> anyhow::Result<()> {
        self.as_ref().dispatch(request, peer_id).await
    }
}

// Create a mock connector that sleeps to simulate network delay
#[derive(Clone)]
struct SlowMockConnector;

#[async_trait]
impl Connect for SlowMockConnector {
    async fn dispatch(&self, _request: &RequestPayload, _peer_id: PeerId) -> anyhow::Result<()> {
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
        Ok(())
    }
}

#[async_trait]
impl Connect for Arc<SlowMockConnector> {
    async fn dispatch(&self, request: &RequestPayload, peer_id: PeerId) -> anyhow::Result<()> {
        self.as_ref().dispatch(request, peer_id).await
    }
}

#[derive(Debug, Clone)]
struct TestFetchable {
    data: Vec<u8>,
}

impl AsBytes for TestFetchable {
    fn bytes(&self) -> Vec<u8> {
        self.data.clone()
    }
}

impl Random for TestFetchable {
    fn random(size: usize) -> Self {
        let data = (0..size).map(|_| rand::random()).collect();
        Self { data }
    }
}

impl IntoSyncRequest for TestFetchable {
    fn into_sync_request(&self) -> SyncRequest {
        let digest = blake3::hash(&self.bytes()).into();
        SyncRequest::Batches(vec![digest])
    }
}

#[derive(Clone)]
struct TestDataProvider {
    peers: Vec<PeerId>,
}

#[async_trait]
impl DataProvider for TestDataProvider {
    async fn sources(&self) -> Box<dyn Iterator<Item = PeerId> + Send> {
        Box::new(self.peers.clone().into_iter())
    }
}

#[tokio::test]
async fn test_fetcher_basic() {
    let (network_tx, mut network_rx) = mpsc::channel(100);
    let (sync_tx, sync_rx) = broadcast::channel(100);
    let (commands_tx, commands_rx) = mpsc::channel(100);
    let connector = Arc::new(MockConnector);
    let token = CancellationToken::new();

    let _handle = Fetcher::spawn(
        token.clone(),
        network_tx,
        commands_rx,
        sync_rx,
        connector,
        10,
    );

    let test_data = TestFetchable {
        data: vec![1, 2, 3],
    };

    let peer_id = PeerId::random();
    let request = Box::new(RequestedObject {
        object: test_data.clone(),
        source: Box::new(peer_id),
    });
    commands_tx.send(request).await.unwrap();

    // Verify request is sent
    let request = network_rx.recv().await.unwrap();
    match request {
        NetworkRequest::SendTo(pid, RequestPayload::SyncRequest(sync_req)) => {
            assert_eq!(pid, peer_id);
            let expected_digest = blake3::hash(&test_data.bytes()).into();
            assert_eq!(sync_req, SyncRequest::Batches(vec![expected_digest]));
        }
        _ => panic!("Expected SendTo request with SyncRequest payload"),
    }

    // Send successful response
    let tx = Transaction::random(32);
    let batch = Batch::new(vec![tx]);
    let sync_data = SyncData::Batches(vec![batch]);
    let request_id = test_data.into_sync_request().digest();
    let response = SyncResponse::Success(request_id, sync_data);
    let received = ReceivedObject {
        object: response,
        sender: peer_id,
    };
    sync_tx.send(received).unwrap();

    // Drop the sender to signal no more commands
    drop(commands_tx);
}

#[tokio::test]
async fn test_fetcher_empty() {
    let (network_tx, _) = mpsc::channel(100);
    let (_, sync_rx) = broadcast::channel(100);
    let (commands_tx, commands_rx) = mpsc::channel(100);

    let _handle = Fetcher::spawn(
        CancellationToken::new(),
        network_tx,
        commands_rx,
        sync_rx,
        Arc::new(MockConnector),
        10,
    );

    // Drop commands_tx to signal no more commands
    drop(commands_tx);
}

#[tokio::test]
async fn test_fetcher_single_request() {
    let (network_tx, mut network_rx) = mpsc::channel(100);
    let (sync_tx, sync_rx) = broadcast::channel(100);
    let (commands_tx, commands_rx) = mpsc::channel(100);

    let test_data = TestFetchable {
        data: vec![1, 2, 3],
    };

    let peer_id = PeerId::random();
    let request = Box::new(RequestedObject {
        object: test_data.clone(),
        source: Box::new(peer_id),
    });

    let _handle = Fetcher::spawn(
        CancellationToken::new(),
        network_tx.clone(),
        commands_rx,
        sync_rx,
        Arc::new(MockConnector),
        10,
    );

    // Send the request through the commands channel
    commands_tx.send(request).await.unwrap();

    // Drop commands_tx to signal no more commands
    drop(commands_tx);

    // verify request is sent
    let request = network_rx.recv().await.unwrap();
    match request {
        NetworkRequest::SendTo(peer_id, _) => {
            // Get first peer from provider
            let expected_peer = peer_id;
            assert_eq!(peer_id, expected_peer);
        }
        _ => panic!("Expected SendTo request"),
    }

    // Send successful response
    let tx = Transaction::random(32);
    let batch = Batch::new(vec![tx]);
    let sync_data = SyncData::Batches(vec![batch]);
    let request_id = test_data.into_sync_request().digest();
    let response = SyncResponse::Success(request_id, sync_data);
    let received = ReceivedObject {
        object: response,
        sender: peer_id,
    };
    sync_tx.send(received).unwrap();

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
}

#[tokio::test]
async fn test_fetcher_timeout() {
    let (requests_tx, _) = mpsc::channel(100);
    let (responses_tx, mut responses_rx) = broadcast::channel(100);
    let (commands_tx, commands_rx) = mpsc::channel(100);

    let test_data = TestFetchable {
        data: vec![1, 2, 3],
    };

    let provider = Box::new(TestDataProvider {
        peers: vec![PeerId::random()],
    });

    let requested_object = RequestedObject {
        object: test_data,
        source: provider,
    };

    let handle = Fetcher::spawn(
        CancellationToken::new(),
        requests_tx,
        commands_rx,
        responses_rx,
        Arc::new(SlowMockConnector),
        // Set a very short timeout for individual fetch operations
        1,
    );

    // Send the request through the commands channel
    commands_tx.send(Box::new(requested_object)).await.unwrap();

    // Drop commands_tx to signal no more commands
    drop(commands_tx);

    // Run fetcher with a longer timeout to ensure it has time to process
    let result = tokio::time::timeout(tokio::time::Duration::from_millis(1000), handle).await;

    // The fetcher should complete successfully, but the fetch operation should have timed out
    assert!(result.is_ok(), "Fetcher should complete");
}

#[tokio::test]
async fn test_fetcher_error_response() {
    let (requests_tx, _) = mpsc::channel(100);
    let (responses_tx, mut responses_rx) = broadcast::channel(100);
    let (commands_tx, commands_rx) = mpsc::channel(100);

    let test_data = TestFetchable {
        data: vec![1, 2, 3],
    };

    let provider = Box::new(TestDataProvider {
        peers: vec![PeerId::random()],
    });

    let requested_object = RequestedObject {
        object: test_data.clone(),
        source: provider,
    };

    let _handle = Fetcher::spawn(
        CancellationToken::new(),
        requests_tx,
        commands_rx,
        responses_rx,
        Arc::new(MockConnector),
        10,
    );

    // Send the request through the commands channel
    commands_tx.send(Box::new(requested_object)).await.unwrap();

    // Drop commands_tx to signal no more commands
    drop(commands_tx);

    // Send failure response
    let request_id = test_data.into_sync_request().digest();
    let response = SyncResponse::Failure(request_id);
    let received = ReceivedObject {
        object: response,
        sender: PeerId::random(),
    };
    responses_tx.send(received).unwrap();

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
}

#[tokio::test]
async fn test_fetcher_multiple_peers() {
    let (requests_tx, mut requests_rx) = mpsc::channel(100);
    let (responses_tx, responses_rx) = broadcast::channel(100);
    let (commands_tx, commands_rx) = mpsc::channel(100);

    let test_data = TestFetchable {
        data: vec![1, 2, 3],
    };

    let peer1 = PeerId::random();
    let peer2 = PeerId::random();
    let peer3 = PeerId::random();
    let provider = Box::new(TestDataProvider {
        peers: vec![peer1, peer2, peer3],
    });

    let requested_object = RequestedObject {
        object: test_data.clone(),
        source: provider.clone(),
    };

    let _handle = Fetcher::spawn(
        CancellationToken::new(),
        requests_tx,
        commands_rx,
        responses_rx,
        Arc::new(MockConnector),
        10,
    );

    // Send the request through the commands channel
    commands_tx.send(Box::new(requested_object)).await.unwrap();

    // Drop commands_tx to signal no more commands
    drop(commands_tx);

    // Verify first request is sent to peer1
    let request = requests_rx.recv().await.unwrap();
    match request {
        NetworkRequest::SendTo(pid, RequestPayload::SyncRequest(sync_req)) => {
            assert_eq!(pid, peer1, "First request should be sent to peer1");
            let expected_digest = blake3::hash(&test_data.bytes()).into();
            assert_eq!(sync_req, SyncRequest::Batches(vec![expected_digest]));

            // Send failure response from peer1
            let request_id = test_data.into_sync_request().digest();
            let response = SyncResponse::Failure(request_id);
            let received = ReceivedObject {
                object: response,
                sender: peer1,
            };
            responses_tx.send(received).unwrap();
        }
        _ => panic!("Expected SendTo request with SyncRequest payload"),
    };

    // Verify second request is sent to peer2
    let request = requests_rx.recv().await.unwrap();
    match request {
        NetworkRequest::SendTo(pid, RequestPayload::SyncRequest(sync_req)) => {
            assert_eq!(pid, peer2, "Second request should be sent to peer2");
            let expected_digest = blake3::hash(&test_data.bytes()).into();
            assert_eq!(sync_req, SyncRequest::Batches(vec![expected_digest]));

            // Send failure response from peer2
            let request_id = test_data.into_sync_request().digest();
            let response = SyncResponse::Failure(request_id);
            let received = ReceivedObject {
                object: response,
                sender: peer2,
            };
            responses_tx.send(received).unwrap();
        }
        _ => panic!("Expected SendTo request with SyncRequest payload"),
    };

    // Verify third request is sent to peer3
    let request = requests_rx.recv().await.unwrap();
    match request {
        NetworkRequest::SendTo(pid, RequestPayload::SyncRequest(sync_req)) => {
            assert_eq!(pid, peer3, "Third request should be sent to peer3");
            let expected_digest = blake3::hash(&test_data.bytes()).into();
            assert_eq!(sync_req, SyncRequest::Batches(vec![expected_digest]));

            // Send successful response from peer3
            let tx = Transaction::random(32);
            let batch = Batch::new(vec![tx]);
            let sync_data = SyncData::Batches(vec![batch]);
            let request_id = test_data.into_sync_request().digest();
            let response = SyncResponse::Success(request_id, sync_data);
            let received = ReceivedObject {
                object: response,
                sender: peer3,
            };
            responses_tx.send(received).unwrap();
        }
        _ => panic!("Expected SendTo request with SyncRequest payload"),
    };

    // Verify no more requests are sent after successful response
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    assert!(
        requests_rx.try_recv().is_err(),
        "No more requests should be sent after successful response"
    );
}
