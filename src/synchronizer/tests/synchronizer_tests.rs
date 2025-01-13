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
use rstest::*;
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

type TestReceivedObject = ReceivedObject<SyncResponse>;
type TestRequestedObject = RequestedObject<MockData>;
type BoxedFetch = Box<dyn Fetch + Send + Sync + 'static>;

#[fixture]
fn test_data() -> MockData {
    MockData {
        data: vec![1, 2, 3],
    }
}

#[fixture]
fn test_data_set() -> Vec<MockData> {
    vec![
        MockData {
            data: vec![1, 2, 3],
        },
        MockData {
            data: vec![4, 5, 6],
        },
        MockData {
            data: vec![7, 8, 9],
        },
    ]
}

#[fixture]
fn channels() -> (
    mpsc::Sender<NetworkRequest>,
    mpsc::Receiver<NetworkRequest>,
    broadcast::Sender<TestReceivedObject>,
    broadcast::Receiver<TestReceivedObject>,
    mpsc::Sender<BoxedFetch>,
    mpsc::Receiver<BoxedFetch>,
) {
    let (network_tx, network_rx) = mpsc::channel(100);
    let (sync_tx, sync_rx) = broadcast::channel(100);
    let (commands_tx, commands_rx) = mpsc::channel(100);
    (
        network_tx,
        network_rx,
        sync_tx,
        sync_rx,
        commands_tx,
        commands_rx,
    )
}

#[fixture]
fn peers() -> (PeerId, PeerId, PeerId) {
    (PeerId::random(), PeerId::random(), PeerId::random())
}

#[fixture]
fn fetcher_handle(
    channels: (
        mpsc::Sender<NetworkRequest>,
        mpsc::Receiver<NetworkRequest>,
        broadcast::Sender<TestReceivedObject>,
        broadcast::Receiver<TestReceivedObject>,
        mpsc::Sender<BoxedFetch>,
        mpsc::Receiver<BoxedFetch>,
    ),
) -> (CancellationToken, tokio::task::JoinHandle<()>) {
    let (network_tx, _, _, sync_rx, _, commands_rx) = channels;
    let token = CancellationToken::new();
    let handle = Fetcher::spawn(
        token.clone(),
        network_tx,
        commands_rx,
        sync_rx,
        Arc::new(MockConnector),
        10,
    );
    (token, handle)
}

// Helper function to create a valid batch response
fn create_valid_response(request_id: Digest) -> SyncResponse {
    let tx = Transaction::random(32);
    let batch = Batch::new(vec![tx]);
    let sync_data = SyncData::Batches(vec![batch]);
    SyncResponse::Success(request_id, sync_data)
}

// Helper function to create an invalid batch response
fn create_invalid_response() -> SyncResponse {
    let different_data = MockData {
        data: vec![4, 5, 6],
    };
    let tx = Transaction::random(32);
    let batch = Batch::new(vec![tx]);
    let sync_data = SyncData::Batches(vec![batch]);
    let request_id = different_data.into_sync_request().digest();
    SyncResponse::Success(request_id, sync_data)
}

#[rstest]
#[tokio::test]
async fn test_synchronizer_invalid_response_data(
    test_data: MockData,
    channels: (
        mpsc::Sender<NetworkRequest>,
        mpsc::Receiver<NetworkRequest>,
        broadcast::Sender<TestReceivedObject>,
        broadcast::Receiver<TestReceivedObject>,
        mpsc::Sender<BoxedFetch>,
        mpsc::Receiver<BoxedFetch>,
    ),
    peers: (PeerId, PeerId, PeerId),
) {
    let (network_tx, mut network_rx, sync_tx, sync_rx, commands_tx, commands_rx) = channels;
    let (peer_id, peer_id2, _) = peers;

    let provider = Box::new(MockDataProvider {
        peers: vec![peer_id, peer_id2],
    });

    let requested_object = TestRequestedObject {
        object: test_data.clone(),
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
    commands_tx
        .send(Box::new(requested_object) as BoxedFetch)
        .await
        .unwrap();

    // Verify initial request is sent with timeout
    let initial_request =
        tokio::time::timeout(tokio::time::Duration::from_secs(5), network_rx.recv())
            .await
            .expect("Timed out waiting for initial request")
            .expect("Channel closed unexpectedly");

    match initial_request {
        NetworkRequest::SendTo(pid, RequestPayload::SyncRequest(sync_req)) => {
            assert_eq!(pid, peer_id);
            let expected_digest = blake3::hash(&test_data.bytes()).into();
            assert_eq!(sync_req, SyncRequest::Batches(vec![expected_digest]));

            // Send invalid response
            let response = create_invalid_response();
            sync_tx
                .send(TestReceivedObject {
                    object: response,
                    sender: pid,
                })
                .unwrap();
        }
        _ => panic!("Expected initial SendTo request with SyncRequest payload"),
    }

    // Give some time for the invalid response to be processed
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Verify retry request is sent with timeout
    let retry_request =
        tokio::time::timeout(tokio::time::Duration::from_secs(5), network_rx.recv())
            .await
            .expect("Timed out waiting for retry request")
            .expect("Channel closed unexpectedly");

    match retry_request {
        NetworkRequest::SendTo(pid, RequestPayload::SyncRequest(_)) => {
            assert_eq!(pid, peer_id2, "Retry should use the second peer");

            // Send valid response from second peer to complete the test
            let request_id = test_data.into_sync_request().digest();
            let response = create_valid_response(request_id);
            sync_tx
                .send(TestReceivedObject {
                    object: response,
                    sender: peer_id2,
                })
                .unwrap();
        }
        _ => panic!("Expected retry request with SyncRequest payload"),
    }

    // Verify no more requests are sent
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    assert!(
        network_rx.try_recv().is_err(),
        "No more requests should be sent after successful response"
    );

    // Clean shutdown
    drop(commands_tx);
    token.cancel();
    let _ = handle.await;
}

#[rstest]
#[tokio::test]
async fn test_synchronizer_multiple_peers(
    test_data: MockData,
    channels: (
        mpsc::Sender<NetworkRequest>,
        mpsc::Receiver<NetworkRequest>,
        broadcast::Sender<TestReceivedObject>,
        broadcast::Receiver<TestReceivedObject>,
        mpsc::Sender<BoxedFetch>,
        mpsc::Receiver<BoxedFetch>,
    ),
    peers: (PeerId, PeerId, PeerId),
) {
    let (network_tx, mut network_rx, sync_tx, sync_rx, commands_tx, commands_rx) = channels;
    let (peer1, peer2, peer3) = peers;

    let provider = Box::new(MockDataProvider {
        peers: vec![peer1, peer2, peer3],
    });

    let requested_object = TestRequestedObject {
        object: test_data.clone(),
        source: provider.clone(),
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
    commands_tx
        .send(Box::new(requested_object) as BoxedFetch)
        .await
        .unwrap();

    // Verify first request is sent
    let request = network_rx.recv().await.unwrap();
    match request {
        NetworkRequest::SendTo(pid, RequestPayload::SyncRequest(sync_req)) => {
            assert_eq!(pid, peer1);
            let expected_digest = blake3::hash(&test_data.bytes()).into();
            assert_eq!(sync_req, SyncRequest::Batches(vec![expected_digest]));

            // Send successful response
            let request_id = test_data.into_sync_request().digest();
            let response = create_valid_response(request_id);
            sync_tx
                .send(TestReceivedObject {
                    object: response,
                    sender: peer1,
                })
                .unwrap();
        }
        _ => panic!("Expected SendTo request with SyncRequest payload"),
    }

    // Verify no more requests are sent
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    assert!(
        network_rx.try_recv().is_err(),
        "No more requests should be sent after successful response"
    );

    // Clean shutdown
    drop(commands_tx);
    token.cancel();
    let _ = handle.await;
}

#[rstest]
#[tokio::test]
async fn test_synchronizer_concurrent_requests(
    test_data_set: Vec<MockData>,
    channels: (
        mpsc::Sender<NetworkRequest>,
        mpsc::Receiver<NetworkRequest>,
        broadcast::Sender<TestReceivedObject>,
        broadcast::Receiver<TestReceivedObject>,
        mpsc::Sender<BoxedFetch>,
        mpsc::Receiver<BoxedFetch>,
    ),
    peers: (PeerId, PeerId, PeerId),
) {
    let (network_tx, mut network_rx, sync_tx, sync_rx, commands_tx, commands_rx) = channels;
    let (peer1, peer2, peer3) = peers;

    let token = CancellationToken::new();
    let handle = Fetcher::spawn(
        token.clone(),
        network_tx,
        commands_rx,
        sync_rx,
        Arc::new(MockConnector),
        10,
    );

    // Send multiple requests concurrently and track their digests
    let mut expected_digests = HashSet::new();
    for test_data in test_data_set.iter() {
        let provider = Box::new(MockDataProvider {
            peers: vec![peer1, peer2, peer3],
        });

        let requested_object = TestRequestedObject {
            object: test_data.clone(),
            source: provider,
        };

        // Store the expected digest
        let sync_req = test_data.into_sync_request();
        if let SyncRequest::Batches(digests) = sync_req {
            expected_digests.insert(digests[0]);
        }

        commands_tx
            .send(Box::new(requested_object) as BoxedFetch)
            .await
            .unwrap();
    }

    // Verify all requests are processed
    let mut received_digests = HashSet::new();
    for _ in 0..test_data_set.len() {
        let request = network_rx.recv().await.unwrap();
        match request {
            NetworkRequest::SendTo(
                pid,
                RequestPayload::SyncRequest(SyncRequest::Batches(digests)),
            ) => {
                let digest = digests[0];
                assert!(
                    expected_digests.contains(&digest),
                    "Received unexpected request digest: {:?}, expected one of: {:?}",
                    digest,
                    expected_digests
                );
                received_digests.insert(digest);

                // Send successful response
                let response = create_valid_response(digest);
                sync_tx
                    .send(TestReceivedObject {
                        object: response,
                        sender: pid,
                    })
                    .unwrap();
            }
            _ => panic!("Expected SendTo request with SyncRequest payload"),
        }
    }

    // Verify we received requests for all expected digests
    assert_eq!(
        received_digests.len(),
        expected_digests.len(),
        "Should receive requests for all test data"
    );
    assert!(
        received_digests.is_subset(&expected_digests),
        "All received digests should be from our requests"
    );
    assert!(
        expected_digests.is_subset(&received_digests),
        "All expected digests should be requested"
    );

    // Verify no more requests are sent
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    assert!(
        network_rx.try_recv().is_err(),
        "No more requests should be sent after all responses"
    );

    // Clean shutdown
    drop(commands_tx);
    token.cancel();
    let _ = handle.await;
}

#[rstest]
#[tokio::test]
async fn test_synchronizer_retry_on_failure(
    test_data: MockData,
    channels: (
        mpsc::Sender<NetworkRequest>,
        mpsc::Receiver<NetworkRequest>,
        broadcast::Sender<TestReceivedObject>,
        broadcast::Receiver<TestReceivedObject>,
        mpsc::Sender<BoxedFetch>,
        mpsc::Receiver<BoxedFetch>,
    ),
    peers: (PeerId, PeerId, PeerId),
) {
    let (network_tx, mut network_rx, sync_tx, sync_rx, commands_tx, commands_rx) = channels;
    let (peer1, peer2, _) = peers;

    let provider = Box::new(MockDataProvider {
        peers: vec![peer1, peer2],
    });

    let requested_object = TestRequestedObject {
        object: test_data.clone(),
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
    commands_tx
        .send(Box::new(requested_object) as BoxedFetch)
        .await
        .unwrap();

    // Verify first request is sent
    let request = network_rx.recv().await.unwrap();
    match request {
        NetworkRequest::SendTo(pid, RequestPayload::SyncRequest(_)) => {
            assert_eq!(pid, peer1, "First request should be sent to peer1");

            // Send failure response
            let request_id = test_data.into_sync_request().digest();
            let response = SyncResponse::Failure(request_id);
            sync_tx
                .send(TestReceivedObject {
                    object: response,
                    sender: peer1,
                })
                .unwrap();
        }
        _ => panic!("Expected SendTo request with SyncRequest payload"),
    }

    // Verify retry request is sent to second peer
    let retry_request = network_rx.recv().await.unwrap();
    match retry_request {
        NetworkRequest::SendTo(pid, RequestPayload::SyncRequest(_)) => {
            assert_eq!(pid, peer2, "Retry should be sent to peer2");
            assert_ne!(pid, peer1, "Retry should use a different peer");

            // Send successful response from second peer
            let request_id = test_data.into_sync_request().digest();
            let response = create_valid_response(request_id);
            sync_tx
                .send(TestReceivedObject {
                    object: response,
                    sender: peer2,
                })
                .unwrap();
        }
        _ => panic!("Expected retry request with SyncRequest payload"),
    }

    // Verify no more requests are sent
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    assert!(
        network_rx.try_recv().is_err(),
        "No more requests should be sent after successful response"
    );

    // Clean shutdown
    drop(commands_tx);
    token.cancel();
    let _ = handle.await;
}
