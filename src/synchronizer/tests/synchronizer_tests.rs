use std::{collections::HashSet, sync::Arc};

use tokio::sync::{broadcast, mpsc};

use crate::{
    synchronizer::{
        fetcher::Fetcher,
        traits::{DataProvider, Fetch},
        RequestedObject,
    },
    types::{
        network::{NetworkRequest, ReceivedObject, RequestPayload, SyncRequest, SyncResponse},
        traits::{AsBytes, Hash},
    },
};

#[derive(Clone, Debug)]
struct MockData {
    id: String,
}

impl Hash for MockData {
    fn hash(&self) -> String {
        self.id.clone()
    }
}

impl AsBytes for MockData {
    fn bytes(&self) -> Vec<u8> {
        self.id.as_bytes().to_vec()
    }
}

struct MockDataProvider {
    peers: Vec<String>,
}

impl DataProvider for MockDataProvider {
    fn peers(&self) -> Vec<String> {
        self.peers.clone()
    }
}

#[tokio::test]
async fn test_fetcher_basic() {
    let (requests_tx, mut requests_rx) = mpsc::channel(100);
    let (responses_tx, responses_rx) = broadcast::channel(100);
    
    // create mock data and provider
    let mock_data = MockData {
        id: "test_data".to_string(),
    };
    let mock_provider = Box::new(MockDataProvider {
        peers: vec!["peer1".to_string(), "peer2".to_string()],
    });
    
    let requested_object = RequestedObject {
        object: mock_data,
        source: mock_provider,
    };
    
    // create and start fetcher
    let mut fetcher = Fetcher::new();
    fetcher.push(Box::new(requested_object));
    
    // run fetcher
    let handle = tokio::spawn(async move {
        fetcher.run(requests_tx, responses_rx).await.unwrap();
    });
    
    // verify request is sent
    let request = requests_rx.recv().await.unwrap();
    match request {
        NetworkRequest::Sync { request, peer_id } => {
            assert!(peer_id == "peer1" || peer_id == "peer2");
        }
        _ => panic!("Expected sync request"),
    }
    
    handle.abort();
}

#[tokio::test]
async fn test_fetcher_response_handling() {
    let (requests_tx, _) = mpsc::channel(100);
    let (responses_tx, responses_rx) = broadcast::channel(100);
    
    // create mock data and provider
    let mock_data = MockData {
        id: "test_data".to_string(),
    };
    let mock_provider = Box::new(MockDataProvider {
        peers: vec!["peer1".to_string()],
    });
    
    let requested_object = RequestedObject {
        object: mock_data.clone(),
        source: mock_provider,
    };
    
    // create and start fetcher
    let mut fetcher = Fetcher::new();
    fetcher.push(Box::new(requested_object));
    
    // run fetcher
    let handle = tokio::spawn(async move {
        fetcher.run(requests_tx, responses_rx).await.unwrap();
    });
    
    // send mock response
    let response = SyncResponse::Success(RequestPayload::Data(mock_data.bytes()));
    let received_response = ReceivedObject {
        object: response,
        peer_id: "peer1".to_string(),
    };
    responses_tx.send(received_response).unwrap();
    
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    
    handle.abort();
}

#[tokio::test]
async fn test_fetcher_multiple_objects() {
    let (requests_tx, mut requests_rx) = mpsc::channel(100);
    let (responses_tx, responses_rx) = broadcast::channel(100);
    
    let mock_data1 = MockData {
        id: "test_data1".to_string(),
    };
    let mock_data2 = MockData {
        id: "test_data2".to_string(),
    };
    
    let mock_provider = Box::new(MockDataProvider {
        peers: vec!["peer1".to_string()],
    });
    
    let requested_object1 = RequestedObject {
        object: mock_data1,
        source: mock_provider.clone(),
    };
    let requested_object2 = RequestedObject {
        object: mock_data2,
        source: mock_provider,
    };
    
    // create and start fetcher
    let mut fetcher = Fetcher::new();
    fetcher.push(Box::new(requested_object1));
    fetcher.push(Box::new(requested_object2));
    
    // run fetcher
    let handle = tokio::spawn(async move {
        fetcher.run(requests_tx, responses_rx).await.unwrap();
    });
    
    // verify multiple requests are sent
    let request1 = requests_rx.recv().await.unwrap();
    let request2 = requests_rx.recv().await.unwrap();
    
    assert!(matches!(request1, NetworkRequest::Sync { .. }));
    assert!(matches!(request2, NetworkRequest::Sync { .. }));
    
    handle.abort();
}

#[tokio::test]
async fn test_synchronizer_concurrent_requests() {
    let (requests_tx, mut requests_rx) = mpsc::channel(100);
    let (responses_tx, responses_rx) = broadcast::channel(100);
    
    // create multiple mock objects with different providers
    let mock_data1 = MockData {
        id: "data1".to_string(),
    };
    let mock_data2 = MockData {
        id: "data2".to_string(),
    };
    let mock_data3 = MockData {
        id: "data3".to_string(),
    };
    
    let provider1 = Box::new(MockDataProvider {
        peers: vec!["peer1".to_string()],
    });
    let provider2 = Box::new(MockDataProvider {
        peers: vec!["peer2".to_string()],
    });
    let provider3 = Box::new(MockDataProvider {
        peers: vec!["peer3".to_string()],
    });
    
    let mut fetcher = Fetcher::new();
    fetcher.push(Box::new(RequestedObject {
        object: mock_data1.clone(),
        source: provider1,
    }));
    fetcher.push(Box::new(RequestedObject {
        object: mock_data2.clone(),
        source: provider2,
    }));
    fetcher.push(Box::new(RequestedObject {
        object: mock_data3.clone(),
        source: provider3,
    }));
    
    // run fetcher
    let handle = tokio::spawn(async move {
        fetcher.run(requests_tx, responses_rx).await.unwrap();
    });
    
    // collect all requests
    let mut received_requests = HashSet::new();
    for _ in 0..3 {
        if let NetworkRequest::Sync { request: _, peer_id } = requests_rx.recv().await.unwrap() {
            received_requests.insert(peer_id);
        }
    }
    
    assert_eq!(received_requests.len(), 3);
    assert!(received_requests.contains("peer1"));
    assert!(received_requests.contains("peer2"));
    assert!(received_requests.contains("peer3"));
    
    handle.abort();
}

#[tokio::test]
async fn test_synchronizer_retry_on_failure() {
    let (requests_tx, mut requests_rx) = mpsc::channel(100);
    let (responses_tx, responses_rx) = broadcast::channel(100);
    
    let mock_data = MockData {
        id: "test_data".to_string(),
    };
    
    let provider = Box::new(MockDataProvider {
        peers: vec!["peer1".to_string(), "peer2".to_string()],
    });
    
    let requested_object = RequestedObject {
        object: mock_data.clone(),
        source: provider,
    };
    
    let mut fetcher = Fetcher::new();
    fetcher.push(Box::new(requested_object));
    
    // run fetcher
    let handle = tokio::spawn(async move {
        fetcher.run(requests_tx, responses_rx).await.unwrap();
    });
    
    // get first request
    let request1 = requests_rx.recv().await.unwrap();
    match request1 {
        NetworkRequest::Sync { request: _, peer_id } => {
            // send error response
            let error_response = SyncResponse::Error("test error".to_string());
            responses_tx
                .send(ReceivedObject {
                    object: error_response,
                    peer_id: peer_id.clone(),
                })
                .unwrap();
        }
        _ => panic!("Expected sync request"),
    }
    
    // should get another request to different peer
    let request2 = requests_rx.recv().await.unwrap();
    match request2 {
        NetworkRequest::Sync { request: _, peer_id } => {
            assert_ne!(
                peer_id,
                match request1 {
                    NetworkRequest::Sync { peer_id, .. } => peer_id,
                    _ => panic!("Expected sync request"),
                }
            );
        }
        _ => panic!("Expected sync request"),
    }
    
    handle.abort();
}

#[tokio::test]
async fn test_synchronizer_partial_response() {
    let (requests_tx, mut requests_rx) = mpsc::channel(100);
    let (responses_tx, responses_rx) = broadcast::channel(100);
    
    let mock_data1 = MockData {
        id: "data1".to_string(),
    };
    let mock_data2 = MockData {
        id: "data2".to_string(),
    };
    
    let provider = Box::new(MockDataProvider {
        peers: vec!["peer1".to_string()],
    });
    
    let mut fetcher = Fetcher::new();
    fetcher.push(Box::new(RequestedObject {
        object: mock_data1.clone(),
        source: provider.clone(),
    }));
    fetcher.push(Box::new(RequestedObject {
        object: mock_data2.clone(),
        source: provider,
    }));
    
    // run fetcher
    let handle = tokio::spawn(async move {
        fetcher.run(requests_tx, responses_rx).await.unwrap();
    });
    
    // get first request
    let request = requests_rx.recv().await.unwrap();
    match request {
        NetworkRequest::Sync { request: _, peer_id } => {
            // Send partial success response
            let response = SyncResponse::Success(RequestPayload::Data(mock_data1.bytes()));
            responses_tx
                .send(ReceivedObject {
                    object: response,
                    peer_id,
                })
                .unwrap();
        }
        _ => panic!("Expected sync request"),
    }
    
    // Should get another request for remaining data
    let request = requests_rx.recv().await.unwrap();
    match request {
        NetworkRequest::Sync { request: _, peer_id } => {
            // Send success response for second object
            let response = SyncResponse::Success(RequestPayload::Data(mock_data2.bytes()));
            responses_tx
                .send(ReceivedObject {
                    object: response,
                    peer_id,
                })
                .unwrap();
        }
        _ => panic!("Expected sync request"),
    }
    
    handle.abort();
}

#[tokio::test]
async fn test_synchronizer_invalid_response_data() {
    let (requests_tx, mut requests_rx) = mpsc::channel(100);
    let (responses_tx, responses_rx) = broadcast::channel(100);
    
    let mock_data = MockData {
        id: "test_data".to_string(),
    };
    
    let provider = Box::new(MockDataProvider {
        peers: vec!["peer1".to_string(), "peer2".to_string()],
    });
    
    let requested_object = RequestedObject {
        object: mock_data.clone(),
        source: provider,
    };
    
    let mut fetcher = Fetcher::new();
    fetcher.push(Box::new(requested_object));
    
    // run fetcher
    let handle = tokio::spawn(async move {
        fetcher.run(requests_tx, responses_rx).await.unwrap();
    });
    
    // get request
    let request = requests_rx.recv().await.unwrap();
    match request {
        NetworkRequest::Sync { request: _, peer_id } => {
            // Send invalid response data
            let invalid_data = vec![0, 1, 2]; // Different from what was requested
            let response = SyncResponse::Success(RequestPayload::Data(invalid_data));
            responses_tx
                .send(ReceivedObject {
                    object: response,
                    peer_id,
                })
                .unwrap();
        }
        _ => panic!("Expected sync request"),
    }
    
    // should get another request due to invalid response
    let request = requests_rx.recv().await.unwrap();
    assert!(matches!(request, NetworkRequest::Sync { .. }));
    
    handle.abort();
} 