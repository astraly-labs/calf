use std::collections::HashSet;
use tokio::sync::{broadcast, mpsc};

use crate::{
    synchronizer::{
        fetcher::Fetcher,
        traits::{DataProvider, Fetch},
        FetchError, RequestedObject,
    },
    types::{
        network::{NetworkRequest, ReceivedObject, RequestPayload, SyncRequest, SyncResponse},
        traits::{AsBytes, Hash},
    },
};

#[derive(Clone, Debug)]
struct TestFetchable {
    id: String,
    data: Vec<u8>,
}

impl Hash for TestFetchable {
    fn hash(&self) -> String {
        self.id.clone()
    }
}

impl AsBytes for TestFetchable {
    fn bytes(&self) -> Vec<u8> {
        self.data.clone()
    }
}

struct TestDataProvider {
    peers: Vec<String>,
}

impl DataProvider for TestDataProvider {
    fn peers(&self) -> Vec<String> {
        self.peers.clone()
    }
}

#[tokio::test]
async fn test_fetcher_empty() {
    let (requests_tx, _) = mpsc::channel(100);
    let (_, responses_rx) = broadcast::channel(100);
    
    let mut fetcher = Fetcher::new();
    let result = fetcher.run(requests_tx, responses_rx).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_fetcher_single_request() {
    let (requests_tx, mut requests_rx) = mpsc::channel(100);
    let (responses_tx, responses_rx) = broadcast::channel(100);
    
    let test_data = TestFetchable {
        id: "test1".to_string(),
        data: vec![1, 2, 3],
    };
    
    let provider = Box::new(TestDataProvider {
        peers: vec!["peer1".to_string()],
    });
    
    let requested_object = RequestedObject {
        object: test_data.clone(),
        source: provider,
    };
    
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
            assert_eq!(peer_id, "peer1");
        }
        _ => panic!("Expected sync request"),
    }
    
    // send response
    let response = SyncResponse::Success(RequestPayload::Data(test_data.bytes()));
    let received = ReceivedObject {
        object: response,
        peer_id: "peer1".to_string(),
    };
    responses_tx.send(received).unwrap();
    
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    handle.abort();
}

#[tokio::test]
async fn test_fetcher_timeout() {
    let (requests_tx, _) = mpsc::channel(100);
    let (_, responses_rx) = broadcast::channel(100);
    
    let test_data = TestFetchable {
        id: "test1".to_string(),
        data: vec![1, 2, 3],
    };
    
    let provider = Box::new(TestDataProvider {
        peers: vec!["peer1".to_string()],
    });
    
    let requested_object = RequestedObject {
        object: test_data,
        source: provider,
    };
    
    let mut fetcher = Fetcher::new();
    fetcher.push(Box::new(requested_object));
    
    // Run fetcher with very short timeout
    tokio::time::timeout(
        tokio::time::Duration::from_millis(50),
        fetcher.run(requests_tx, responses_rx)
    ).await.unwrap_err();
}

#[tokio::test]
async fn test_fetcher_error_response() {
    let (requests_tx, _) = mpsc::channel(100);
    let (responses_tx, responses_rx) = broadcast::channel(100);
    
    let test_data = TestFetchable {
        id: "test1".to_string(),
        data: vec![1, 2, 3],
    };
    
    let provider = Box::new(TestDataProvider {
        peers: vec!["peer1".to_string()],
    });
    
    let requested_object = RequestedObject {
        object: test_data,
        source: provider,
    };
    
    let mut fetcher = Fetcher::new();
    fetcher.push(Box::new(requested_object));
    
    // run fetcher
    let handle = tokio::spawn(async move {
        fetcher.run(requests_tx, responses_rx).await.unwrap();
    });
    
    // send error response
    let response = SyncResponse::Error("test error".to_string());
    let received = ReceivedObject {
        object: response,
        peer_id: "peer1".to_string(),
    };
    responses_tx.send(received).unwrap();
    
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    handle.abort();
}

#[tokio::test]
async fn test_fetcher_multiple_peers() {
    let (requests_tx, mut requests_rx) = mpsc::channel(100);
    let (responses_tx, responses_rx) = broadcast::channel(100);
    
    let test_data = TestFetchable {
        id: "test1".to_string(),
        data: vec![1, 2, 3],
    };
    
    let provider = Box::new(TestDataProvider {
        peers: vec!["peer1".to_string(), "peer2".to_string(), "peer3".to_string()],
    });
    
    let requested_object = RequestedObject {
        object: test_data.clone(),
        source: provider,
    };
    
    let mut fetcher = Fetcher::new();
    fetcher.push(Box::new(requested_object));
    
    // run fetcher
    let handle = tokio::spawn(async move {
        fetcher.run(requests_tx, responses_rx).await.unwrap();
    });
    
    // verify first request is sent
    let request = requests_rx.recv().await.unwrap();
    match request {
        NetworkRequest::Sync { request, peer_id } => {
            assert!(["peer1", "peer2", "peer3"].contains(&peer_id.as_str()));
        }
        _ => panic!("Expected sync request"),
    }
    
    // send successful response from one peer
    let response = SyncResponse::Success(RequestPayload::Data(test_data.bytes()));
    let received = ReceivedObject {
        object: response,
        peer_id: "peer2".to_string(),
    };
    responses_tx.send(received).unwrap();
    
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    handle.abort();
} 