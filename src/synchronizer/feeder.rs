use libp2p::PeerId;
use serde::de::DeserializeOwned;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

use anyhow::Context;
use proc_macros::Spawn;
use tokio::sync::{broadcast, mpsc};

use crate::{
    db::{Column, Db},
    types::{
        batch::Batch,
        block_header::BlockHeader,
        certificate::Certificate,
        network::{
            NetworkRequest, ReceivedObject, RequestPayload, SyncData, SyncRequest, SyncResponse,
        },
        traits::{AsHex, Hash},
        transaction::Transaction,
        RequestId,
    },
};

pub trait IntoSyncData {
    fn into_sync_data(self) -> SyncData;
}

impl IntoSyncData for Vec<Certificate> {
    fn into_sync_data(self) -> SyncData {
        SyncData::Certificates(self)
    }
}

impl IntoSyncData for Vec<BlockHeader> {
    fn into_sync_data(self) -> SyncData {
        SyncData::Headers(self)
    }
}

impl IntoSyncData for Vec<Batch<Transaction>> {
    fn into_sync_data(self) -> SyncData {
        SyncData::Batches(self)
    }
}

#[derive(Spawn)]
pub(crate) struct Feeder {
    req_rx: broadcast::Receiver<ReceivedObject<SyncRequest>>,
    network_tx: mpsc::Sender<NetworkRequest>,
    db: Arc<Db>,
}

impl Feeder {
    pub async fn run(mut self) -> anyhow::Result<()> {
        loop {
            let req = self.req_rx.recv().await;
            tracing::info!("Feeder: Received a request");
            match req {
                Ok(request) => {
                    let request_id = request.object.digest();
                    match request.object {
                        SyncRequest::Certificates(payload) => {
                            self.try_retrieve_data::<Certificate>(
                                &payload,
                                request_id,
                                request.sender,
                                Column::Certificates,
                            )
                            .await?
                        }
                        SyncRequest::BlockHeaders(payload) => {
                            self.try_retrieve_data::<BlockHeader>(
                                &payload,
                                request_id,
                                request.sender,
                                Column::Headers,
                            )
                            .await?
                        }
                        SyncRequest::Batches(payload) => {
                            self.try_retrieve_data::<Batch<Transaction>>(
                                &payload,
                                request_id,
                                request.sender,
                                Column::Batches,
                            )
                            .await?
                        }
                        _ => {}
                    }
                }
                Err(e) => tracing::error!("Feeder: Failed to recv {}", e),
            }
        }
    }

    pub async fn try_retrieve_data<T>(
        &self,
        payload: &Vec<[u8; 32]>,
        req_id: RequestId,
        peer_id: PeerId,
        column: Column,
    ) -> anyhow::Result<()>
    where
        T: DeserializeOwned,
        Vec<T>: IntoSyncData,
    {
        let mut datas = vec![];
        let certif_to_retrieve = payload.len();
        for digest in payload {
            match self.db.get::<T>(column, &digest.as_hex_string()) {
                Ok(Some(batch)) => datas.push(batch),
                _ => {}
            };
        }

        let response = match datas.len() {
            len if len == certif_to_retrieve => {
                SyncResponse::Success(req_id.into(), datas.into_sync_data())
            }
            len if len != 0 && len < certif_to_retrieve => {
                SyncResponse::Partial(req_id.into(), datas.into_sync_data())
            }
            len if len == 0 => SyncResponse::Failure(req_id.into()),
            _ => unreachable!(),
        };

        let response = NetworkRequest::SendTo(peer_id, RequestPayload::SyncResponse(response));
        self.network_tx
            .send(response.clone())
            .await
            .context("Failed to send batches data over the channel")?;
        tracing::info!("Feeder: Sent response to {}", peer_id);
        Ok(())
    }
}

#[cfg(test)]
pub mod test {
    use crate::types::{
        network::{RequestPayload, SyncData, SyncResponse},
        traits::{AsHex, Hash},
        Digest,
    };
    use libp2p::PeerId;
    use std::sync::Arc;
    use tokio::sync::{broadcast, mpsc};
    use tokio_util::sync::CancellationToken;

    use crate::{
        db::Db,
        synchronizer::feeder::Feeder,
        types::{
            batch::Batch,
            network::{NetworkRequest, ReceivedObject, SyncRequest},
            traits::Random,
            transaction::Transaction,
        },
    };

    #[rstest::rstest]
    #[tokio::test]
    async fn feeder_full_recovery() {
        //create db
        tracing::info!("Starting test");
        let db = Arc::new(Db::new("/tmp/feeder_test_db_1".into()).expect("failed to open db"));
        //feed db
        let batches: Vec<Batch<Transaction>> =
            (0..10).map(|_| Batch::<Transaction>::random(30)).collect();
        let batch_ids: Vec<Digest> = batches
            .iter()
            .map(|elm: &Batch<Transaction>| elm.digest().into())
            .collect();

        // Insert data in database
        db.insert(
            crate::db::Column::Batches,
            &batch_ids[0].as_hex_string(),
            batches[0].clone(),
        )
        .unwrap();
        let result = db
            .get::<Batch<Transaction>>(crate::db::Column::Batches, &batch_ids[0].as_hex_string())
            .unwrap()
            .unwrap();
        assert_eq!(result, batches[0]);

        //run the feeder
        let cancellation_token = CancellationToken::new();
        let (sync_req_tx, sync_req_rx) = broadcast::channel::<ReceivedObject<SyncRequest>>(10_000);
        let (network_tx, mut network_rx) = mpsc::channel::<NetworkRequest>(10_000);
        let feeder_handle = Feeder::spawn(
            cancellation_token.clone(),
            sync_req_rx,
            network_tx.clone(),
            db.clone(),
        );

        //mock a message in canal
        let peer_id = PeerId::random();
        let sync_request = SyncRequest::Batches(vec![batch_ids[0]]);
        let received_object = ReceivedObject {
            object: sync_request,
            sender: peer_id,
        };
        let _ = sync_req_tx.send(received_object);

        if let Some(msg) = network_rx.recv().await {
            if let NetworkRequest::SendTo(pid, request_payload) = msg {
                assert_eq!(peer_id, pid);
                if let RequestPayload::SyncResponse(SyncResponse::Success(_reqid, syncdata)) =
                    request_payload
                {
                    if let SyncData::Batches(batch_resp) = syncdata {
                        assert_eq!(batch_resp[0], batches[0]);
                    } else {
                        panic!();
                    }
                } else {
                    panic!();
                }
            } else {
                panic!();
            }
        }
        cancellation_token.cancel();
        let _res = tokio::try_join!(feeder_handle);
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn feeder_partial_recovery() {
        //create db
        tracing::info!("Starting test");
        let db = Arc::new(Db::new("/tmp/feeder_test_db_2".into()).expect("failed to open db"));
        //feed db
        let batches: Vec<Batch<Transaction>> =
            (0..10).map(|_| Batch::<Transaction>::random(30)).collect();
        let batch_ids: Vec<Digest> = batches
            .iter()
            .map(|elm: &Batch<Transaction>| elm.digest().into())
            .collect();

        // Insert data in database
        db.insert(
            crate::db::Column::Batches,
            &batch_ids[0].as_hex_string(),
            batches[0].clone(),
        )
        .unwrap();
        let result = db
            .get::<Batch<Transaction>>(crate::db::Column::Batches, &batch_ids[0].as_hex_string())
            .unwrap()
            .unwrap();
        assert_eq!(result, batches[0]);

        //run the feeder
        let cancellation_token = CancellationToken::new();
        let (sync_req_tx, sync_req_rx) = broadcast::channel::<ReceivedObject<SyncRequest>>(10_000);
        let (network_tx, mut network_rx) = mpsc::channel::<NetworkRequest>(10_000);
        let feeder_handle = Feeder::spawn(
            cancellation_token.clone(),
            sync_req_rx,
            network_tx.clone(),
            db.clone(),
        );

        //mock a message in canal
        let peer_id = PeerId::random();
        let sync_request = SyncRequest::Batches(batch_ids);
        let received_object = ReceivedObject {
            object: sync_request,
            sender: peer_id,
        };
        let _ = sync_req_tx.send(received_object);

        if let Some(msg) = network_rx.recv().await {
            if let NetworkRequest::SendTo(pid, request_payload) = msg {
                assert_eq!(peer_id, pid);
                if let RequestPayload::SyncResponse(SyncResponse::Partial(_reqid, syncdata)) =
                    request_payload
                {
                    if let SyncData::Batches(batch_resp) = syncdata {
                        assert_eq!(batch_resp[0], batches[0]);
                    } else {
                        panic!();
                    }
                } else {
                    panic!();
                }
            } else {
                panic!();
            }
        }
        cancellation_token.cancel();
        let _res = tokio::try_join!(feeder_handle);
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn feeder_failed_recovery() {
        //create db
        tracing::info!("Starting test");
        let db = Arc::new(Db::new("/tmp/feeder_test_db_3".into()).expect("failed to open db"));
        //feed db
        let batches: Vec<Batch<Transaction>> =
            (0..10).map(|_| Batch::<Transaction>::random(30)).collect();
        let batch_ids: Vec<Digest> = batches
            .iter()
            .map(|elm: &Batch<Transaction>| elm.digest().into())
            .collect();

        //run the feeder
        let cancellation_token = CancellationToken::new();
        let (sync_req_tx, sync_req_rx) = broadcast::channel::<ReceivedObject<SyncRequest>>(10_000);
        let (network_tx, mut network_rx) = mpsc::channel::<NetworkRequest>(10_000);
        let feeder_handle = Feeder::spawn(
            cancellation_token.clone(),
            sync_req_rx,
            network_tx.clone(),
            db.clone(),
        );

        //mock a message in canal
        let peer_id = PeerId::random();
        let sync_request = SyncRequest::Batches(vec![batch_ids[0]]);
        let received_object = ReceivedObject {
            object: sync_request.clone(),
            sender: peer_id,
        };
        let _ = sync_req_tx.send(received_object);

        if let Some(msg) = network_rx.recv().await {
            if let NetworkRequest::SendTo(pid, request_payload) = msg {
                assert_eq!(peer_id, pid);
                if let RequestPayload::SyncResponse(sync_response) = request_payload {
                    assert_eq!(
                        sync_response,
                        SyncResponse::Failure(sync_request.digest().into())
                    );
                } else {
                    panic!();
                }
            } else {
                panic!();
            }
        }
        cancellation_token.cancel();
        let _res = tokio::try_join!(feeder_handle);
    }
}
