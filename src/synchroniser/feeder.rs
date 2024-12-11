use std::sync::Arc;
use libp2p::PeerId;
use serde::de::DeserializeOwned;
use tokio_util::sync::CancellationToken;

use anyhow::Context;
use proc_macros::Spawn;
use tokio::sync::{broadcast, mpsc};

use crate::{
    db::{Column, Db},
    types::{
        batch::Batch, block_header::BlockHeader, certificate::Certificate, network::{NetworkRequest, ReceivedObject, RequestPayload, SyncData, SyncRequest, SyncResponse}, traits::{AsHex, Hash}, transaction::Transaction, Digest, RequestId
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
            match self.req_rx.recv().await {
                Ok(request) => {
                    let request_id = request.object.digest();
                    match request.object {
                        SyncRequest::RequestCertificates(payload) => {
                            self.try_retrieve_data::<Certificate>(&payload, request_id, request.sender, Column::Certificates).await?
                        }
                        SyncRequest::RequestBlockHeaders(payload) => {
                            self.try_retrieve_data::<BlockHeader>(&payload, request_id, request.sender, Column::Headers).await?
                        }
                        SyncRequest::RequestBatches(payload) => {
                            self.try_retrieve_data::<Batch<Transaction>>(&payload, request_id, request.sender, Column::Batches).await?
                        }
                        SyncRequest::SyncDigest(_) => todo!(),
                    }
                },
                Err(e) => tracing::error!("Feeder: Failed to recv {}",e),
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
        Vec<T>: IntoSyncData {
        let mut datas = vec![];
        let certif_to_retrieve = payload.len();
        for digest in payload {
            match self
                .db
                .get::<T>(column, &digest.as_hex_string())
            {
                Ok(Some(batch)) => datas.push(batch),
                _ => {}
            };
        }

        let response = match datas.len() {
            len if len == certif_to_retrieve => {
                SyncResponse::Success(req_id.into(), datas.into_sync_data())
            }
            len if len < certif_to_retrieve => {
                SyncResponse::Partial(req_id.into(), datas.into_sync_data())
            }
            len if len == 0 => SyncResponse::Failure,
            _ => unreachable!(),
        };

        let response = NetworkRequest::SendTo(peer_id, RequestPayload::SyncResponse(response));
        self.network_tx
            .send(response)
            .await
            .context("Failed to send batches data over the channel")?;
        Ok(())
    }

}


#[cfg(test)]
pub mod test{

    #[rstest::rstest]
    #[tokio::test]
    async fn basic_test() {
        //feed db
        //mock a message in canal
        //verify
    }
}