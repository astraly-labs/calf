use std::sync::Arc;
use tokio_util::sync::CancellationToken;

use anyhow::Context;
use proc_macros::Spawn;
use tokio::sync::broadcast;

use crate::{
    db::{Column, Db},
    types::{
        batch::Batch,
        block_header::BlockHeader,
        certificate::Certificate,
        network::{ReceivedObject, SyncData, SyncRequest, SyncResponse},
        traits::{Hash, AsHex},
        transaction::Transaction,
        RequestId,
    },
};

#[derive(Spawn)]
pub(crate) struct Feeder {
    req_rx: broadcast::Receiver<ReceivedObject<SyncRequest>>,
    data_tx: broadcast::Sender<SyncResponse>,
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
                            self.try_retrieve_certficate(&payload, request_id).await?
                        }
                        SyncRequest::RequestBlockHeaders(payload) => {
                            self.try_retrieve_block_header(&payload, request_id).await?
                        }
                        SyncRequest::RequestBatches(payload) => {
                            self.try_retrieve_batches(&payload, request_id).await?
                        }
                        SyncRequest::SyncDigest(_) => todo!(),
                    }
                },
                Err(e) => tracing::error!("Feeder: Failed to recv {}",e),
            }
        } 
    }

    pub async fn try_retrieve_certficate(
        &self,
        payload: &Vec<[u8; 32]>,
        req_id: RequestId,
    ) -> anyhow::Result<()> {
        let mut datas: Vec<Certificate> = vec![];
        let certif_to_retrieve = payload.len();
        for certificate_id in payload {
            match self
                .db
                .get::<Certificate>(Column::Certificates, &certificate_id.as_hex_string())
            {
                Ok(Some(certif)) => datas.push(certif),
                _ => {}
            };
        }
        let response = match datas.len() {
            len if len == certif_to_retrieve => {
                SyncResponse::Success(req_id.into(), SyncData::Certificates(datas))
            }
            len if len < certif_to_retrieve => {
                SyncResponse::Partial(req_id.into(), SyncData::Certificates(datas))
            }
            len if len == 0 => SyncResponse::Failure,
            _ => unreachable!(),
        };
        self.data_tx
            .send(response)
            .context("Failed to send certificate data over the channel")?;
        Ok(())
    }

    pub async fn try_retrieve_block_header(
        &self,
        payload: &Vec<[u8; 32]>,
        req_id: RequestId,
    ) -> anyhow::Result<()> {
        let mut datas: Vec<BlockHeader> = vec![];
        let certif_to_retrieve = payload.len();
        for header_id in payload {
            match self
                .db
                .get::<BlockHeader>(Column::Headers, &header_id.as_hex_string())
            {
                Ok(Some(header)) => datas.push(header),
                _ => {}
            };
        }
        let response = match datas.len() {
            len if len == certif_to_retrieve => {
                SyncResponse::Success(req_id.into(), SyncData::Headers(datas))
            }
            len if len < certif_to_retrieve => {
                SyncResponse::Partial(req_id.into(), SyncData::Headers(datas))
            }
            len if len == 0 => SyncResponse::Failure,
            _ => unreachable!(),
        };
        self.data_tx
            .send(response)
            .context("Failed to send headers data over the channel")?;
        Ok(())
    }

    pub async fn try_retrieve_batches(
        &self,
        payload: &Vec<[u8; 32]>,
        req_id: RequestId,
    ) -> anyhow::Result<()> {
        let mut datas: Vec<Batch<Transaction>> = vec![];
        let certif_to_retrieve = payload.len();
        for digest in payload {
            match self
                .db
                .get::<Batch<Transaction>>(Column::Batches, &digest.as_hex_string())
            {
                Ok(Some(batch)) => datas.push(batch),
                _ => {}
            };
        }
        let response = match datas.len() {
            len if len == certif_to_retrieve => {
                SyncResponse::Success(req_id.into(), SyncData::Batches(datas))
            }
            len if len < certif_to_retrieve => {
                SyncResponse::Partial(req_id.into(), SyncData::Batches(datas))
            }
            len if len == 0 => SyncResponse::Failure,
            _ => unreachable!(),
        };
        self.data_tx
            .send(response)
            .context("Failed to send batches data over the channel")?;
        Ok(())
    }
}
