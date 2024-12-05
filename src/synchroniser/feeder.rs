use std::sync::Arc;

use blake3::Hash;
use tokio::sync::mpsc;

use crate::{db::{Column, Db}, types::{batch::Batch, block_header::BlockHeader, certificate::Certificate, network::{SyncData, SyncRequest, SyncResponse}, traits::AsHex, transaction::Transaction, Digest}};

pub struct Feeder{
    pub req_rx: mpsc::Receiver<SyncRequest>,
    pub data_tx: mpsc::Sender<SyncResponse>,
    pub db: Arc<Db>,
}

impl Feeder {
    pub fn new(req_rx: mpsc::Receiver<SyncRequest>, data_tx: mpsc::Sender<SyncResponse>, db: Arc<Db>) -> Self {
        Self { 
            req_rx,
            data_tx,
            db,
        }
    }

    pub async fn run_forever(&mut self) -> anyhow::Result<()> {
        while let Some(request) = self.req_rx.recv().await {
            let request_id = blake3::hash(request);
            match request {
                SyncRequest::RequestCertificates(payload) => self.try_retrieve_certficate(&payload, request_id),
                SyncRequest::RequestBlockHeaders(payload) => todo!(),
                SyncRequest::RequestBatches(payload) => todo!(),
                SyncRequest::SyncDigest(_) => todo!(),
            }
        }
        Ok(())
    }

    pub fn try_retrieve_certficate(&self, payload: &Vec<[u8; 32]>, req_id: Hash) {
        let mut datas: Vec<Certificate>=  vec![];
        let certif_to_retrieve = payload.len();
        for certificate_id in payload {
            match self.db.get::<Certificate>(Column::Certificates, &certificate_id.as_hex_string()) {
                Ok(Some(certif)) => datas.push(certif),
                _ => {},
            };
        }
        let response = match datas.len() {
            len if len == certif_to_retrieve => SyncResponse::Success(req_id.into(), SyncData::Certificates(datas)),
            len if len< certif_to_retrieve => SyncResponse::Partial(req_id.into(), SyncData::Certificates(datas)),
            len if len == 0 => SyncResponse::Failure,
            _ => unreachable!(),
        };
        self.data_tx.send(response);
    }

    pub fn try_retrieve_block_header(&self, payload: &Vec<[u8; 32]>, req_id: Hash) {
        let mut datas: Vec<BlockHeader>=  vec![];
        let certif_to_retrieve = payload.len();
        for header_id in payload {
            match self.db.get::<BlockHeader>(Column::Headers, &header_id.as_hex_string()) {
                Ok(Some(header)) => datas.push(header),
                _ => {},
            };
        }
        let response = match datas.len() {
            len if len == certif_to_retrieve => SyncResponse::Success(req_id.into(), SyncData::Headers(datas)),
            len if len< certif_to_retrieve => SyncResponse::Partial(req_id.into(), SyncData::Headers(datas)),
            len if len == 0 => SyncResponse::Failure,
            _ => unreachable!(),
        };
        self.data_tx.send(response);
    }

    pub fn try_retrieve_batches(&self, payload: &Vec<[u8; 32]>, req_id: Hash) {
        let mut datas: Vec<Batch<Transaction>>=  vec![];
        let certif_to_retrieve = payload.len();
        for digest in payload {
            match self.db.get::<Batch<Transaction>>(Column::Batches, &digest.as_hex_string()) {
                Ok(Some(batch)) => datas.push(batch),
                _ => {},
            };
        }
        let response = match datas.len() {
            len if len == certif_to_retrieve => SyncResponse::Success(req_id.into(), SyncData::Batches(datas)),
            len if len< certif_to_retrieve => SyncResponse::Partial(req_id.into(), SyncData::Batches(datas)),
            len if len == 0 => SyncResponse::Failure,
            _ => unreachable!(),
        };
        self.data_tx.send(response);
    }
}