use std::collections::HashSet;

use super::{
    batch::Batch, block_header::BlockHeader, certificate::Certificate, signing::SignedType,
    traits::Hash, transaction::Transaction, vote::Vote, Acknowledgment, Digest, RequestId,
    WorkerId,
};
use derive_more::derive::Constructor;
use libp2p::PeerId;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Eq)]
pub enum NetworkRequest {
    Broadcast(RequestPayload),
    LuckyBroadcast(RequestPayload),
    SendTo(PeerId, RequestPayload),
    SendToPrimary(RequestPayload),
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum RequestPayload {
    Batch(Batch<Transaction>),
    Acknowledgment(Acknowledgment),
    Digest(Digest),
    Header(BlockHeader),
    Certificate(Certificate),
    Vote(Vote),
    SyncRequest(SyncRequest),
    SyncResponse(SyncResponse),
}

impl RequestPayload {
    pub fn id(&self) -> anyhow::Result<RequestId> {
        let ser = bincode::serialize(&self)?;
        Ok(ser.digest())
    }

    pub fn inner(self) -> Box<dyn std::any::Any + 'static> {
        match self {
            RequestPayload::Header(header) => Box::new(header),
            RequestPayload::Certificate(cert) => Box::new(cert),
            RequestPayload::Batch(batch) => Box::new(batch),
            RequestPayload::Vote(vote) => Box::new(vote),
            RequestPayload::Acknowledgment(ack) => Box::new(ack),
            RequestPayload::Digest(digest) => Box::new(digest),
            RequestPayload::SyncRequest(sync_req) => Box::new(sync_req),
            RequestPayload::SyncResponse(sync_resp) => Box::new(sync_resp),
        }
    }
    pub fn inner_id(&self) -> anyhow::Result<Digest> {
        match self {
            RequestPayload::Header(header) => Ok(header.id()),
            RequestPayload::Certificate(cert) => Ok(cert.digest()),
            RequestPayload::Batch(batch) => Ok(batch.digest()),
            RequestPayload::Vote(vote) => Ok(vote.digest()),
            RequestPayload::Acknowledgment(ack) => Ok(ack.digest()),
            RequestPayload::Digest(digest) => Ok(*digest),
            _ => Err(anyhow::anyhow!("Invalid payload type")),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum SyncRequest {
    Certificates(Vec<Digest>), // shortcuts for the feeder
    BlockHeaders(Vec<Digest>),
    Batches(Vec<Digest>),
    // Ask a worker to get the batch corresponding to a digest contained in a header
    SyncDigests(Vec<Digest>),
}

impl SyncRequest {
    pub fn keys(&self) -> Vec<Digest> {
        match self {
            SyncRequest::Certificates(keys) => keys.clone(),
            SyncRequest::BlockHeaders(keys) => keys.clone(),
            SyncRequest::Batches(keys) => keys.clone(),
            SyncRequest::SyncDigests(keys) => keys.clone(),
        }
    }
    pub fn remove_reached(&mut self, reached: HashSet<Digest>) {
        match self {
            SyncRequest::Certificates(keys) => {
                *keys = keys
                    .iter()
                    .cloned()
                    .filter(|key| !reached.contains(key))
                    .collect()
            }
            SyncRequest::BlockHeaders(keys) => {
                *keys = keys
                    .iter()
                    .cloned()
                    .filter(|key| !reached.contains(key))
                    .collect()
            }
            SyncRequest::Batches(keys) => {
                *keys = keys
                    .iter()
                    .cloned()
                    .filter(|key| !reached.contains(key))
                    .collect()
            }
            SyncRequest::SyncDigests(keys) => {
                *keys = keys
                    .iter()
                    .cloned()
                    .filter(|key| !reached.contains(key))
                    .collect()
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum SyncResponse {
    Success(RequestId, SyncData),
    Partial(RequestId, SyncData),
    Failure(RequestId),
}

impl SyncResponse {
    pub fn id(&self) -> RequestId {
        match self {
            SyncResponse::Success(id, _) => *id,
            SyncResponse::Partial(id, _) => *id,
            SyncResponse::Failure(id) => *id,
        }
    }
    pub fn is_success(&self) -> bool {
        matches!(self, SyncResponse::Success(_, _))
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum SyncData {
    Certificates(Vec<Certificate>),
    Headers(Vec<BlockHeader>),
    Batches(Vec<Batch<Transaction>>),
}

impl SyncData {
    pub fn into_payloads(&self) -> Vec<RequestPayload> {
        match self {
            SyncData::Certificates(certs) => certs
                .iter()
                .map(|cert| RequestPayload::Certificate(cert.clone()))
                .collect(),
            SyncData::Headers(headers) => headers
                .iter()
                .map(|header| RequestPayload::Header(header.clone()))
                .collect(),
            SyncData::Batches(batches) => batches
                .iter()
                .map(|batch| RequestPayload::Batch(batch.clone()))
                .collect(),
        }
    }
}

#[derive(Clone, Debug, Constructor)]
pub struct ReceivedObject<T> {
    pub object: T,
    pub sender: PeerId,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum IdentifyInfo {
    Worker(WorkerId),
    Primary(PrimaryInfo),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct WorkerInfo {
    pub id: WorkerId,
    pub signature: SignedType<PeerId>,
    pub authority_pubkey: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PrimaryInfo {
    pub signature: SignedType<PeerId>,
    pub authority_pubkey: String,
}
