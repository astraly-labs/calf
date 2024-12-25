use std::collections::HashSet;

use super::{
    batch::Batch,
    block_header::BlockHeader,
    certificate::Certificate,
    signing::SignedType,
    traits::{AsBytes, Hash},
    transaction::Transaction,
    vote::Vote,
    Acknowledgment, Digest, RequestId, WorkerId,
};
use derive_more::derive::Constructor;
use libp2p::PeerId;
use serde::{Deserialize, Serialize};

/// Represents a network request with different modes of sensding.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub enum NetworkRequest {
    /// Broadcast a payload to all counterparts (same role (worker - worker or primary - primary)) peers.
    BroadcastCounterparts(RequestPayload),
    /// Broadcast a payload to all peers on the same node.
    BroadcastSameNode(RequestPayload),
    /// Broadcast a payload to a random subset of counterparts peers.
    LuckyBroadcast(RequestPayload),
    /// Send a payload to a specific peer.
    SendTo(PeerId, RequestPayload),
    /// Send a payload to the primary node.
    SendToPrimary(RequestPayload),
}

/// Represents the payloads that can be included in a network request.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash)]
pub enum RequestPayload {
    Batch(Batch<Transaction>),
    /// An acknowledgment message for a batch.
    Acknowledgment(Acknowledgment),
    /// A digest of a transactions batch.
    Digest(Digest, ObjectSource),
    /// A block header.
    Header(BlockHeader),
    /// A certificate validating a header.
    Certificate(Certificate),
    /// A vote for a header.
    Vote(Vote),
    SyncRequest(SyncRequest),
    SyncResponse(SyncResponse),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, Copy)]
pub enum ObjectSource {
    SameNode,
    Counterpart,
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
            RequestPayload::Digest(digest, source) => Box::new((digest, source)),
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
            RequestPayload::Digest(digest, _) => Ok(*digest),
            _ => Err(anyhow::anyhow!("Invalid payload type")),
        }
    }
}

///A specific request to ask some data from a peer, the peer will answer with a SyncResponse (exept for SyncDigests)
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash)]
pub enum SyncRequest {
    Certificates(Vec<Digest>),
    BlockHeaders(Vec<Digest>),
    Batches(Vec<Digest>),
    /// Ask a worker to get the batch corresponding to a digest contained in a header. When the worker get the batches, it send the digests to his primary.
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

impl AsBytes for SyncRequest {
    fn bytes(&self) -> Vec<u8> {
        match bincode::serialize(self) {
            Ok(bytes) => bytes,
            Err(e) => {
                tracing::error!("Error serializing SyncRequest: {:?}", e);
                vec![]
            }
        }
    }
}

///A response to a SyncRequest, the requestId is the hash of the SyncRequest.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash)]
pub enum SyncResponse {
    ///All the asked data found
    Success(RequestId, SyncData),
    ///Only a subset of the requested data found
    Partial(RequestId, SyncData),
    ///None of the requested data found
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

///The data corresponding to the ids in the SyncRequest
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash)]
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

/// An object received by a peer.
#[derive(Clone, Debug, Constructor)]
pub struct ReceivedObject<T> {
    /// The object that was received.
    pub object: T,
    /// The id of the peer that sent the object.
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
