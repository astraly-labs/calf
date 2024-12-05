use super::{
    batch::Batch,
    block_header::BlockHeader,
    certificate::{Certificate, CertificateId},
    signing::SignedType,
    traits::AsBytes,
    transaction::Transaction,
    vote::Vote,
    Acknowledgment, Digest, HeaderId, RequestId, WorkerId,
};
use derive_more::derive::Constructor;
use libp2p::PeerId;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Eq, Hash)]
pub enum NetworkRequest {
    Broadcast(RequestPayload),
    SendTo(PeerId, RequestPayload),
    SendToPrimary(RequestPayload),
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash)]
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

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash)]
pub enum SyncRequest {
    RequestCertificates(Vec<CertificateId>),
    RequestBlockHeaders(Vec<HeaderId>),
    // Worker to Worker
    RequestBatches(Vec<Digest>),
    // Ask a worker to get the batch corresponding to a digest contained in a header
    SyncDigest(Digest),
}

impl AsBytes for SyncRequest {
    fn bytes(&self) -> Vec<u8> {
        // Serialize self using bincode
        bincode::serialize(self).expect("Failed to serialize SyncRequest")
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash)]
pub enum SyncResponse {
    Success(RequestId, SyncData),
    Partial(RequestId, SyncData),
    Failure,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash)]
pub enum SyncData {
    Certificates(Vec<Certificate>),
    Headers(Vec<BlockHeader>),
    Batches(Vec<Batch<Transaction>>),
}

#[derive(Clone, Debug, Constructor)]
pub struct ReceivedObject<T>
where
    T: Clone,
{
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
