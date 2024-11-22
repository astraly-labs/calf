pub mod agents;
pub mod signing;

use std::collections::HashMap;

use libp2p::PeerId;
use serde::{Deserialize, Serialize};
use signing::{Signable, SignedType};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Transaction {
    pub data: Vec<u8>,
}

impl Transaction {
    pub fn new(data: Vec<u8>) -> Self {
        Self { data }
    }
}

impl Transaction {
    pub fn as_bytes(&self) -> Vec<u8> {
        todo!()
    }
}
#[derive(Debug, PartialEq, Eq)]
pub enum NetworkRequest {
    Broadcast(RequestPayload),
    SendTo(PeerId, RequestPayload),
    SendToPrimary(RequestPayload),
}

pub type BatchAcknowledgement = Vec<u8>;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum RequestPayload {
    Batch(TxBatch),
    Acknoledgment(BatchAcknowledgement),
    Digest(Digest),
    Header(SignedBlockHeader),
    Vote(Vote),
}

pub struct ReceivedBatch {
    pub batch: TxBatch,
    pub sender: PeerId,
}

#[derive(Debug, Clone)]
pub struct ReceivedAcknowledgment {
    pub acknoledgement: BatchAcknowledgement,
    pub sender: PeerId,
}

pub type TxBatch = Vec<Transaction>;
pub type Digest = [u8; 32];
pub type PublicKey = Vec<u8>;
pub type WorkerId = u32;
pub type Stake = u64;
pub type Round = u64;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct BlockHeader {
    pub author: PeerId,
    pub round: Round,
    pub timestamp_ms: u128,
    pub digests: Vec<Digest>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct Certificate {
    signed_authorities: Vec<PeerId>,
    header: BlockHeader,
}

impl Certificate {
    pub fn new(signed_authorities: Vec<PeerId>, header: BlockHeader) -> Self {
        Self {
            signed_authorities,
            header,
        }
    }
}

impl Signable for BlockHeader {}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct Vote {
    peer_id: PeerId,
    header: BlockHeader,
}

impl Vote {
    pub fn new(peer_id: PeerId, header: BlockHeader) -> Self {
        Self { peer_id, header }
    }
    pub fn peer_id(&self) -> &PeerId {
        &self.peer_id
    }

    pub fn header(&self) -> &BlockHeader {
        &self.header
    }
}

pub type SignedBlockHeader = SignedType<BlockHeader>;

pub type Dag = HashMap<Round, (PeerId, Certificate)>;
