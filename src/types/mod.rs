pub mod agents;

use libp2p::PeerId;
use serde::{Deserialize, Serialize};

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

pub enum NetworkRequest {
    Broadcast(RequestPayload),
    SendTo(PeerId, RequestPayload),
}

pub type BatchAcknowledgement = Vec<u8>;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum RequestPayload {
    Batch(TxBatch),
    Acknoledgment(BatchAcknowledgement),
    Digest(Digest),
    Header(BlockHeader),
    Vote(BlockHeader),
}

pub struct ReceivedBatch {
    pub batch: TxBatch,
    pub sender: PeerId,
}

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

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct BlockHeader {
    pub author: PublicKey,
    pub round: Round,
    pub parents_hashes: Vec<Digest>,
    pub timestamp_ms: u128,
    pub digests: Vec<Digest>,
}
