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

pub struct BlockHeader {
    // parents_hash: Vec<Hash>,
}

pub struct Block {
    _header: BlockHeader,
}

#[derive(PartialEq, Eq, Debug)]
pub enum NetworkRequest {
    Broadcast(RequestPayload),
    SendTo(PeerId, RequestPayload),
}

pub type BatchAcknowledgement = Vec<u8>;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum RequestPayload {
    Batch(TxBatch),
    Acknoledgment(BatchAcknowledgement),
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
pub type Digest = Vec<u8>;
pub type PublicKey = String;
pub type WorkerId = u32;
pub type Stake = u64;
