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

pub type TxBatch = Vec<Transaction>;

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

pub enum NetworkRequest {
    Broadcast(Vec<u8>),
    SendTo(PeerId, Vec<u8>),
}
