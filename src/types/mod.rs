pub mod services;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transaction {
    data: Vec<u8>,
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
