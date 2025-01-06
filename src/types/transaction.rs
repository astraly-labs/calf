use rand::random;
use serde::{Deserialize, Serialize};

use super::traits::{AsBytes, Random};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Transaction {
    pub data: Vec<u8>,
}

impl Transaction {
    pub fn new(data: Vec<u8>) -> Self {
        Self { data }
    }
    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }
}

impl AsBytes for Transaction {
    fn bytes(&self) -> Vec<u8> {
        self.data.clone()
    }
}

impl Random for Transaction {
    fn random(size: usize) -> Self {
        let data = (0..size).into_iter().map(|_| random()).collect();
        Self::new(data)
    }
}
