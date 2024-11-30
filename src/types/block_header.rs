use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use super::{certificate::Certificate, signing::Signable, Digest, PublicKey, Round};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash)]
pub struct BlockHeader {
    pub author: PublicKey,
    pub round: Round,
    pub timestamp_ms: u128,
    pub digests: Vec<Digest>,
    pub certificates: Vec<Certificate>,
}

impl BlockHeader {
    pub fn new(
        author: PublicKey,
        digests: Vec<Digest>,
        certificates: Vec<Certificate>,
        round: Round,
    ) -> Self {
        Self {
            author,
            round,
            timestamp_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("critical error: time is broken")
                .as_millis(),
            digests,
            certificates,
        }
    }
    pub fn genesis() -> Self {
        Self::new(todo!("block header genesis author"), vec![], vec![], 0)
    }
}

impl Signable for BlockHeader {}
