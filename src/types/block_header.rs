use std::{
    collections::HashSet,
    time::{SystemTime, UNIX_EPOCH},
};

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
    //TODO: fully verify the header
    pub fn verify_parents(
        &self,
        potential_parents: HashSet<Certificate>,
        quorum_threshold: u32,
    ) -> bool {
        if self.round == 1 {
            match self.certificates.first() {
                Some(Certificate::Genesis(_)) => true,
                _ => false,
            }
        } else {
            //TODO: remove the cloned
            let parents = self
                .certificates
                .iter()
                .cloned()
                .collect::<HashSet<Certificate>>();
            parents.intersection(&potential_parents).count() >= quorum_threshold as usize
        }
    }
}

impl Signable for BlockHeader {}
