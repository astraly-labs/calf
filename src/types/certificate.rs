use std::collections::HashSet;

use derive_more::derive::Constructor;
use serde::{Deserialize, Serialize};

use super::{block_header::BlockHeader, Digest, PublicKey, Round, Vote};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Constructor, Hash)]
pub struct Certificate {
    round: Round,
    author: PublicKey,
    votes: Vec<Vote>,
    header: BlockHeader,
}

pub type CertificateId = Digest;

impl Certificate {
    //ID = H(author, round)
    pub fn id(&self) -> CertificateId {
        let mut data = self.author.to_vec();
        data.extend_from_slice(&self.round.to_le_bytes());
        *blake3::hash(&data).as_bytes()
    }
    pub fn parents(&self) -> HashSet<&Certificate> {
        self.header
            .certificates
            .iter()
            .collect::<HashSet<&Certificate>>()
    }
}
