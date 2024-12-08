use std::{
    collections::HashSet,
    time::{SystemTime, UNIX_EPOCH},
};

use proc_macros::Id;
use serde::{Deserialize, Serialize};

use super::{
    batch::BatchId,
    certificate::CertificateId,
    signing::Signable,
    traits::{AsBytes, Hash},
    Digest, PublicKey, Round,
};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Id, Hash)]
pub struct HeaderId(pub Digest);

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash)]
pub struct BlockHeader {
    pub author: PublicKey,
    pub round: Round,
    pub timestamp_ms: u128,
    pub digests: Vec<BatchId>,
    pub certificates_ids: Vec<CertificateId>,
}

impl BlockHeader {
    pub fn new(
        author: PublicKey,
        digests: Vec<BatchId>,
        certificates_ids: Vec<CertificateId>,
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
            certificates_ids,
        }
    }
    pub fn verify_parents(
        &self,
        potential_parents: HashSet<CertificateId>,
        quorum_threshold: u32,
    ) -> Result<(), HeaderError> {
        //genesis round
        if self.round == 1 {
            if !(potential_parents.is_empty() || self.certificates_ids.is_empty()) {
                if self.certificates_ids[0]
                    == *potential_parents.iter().collect::<Vec<&CertificateId>>()[0]
                {
                    Ok(())
                } else {
                    Err(HeaderError::NotEnoughParents)
                }
            } else {
                Err(HeaderError::NotEnoughParents)
            }
        } else {
            let parents = self
                .certificates_ids
                .iter()
                .map(|id| *id)
                .collect::<HashSet<CertificateId>>();
            potential_parents
                .intersection(&parents)
                .count()
                .checked_sub(quorum_threshold as usize)
                .map_or(Err(HeaderError::NotEnoughParents), |_| Ok(()))
        }
    }
    pub fn id(&self) -> Digest {
        self.digest()
    }
}

impl AsBytes for BlockHeader {
    fn bytes(&self) -> Vec<u8> {
        self.author
            .iter()
            .chain(self.round.to_le_bytes().iter())
            .chain(self.timestamp_ms.to_le_bytes().iter())
            .chain(self.digests.iter().flat_map(|d| d.0.iter()))
            .chain(self.certificates_ids.iter().flat_map(|c| c.0.iter()))
            .copied()
            .collect()
    }
}

#[derive(thiserror::Error, Debug)]
pub enum HeaderError {
    #[error("Not enough parents")]
    NotEnoughParents,
    #[error("Invalid parents")]
    InvalidParents,
    #[error("Invalid header")]
    Invalid,
}

impl Signable for BlockHeader {}
