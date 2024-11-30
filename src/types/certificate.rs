use std::collections::HashSet;

use derive_more::derive::Constructor;
use serde::{Deserialize, Serialize};

use super::{block_header::BlockHeader, Digest, PublicKey, Round, Vote};

pub type Seed = Digest;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash)]
pub enum Certificate {
    Genesis(Seed),
    Derived(DerivedCertificate),
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Constructor, Hash)]
pub struct DerivedCertificate {
    pub round: Round,
    pub author: PublicKey,
    pub votes: Vec<Vote>,
    pub header: BlockHeader,
}

pub type CertificateId = Digest;

impl Certificate {
    //ID = H(author, round)
    pub fn id(&self) -> CertificateId {
        match self {
            Certificate::Genesis(seed) => *seed,
            Certificate::Derived(derived) => {
                let mut data = derived.author.to_vec();
                data.extend_from_slice(&derived.round.to_le_bytes());
                *blake3::hash(&data).as_bytes()
            }
        }
    }
    pub fn parents(&self) -> HashSet<&Certificate> {
        match self {
            Certificate::Genesis(_) => HashSet::new(),
            Certificate::Derived(derived) => derived
                .header
                .certificates
                .iter()
                .collect::<HashSet<&Certificate>>(),
        }
    }
    pub fn derived(round: Round, author: PublicKey, votes: Vec<Vote>, header: BlockHeader) -> Self {
        Certificate::Derived(DerivedCertificate::new(round, author, votes, header))
    }
    pub fn genesis(seed: Seed) -> Self {
        Certificate::Genesis(seed)
    }
}

#[derive(thiserror::Error, Debug)]
pub enum CertificateError {
    #[error("Certificate is falsified")]
    Falsified,
    #[error("unknown parents")]
    UnknownParents,
    #[error("not enough parents")]
    NotEnoughParents,
    #[error("not enough votes")]
    NotEnoughVotes,
    #[error("invalid header")]
    InvalidHeader,
}
