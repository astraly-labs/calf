use std::collections::HashSet;

use derive_more::derive::Constructor;
use serde::{Deserialize, Serialize};

use super::{block_header::BlockHeader, Digest, Hash, PublicKey, Round, Vote};

pub type Seed = Digest;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash)]
pub enum Certificate {
    Dummy,
    Genesis(Seed),
    Derived(DerivedCertificate),
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Constructor, Hash)]
pub struct DerivedCertificate {
    pub round: Round,
    pub author: PublicKey,
    pub votes: Vec<Vote>,
    pub header_hash: Digest,
    pub parents: Vec<CertificateId>,
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
            Certificate::Dummy => [0; 32],
        }
    }
    pub fn set_round(&mut self, round: Round) {
        match self {
            Certificate::Genesis(_) => {}
            Certificate::Derived(derived) => derived.round = round,
            Certificate::Dummy => {}
        }
    }
    pub fn round(&self) -> Round {
        match self {
            Certificate::Genesis(_) => 0,
            Certificate::Derived(derived) => derived.round,
            Certificate::Dummy => 0,
        }
    }
    pub fn parents(&self) -> HashSet<&CertificateId> {
        match self {
            Certificate::Genesis(_) => HashSet::new(),
            Certificate::Derived(derived) => {
                derived.parents.iter().collect::<HashSet<&CertificateId>>()
            }
            Certificate::Dummy => HashSet::new(),
        }
    }
    pub fn derived(
        round: Round,
        author: PublicKey,
        votes: Vec<Vote>,
        header: &BlockHeader,
    ) -> Result<Self, anyhow::Error> {
        let header_hash = header.digest()?;
        let parents = header.certificates.iter().map(|c| c.id()).collect();
        Ok(Certificate::Derived(DerivedCertificate::new(
            round,
            author,
            votes,
            header_hash,
            parents,
        )))
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
