use super::{
    block_header::BlockHeader,
    traits::{AsBytes, Hash},
    vote::Vote,
    Digest, PublicKey, Round,
};
use derive_more::derive::Constructor;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

pub type Seed = Digest;
pub type CertificateId = Digest;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash)]
pub enum Certificate {
    Dummy,
    Genesis(Seed),
    Derived(DerivedCertificate),
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Constructor, Hash)]
pub struct DerivedCertificate {
    pub author: PublicKey,
    pub round: Round,
    pub votes: Vec<Vote>,
    pub header_hash: Digest,
    pub parents: Vec<CertificateId>,
}

impl Certificate {
    pub fn id(&self) -> CertificateId {
        match self {
            Certificate::Genesis(seed) => *seed,
            Certificate::Derived(_) => self.digest(),
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
        let header_hash = header.digest();
        let parents = header.certificates_ids.clone();
        Ok(Certificate::Derived(DerivedCertificate::new(
            author,
            round,
            votes,
            header_hash,
            parents,
        )))
    }
    pub fn genesis(seed: Seed) -> Self {
        Certificate::Genesis(seed)
    }
}

impl AsBytes for Certificate {
    fn bytes(&self) -> Vec<u8> {
        match &self {
            Certificate::Derived(certificate) => {
                let votes: Vec<u8> = certificate
                    .votes
                    .iter()
                    .flat_map(|elm| {
                        elm.authority
                            .iter()
                            .chain(elm.signature.iter())
                            .copied()
                            .collect::<Vec<u8>>()
                    })
                    .collect();
                let data: Vec<u8> = certificate
                    .author
                    .iter()
                    .chain(certificate.round.to_le_bytes().iter())
                    .chain(votes.iter())
                    .chain(certificate.header_hash.iter())
                    .chain(certificate.parents.iter().flat_map(|p| p.iter()))
                    .copied()
                    .collect();
                data
            }
            Certificate::Genesis(seed) => seed.to_vec(),
            Certificate::Dummy => vec![0; 32],
        }
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
