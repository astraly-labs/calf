use crate::settings::parser::Committee;

use super::{
    block_header::{BlockHeader, HeaderId},
    traits::{AsBytes, Hash},
    vote::Vote,
    Digest, PublicKey, Round,
};
use derive_more::derive::Constructor;
use proc_macros::Id;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

pub type Seed = Digest;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash, Copy, Id)]
pub struct CertificateId(pub Digest);

impl TryFrom<String> for CertificateId {
    type Error = anyhow::Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        let bytes = hex::decode(value)
            .map_err(|e| anyhow::anyhow!("Failed to decode hex string: {}", e))?;
        if bytes.len() != 32 {
            return Err(anyhow::anyhow!("Expected 32 bytes, got {}", bytes.len()));
        }
        let array: [u8; 32] = bytes
            .try_into()
            .map_err(|_| anyhow::anyhow!("Failed to convert bytes into [u8; 32]"))?;
        Ok(CertificateId::from(array))
    }
}

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
            Certificate::Genesis(seed) => seed.digest().into(),
            Certificate::Derived(_) => self.digest().into(),
            Certificate::Dummy => [0; 32].into(),
        }
    }
    pub fn id_as_hex(&self) -> String {
        hex::encode(self.id().0)
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
    pub fn parents_as_hex(&self) -> HashSet<String> {
        match self {
            Certificate::Genesis(_) => HashSet::new(),
            Certificate::Derived(derived) => {
                derived.parents.iter().map(|p| hex::encode(p.0)).collect()
            }
            Certificate::Dummy => HashSet::new(),
        }
    }
    pub fn header(&self) -> Option<HeaderId> {
        match self {
            Certificate::Derived(cert) => Some(cert.header_hash.into()),
            _ => None,
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
    pub fn verify_votes(&self, committee: &Committee) -> Result<(), CertificateError> {
        match self {
            Certificate::Genesis(_) => Ok(()),
            Certificate::Dummy => Ok(()),
            Certificate::Derived(cert) => {
                if cert.votes.len() < committee.quorum_threshold() as usize {
                    Err(CertificateError::NotEnoughVotes)
                } else {
                    let header_hash = cert.header_hash;
                    if cert
                        .votes
                        .iter()
                        .all(|elm| elm.verify(&header_hash).unwrap_or(false))
                    {
                        Ok(())
                    } else {
                        Err(CertificateError::InvalidVote)
                    }
                }
            }
        }
    }
    pub fn genesis(seed: Seed) -> Self {
        Certificate::Genesis(seed)
    }
    pub fn author(&self) -> Option<PublicKey> {
        match self {
            Certificate::Genesis(_) => None,
            Certificate::Derived(derived) => Some(derived.author),
            Certificate::Dummy => None,
        }
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
                    .chain(certificate.parents.iter().flat_map(|p| p.0.iter()))
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
    #[error("One of the votes could not be verified")]
    InvalidVote,
    #[error("unknown parents")]
    UnknownParents,
    #[error("not enough parents")]
    NotEnoughParents,
    #[error("not enough votes")]
    NotEnoughVotes,
    #[error("invalid header")]
    InvalidHeader,
}
