use derive_more::derive::Constructor;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

use super::{
    certificate::{Certificate, CertificateId},
    Round,
};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Dag(pub HashMap<String, Vertex>);

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Vertex {
    pub certificate: Certificate,
    pub parents: HashSet<String>,
}

impl Vertex {
    pub fn from_certificate(certificate: Certificate) -> Self {
        Self {
            parents: certificate
                .parents()
                .iter()
                .map(|parent| hex::encode(parent.id()))
                .collect(),
            certificate,
        }
    }
}

impl Dag {
    pub fn new(genesis: Certificate) -> anyhow::Result<Self> {
        let mut dag = Dag(HashMap::new());
        dag.insert_certificate(genesis)?;
        Ok(dag)
    }
    pub fn insert_certificate(&mut self, certificate: Certificate) -> Result<(), DagError> {
        let certificate_id = hex::encode(certificate.id());
        let parents_ids = certificate
            .parents()
            .iter()
            .map(|parent| hex::encode(parent.id()))
            .collect::<HashSet<String>>();
        let current_vertices_ids = self.0.keys().collect::<HashSet<&String>>();

        let missing_parents: HashSet<&String> = parents_ids
            .iter()
            .filter(|parent_id| !current_vertices_ids.contains(parent_id))
            .collect();
        if !missing_parents.is_empty() {
            return Err(DagError::MissingParents(
                missing_parents.into_iter().map(|elm| elm.clone()).collect(),
            ));
        }

        let vertex = Vertex {
            certificate,
            parents: parents_ids,
        };

        self.0
            .insert(certificate_id, vertex)
            .map(|_| Err(DagError::CertificateAlreadyExists))
            .unwrap_or(Ok(()))
    }
    /// Returns references to all certificates that satisfy a given predicate.
    pub fn get_all(&self, predicate: impl Fn(&Certificate) -> bool) -> HashSet<&Certificate> {
        self.0
            .values()
            .filter(|vertex| predicate(&vertex.certificate))
            .map(|vertex| &vertex.certificate)
            .collect()
    }

    pub fn count_all(&self, predicate: impl Fn(&Certificate) -> bool) -> usize {
        self.0
            .values()
            .filter(|vertex| predicate(&vertex.certificate))
            .count()
    }
    pub fn children_number(&self, certificate: &Certificate) -> usize {
        self.0
            .values()
            .filter(|vertex| vertex.parents.contains(&hex::encode(certificate.id())))
            .count()
    }
    pub fn round_certificates_number(&self, round: Round) -> usize {
        self.count_all(|certificate| match certificate {
            Certificate::Derived(derived) => derived.round == round,
            Certificate::Genesis(_) => round == 0,
            _ => unreachable!(),
        })
    }
    pub fn round_certificates(&self, round: Round) -> HashSet<Certificate> {
        self.get_all(|certificate| match certificate {
            Certificate::Derived(derived) => derived.round == round,
            Certificate::Genesis(_) => round == 0,
            _ => unreachable!(),
        })
        .into_iter()
        .cloned()
        .collect()
    }
    pub fn simplified(&self) -> Self {
        let mut simplified = self.0.clone();
        for key in self.0.keys() {
            let mut vertex = self.0.get(key).unwrap().clone();
            vertex.certificate = Certificate::Dummy;
            simplified.insert(key.clone(), vertex.clone());
        }
        Self(simplified)
    }
}

#[derive(thiserror::Error, Debug)]
pub enum DagError {
    #[error("Certificate already exists")]
    CertificateAlreadyExists,
    #[error("Missing parents")]
    MissingParents(HashSet<String>),
    #[error("Invalid certificate")]
    InvalidCertificate,
}
