use derive_more::derive::Constructor;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

use super::certificate::{Certificate, CertificateId};

#[derive(Serialize, Deserialize, Debug)]
pub struct Dag(HashMap<CertificateId, Vertex>);

#[derive(Serialize, Deserialize, Debug)]
struct Vertex {
    certificate: Certificate,
    parents: HashSet<CertificateId>,
}

impl Vertex {
    pub fn from_certificate(certificate: Certificate) -> Self {
        Self {
            parents: certificate
                .parents()
                .iter()
                .map(|parent| parent.id())
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
        let certificate_id = certificate.id();
        let parents_ids = certificate
            .parents()
            .iter()
            .map(|parent| parent.id())
            .collect::<HashSet<CertificateId>>();
        let current_vertices_ids = self.0.keys().collect::<HashSet<&CertificateId>>();

        let missing_parents: HashSet<&CertificateId> = parents_ids
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
            .filter(|vertex| vertex.parents.contains(&certificate.id()))
            .count()
    }
}

#[derive(thiserror::Error, Debug)]
pub enum DagError {
    #[error("Certificate already exists")]
    CertificateAlreadyExists,
    #[error("Missing parents: {0:?}")]
    MissingParents(HashSet<CertificateId>),
    #[error("Invalid certificate")]
    InvalidCertificate,
}
