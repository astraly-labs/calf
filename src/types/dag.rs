use derive_more::derive::Constructor;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

use super::certificate::{Certificate, CertificateId};

#[derive(Constructor, Serialize, Deserialize, Debug)]
pub struct Dag(HashMap<CertificateId, Vertex>);

#[derive(Serialize, Deserialize, Debug)]
struct Vertex {
    certificate: Certificate,
    parents: HashSet<CertificateId>,
}

impl Dag {
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
