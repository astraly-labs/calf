use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

use super::{certificate::Certificate, Round};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Dag {
    certificates: HashMap<String, Vertex>,
    gc_round: Round,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Vertex {
    certificate: Certificate,
    parents: HashSet<String>,
}

impl Vertex {
    pub fn from_certificate(certificate: Certificate) -> Self {
        Self {
            parents: certificate.parents_as_hex(),
            certificate,
        }
    }
}

impl Dag {
    pub fn new(genesis: Certificate) -> anyhow::Result<Self> {
        let mut dag = Dag {
            certificates: HashMap::new(),
            gc_round: 0,
        };
        dag.insert_certificate(genesis)?;
        Ok(dag)
    }

    pub fn update_gc_round(&mut self, new_gc_round: Round) {
        if new_gc_round > self.gc_round {
            self.gc_round = new_gc_round;
            // Remove certificates from rounds before gc_round
            self.certificates
                .retain(|_, vertex| match &vertex.certificate {
                    Certificate::Derived(derived) => derived.round >= self.gc_round,
                    Certificate::Genesis(_) => true,
                    Certificate::Dummy => false,
                });
            tracing::info!("🗑️ Garbage collected rounds before {}", new_gc_round);
        }
    }

    pub fn insert_certificate(&mut self, certificate: Certificate) -> Result<(), DagError> {
        // Check if certificate is from a round that should be garbage collected
        match &certificate {
            Certificate::Derived(derived) if derived.round < self.gc_round => {
                return Err(DagError::InvalidCertificate);
            }
            _ => {}
        }

        let certificate_id = hex::encode(certificate.id());
        let parents_ids = certificate
            .parents()
            .iter()
            .map(|parent| hex::encode(parent))
            .collect::<HashSet<String>>();

        let current_vertices_ids = self.certificates.keys().collect::<HashSet<&String>>();

        let missing_parents: HashSet<&String> = parents_ids
            .iter()
            .filter(|parent_id| !current_vertices_ids.contains(parent_id))
            .collect();

        if !missing_parents.is_empty() {
            return Err(DagError::MissingParents(
                missing_parents.into_iter().map(|elm| elm.clone()).collect(),
            ));
        }

        let vertex = Vertex::from_certificate(certificate);
        self.certificates.insert(certificate_id, vertex);
        Ok(())
    }
    /// Returns references to all certificates that satisfy a given predicate.
    pub fn get_all(&self, predicate: impl Fn(&Certificate) -> bool) -> HashSet<&Certificate> {
        self.certificates
            .values()
            .filter(|vertex| predicate(&vertex.certificate))
            .map(|vertex| &vertex.certificate)
            .collect()
    }

    pub fn count_all(&self, predicate: impl Fn(&Certificate) -> bool) -> usize {
        self.certificates
            .values()
            .filter(|vertex| predicate(&vertex.certificate))
            .count()
    }
    pub fn children_number(&self, certificate: &Certificate) -> usize {
        self.certificates
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
        let mut simplified = self.certificates.clone();
        for key in self.certificates.keys() {
            let mut vertex = self.certificates.get(key).unwrap().clone();
            vertex.certificate = Certificate::Dummy;
            simplified.insert(key.clone(), vertex.clone());
        }
        Self {
            certificates: simplified,
            gc_round: self.gc_round,
        }
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
