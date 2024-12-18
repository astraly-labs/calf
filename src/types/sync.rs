use std::collections::HashSet;

use derive_more::derive::Constructor;
use libp2p::PeerId;

use super::{batch::BatchId, block_header::BlockHeader, certificate::CertificateId};

#[derive(Debug, Constructor)]
pub struct OrphanCertificate {
    pub id: CertificateId,
    pub missing_parents: Vec<CertificateId>,
}

#[derive(Debug, Constructor)]
pub struct IncompleteHeader {
    pub missing_certificates: HashSet<CertificateId>,
    pub missing_batches: HashSet<BatchId>,
    pub header: BlockHeader,
    pub sender: PeerId,
}
