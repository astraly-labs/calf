use std::collections::HashSet;

use derive_more::derive::Constructor;
use libp2p::PeerId;

use super::{batch::BatchId, block_header::BlockHeader, certificate::CertificateId};

///The id of a certificate received from a peer with the ids of its parents that we doesn't have yet in the DAG / storage and needs to be synchronized.
/// it contains only the id and not the certificate itself because certififictaes are inserted even if some parents are missing (and the sync status is set to incomplete)
#[derive(Debug, Constructor)]
pub struct OrphanCertificate {
    pub id: CertificateId,
    pub missing_parents: Vec<CertificateId>,
}

///A header with all the missing data referenced into it.
///the header (and not only its id) and the peer id from which it was received are stored because the header will be re sent to the elector to be processed.
#[derive(Debug, Constructor)]
pub struct IncompleteHeader {
    pub missing_certificates: HashSet<CertificateId>,
    pub missing_batches: HashSet<BatchId>,
    pub header: BlockHeader,
    pub sender: PeerId,
}

///Describe the synchronization state, Incomplete if any valid certificate is missing.
pub enum SyncStatus {
    Complete,
    Incomplete,
}
