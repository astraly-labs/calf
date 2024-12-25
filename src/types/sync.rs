use std::{alloc::System, collections::HashSet};

use derive_more::derive::Constructor;
use libp2p::PeerId;

use super::{batch::BatchId, block_header::BlockHeader, certificate::CertificateId};

///The id of a certificate received from a peer with the ids of its parents that we doesn't have yet in the DAG / storage and needs to be synchronized.
/// it contains only the id and not the certificate itself because certififictaes are inserted even if some parents are missing (and the sync status is set to incomplete)
#[derive(Debug, Constructor, Eq, PartialEq, Clone)]
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncStatus {
    Complete,
    Incomplete,
}

pub struct TrackedSet<T>
where
    T: std::hash::Hash + PartialEq + Eq + Clone,
{
    objects: HashSet<TimestampedObject<T>>,
    timeout: u64,
}

impl<T> TrackedSet<T>
where
    T: std::hash::Hash + PartialEq + Eq + Clone,
{
    pub fn new(timeout: u64) -> Self {
        Self {
            objects: HashSet::new(),
            timeout,
        }
    }
    pub fn insert(&mut self, object: T) -> bool {
        if self
            .objects
            .iter()
            .any(|timestamped_object| timestamped_object.object == object)
        {
            return false;
        }
        self.objects.insert(TimestampedObject::from(object))
    }
    pub fn remove(&mut self, object: &T) {
        self.objects
            .retain(|timestamped_object| timestamped_object.object != *object);
    }
    pub fn get_timed_out(&self) -> HashSet<T> {
        self.objects
            .iter()
            .filter(|timestamped_object| timestamped_object.timed_out(self.timeout))
            .map(|timestamped_object| timestamped_object.object.clone())
            .collect()
    }
    pub fn drain_timed_out(&mut self) -> HashSet<T> {
        let timed_out = self.get_timed_out();
        self.objects
            .retain(|timestamped_object| !timestamped_object.timed_out(self.timeout));
        timed_out
    }
    pub fn is_empty(&self) -> bool {
        self.objects.is_empty()
    }
    pub fn contains(&self, object: &T) -> bool {
        self.objects
            .iter()
            .any(|timestamped_object| timestamped_object.object == *object)
    }
    pub fn retain<F>(&mut self, f: F)
    where
        F: Fn(&T) -> bool,
    {
        self.objects
            .retain(|timestamped_object| f(&timestamped_object.object));
    }
    pub fn extend(&mut self, objects: impl IntoIterator<Item = T>) {
        self.objects
            .extend(objects.into_iter().map(TimestampedObject::from));
    }
    pub fn len(&self) -> usize {
        self.objects.len()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TimestampedObject<T>
where
    T: std::hash::Hash + PartialEq + Eq + Clone,
{
    pub object: T,
    pub timestamp: i64,
}

impl<T> TimestampedObject<T>
where
    T: std::hash::Hash + PartialEq + Eq + Clone,
{
    pub fn timed_out(&self, timeout: u64) -> bool {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("time is broken")
            .as_millis() as i64;
        (now - self.timestamp) as u64 > timeout
    }
}

impl<T> From<T> for TimestampedObject<T>
where
    T: std::hash::Hash + PartialEq + Eq + Clone,
{
    fn from(object: T) -> Self {
        Self {
            object,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("time is broken")
                .as_millis() as i64,
        }
    }
}
