use libp2p::PeerId;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap},
    path::Path,
};

use crate::types::{PublicKey, Stake, WorkerId};

// Helper trait for file operations
pub trait FileLoader: Sized {
    fn load_from_file<P: AsRef<Path>>(path: P) -> anyhow::Result<Self>;
    fn write_to_file<P: AsRef<Path>>(&self, path: P) -> anyhow::Result<()>;
}

// Implementation for any type that can be serialized/deserialized
impl<T: Serialize + for<'a> Deserialize<'a>> FileLoader for T {
    fn load_from_file<P: AsRef<Path>>(path: P) -> anyhow::Result<Self> {
        let file = std::fs::File::open(path)?;
        let reader = std::io::BufReader::new(file);
        Ok(serde_json::from_reader(reader)?)
    }

    fn write_to_file<P: AsRef<Path>>(&self, path: P) -> anyhow::Result<()> {
        let file = std::fs::File::create(path)?;
        let writer = std::io::BufWriter::new(file);
        Ok(serde_json::to_writer_pretty(writer, self)?)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Committee {
    authorities: BTreeMap<PeerId, AuthorityInfo>,
}

impl Committee {
    pub fn has_authority_key(&self, key: &PeerId) -> bool {
        self.authorities.contains_key(key)
    }

    pub fn quorum_threshold(&self) -> usize {
        self.authorities.keys().len() * 2 / 3 + 1
    }

    pub fn get_authority_info_by_key(&self, key: &PeerId) -> Option<&AuthorityInfo> {
        self.authorities.get(key)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct AuthorityInfo {
    pub primary: PrimaryAddresses,
    pub stake: Stake,
    pub workers: HashMap<WorkerId, WorkerAddresses>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PrimaryAddresses {
    pub primary_to_primary: String,
    pub worker_to_primary: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct WorkerAddresses {
    pub primary_to_worker: String,
    pub transactions: String,
    pub worker_to_worker: String,
}

// Instance specific configurations
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum InstanceConfig {
    Primary(PrimaryConfig),
    Worker(WorkerConfig),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct WorkerConfig {
    pub validator_pubkey: PublicKey,
    pub id: WorkerId,
    pub keypair: String,
    pub address: String,
    pub primary: PrimaryInfo,
    pub timeout: u64,        // in milliseconds
    pub quorum_timeout: u64, // in milliseconds
    pub batch_size: usize,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PrimaryConfig {
    pub keypair: String,
    pub address: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PrimaryInfo {
    pub address: String,
}

// Examples of usage:
impl Committee {
    pub fn get_primary_address(&self, authority_key: &PeerId) -> Option<&String> {
        self.authorities
            .get(authority_key)
            .map(|auth| &auth.primary.primary_to_primary)
    }

    pub fn get_worker_address(
        &self,
        authority_key: &PeerId,
        worker_id: WorkerId,
    ) -> Option<&WorkerAddresses> {
        self.authorities.get(authority_key)?.workers.get(&worker_id)
    }
}
