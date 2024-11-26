use libp2p::PeerId;
use serde::{Deserialize, Serialize};
use std::path::Path;

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
    pub authorities: Vec<AuthorityInfo>,
}

// TODO: reparer
impl Committee {
    pub fn quorum_threshold(&self) -> u32 {
        ((self.authorities.len() / 3) * 2 + 1) as u32
    }
    pub fn has_authority_id(&self, peer_id: &PeerId) -> bool {
        self.authorities.iter().any(|a| &a.authority_id == peer_id)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct AuthorityInfo {
    //Primary peer id
    pub authority_id: PeerId,
    pub authority_pubkey: String,
    pub primary_address: (String, String),
    pub stake: Stake,
    pub workers_addresses: Vec<(String, String)>,
}

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
