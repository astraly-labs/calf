use libp2p::{
    identity::{self, ed25519},
    PeerId,
};
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

impl Committee {
    pub fn quorum_threshold(&self) -> u32 {
        if self.authorities.len() == 2 {
            return 2;
        }
        ((self.authorities.len() / 3) * 2 + 1) as u32
    }

    pub fn has_authority_id(&self, peer_id: &PeerId) -> bool {
        self.authorities.iter().any(|a| &a.authority_id == peer_id)
    }

    #[cfg(test)]
    pub fn new_test() -> Self {
        let mut authorities = Vec::new();

        // Add three test authorities
        for i in 0..3 {
            let keypair = ed25519::Keypair::generate();
            let public_key = identity::PublicKey::from(keypair.public());
            let peer_id = PeerId::from_public_key(&public_key);

            let authority = AuthorityInfo {
                authority_id: peer_id,
                authority_pubkey: hex::encode(keypair.public().to_bytes()),
                primary_address: ("127.0.0.1".to_string(), format!("800{}", i)),
                stake: 1,
                workers_addresses: vec![("127.0.0.1".to_string(), format!("900{}", i))],
            };
            authorities.push(authority);
        }

        Committee { authorities }
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
