use libp2p::Multiaddr;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, path::Path};

use crate::types::{PublicKey, Stake, WorkerId};

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct NetworkInfos {
    pub validators: Vec<ValidatorInfos>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct ValidatorInfos {
    pub pubkey: String,
    pub workers: Vec<WorkerInfo>,
    pub primary: PrimaryInfo,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct WorkerInfo {
    pub id: WorkerId,
    pub pubkey: String,
    pub address: String,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct PrimaryInfo {
    pub id: u32,
    pub pubkey: String,
    pub address: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum InstanceConfig {
    Primary(PrimaryConfig),
    Worker(WorkerConfig),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct WorkerConfig {
    pub validator_pubkey: String,
    pub id: u32,
    pub keypair: String,
    pub pubkey: String,
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

impl NetworkInfos {
    pub fn load_from_file<P: AsRef<Path>>(path: P) -> Result<Self, anyhow::Error> {
        let file = std::fs::File::open(path)?;
        let reader = std::io::BufReader::new(file);
        let config = serde_json::from_reader(reader)?;
        Ok(config)
    }

    pub fn write_to_file<P: AsRef<Path>>(&self, path: P) -> Result<(), anyhow::Error> {
        let file = std::fs::File::create(path)?;
        let writer = std::io::BufWriter::new(file);
        serde_json::to_writer_pretty(writer, self)?;
        Ok(())
    }
}

impl InstanceConfig {
    pub fn load_from_file<P: AsRef<Path>>(path: P) -> Result<Self, anyhow::Error> {
        let file = std::fs::File::open(path)?;
        let reader = std::io::BufReader::new(file);
        let config = serde_json::from_reader(reader)?;
        Ok(config)
    }

    pub fn write_to_file<P: AsRef<Path>>(&self, path: P) -> Result<(), anyhow::Error> {
        let file = std::fs::File::create(path)?;
        let writer = std::io::BufWriter::new(file);
        serde_json::to_writer_pretty(writer, self)?;
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PrimaryAddresses {
    pub primary_address: Multiaddr,
    pub network_key: String,
    pub hostname: Multiaddr,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct WorkerAddresses {
    pub primary_to_worker: Multiaddr,
    pub worker_to_primary: Multiaddr,
    pub network_key: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Authority {
    pub protocol_key: String,
    pub protocol_key_bytes: String,
    pub stake: Stake,
    pub primary_address: String,
    pub network_key: String,
    pub hostname: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Committee {
    pub authorities: BTreeMap<PublicKey, Authority>,
    pub epoch: u64,
}

impl Committee {
    pub fn load_from_file<P: AsRef<Path>>(path: P) -> Result<Self, anyhow::Error> {
        let file = std::fs::File::open(path)?;
        let reader = std::io::BufReader::new(file);
        let config = serde_json::from_reader(reader)?;
        Ok(config)
    }

    pub fn write_to_file<P: AsRef<Path>>(&self, path: P) -> Result<(), anyhow::Error> {
        let file = std::fs::File::create(path)?;
        let writer = std::io::BufWriter::new(file);
        serde_json::to_writer_pretty(writer, self)?;
        Ok(())
    }
}
