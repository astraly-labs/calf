use std::path::Path;

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct NetworkInfos {
    pub validators: Vec<ValidatorInfos>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ValidatorInfos {
    pub pubkey: String,
    pub workers: Vec<WorkerInfo>,
    pub primary: PrimaryInfo,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct WorkerInfo {
    pub id: u32,
    pub pubkey: String,
    pub address: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
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
    pub timeout: u64, // in milliseconds
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

// Remplacer les String par un type keypair serialisable (celui de la libp2p ne l'est pas)
