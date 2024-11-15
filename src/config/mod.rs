use libp2p::identity::PublicKey;
use serde::{Serialize, Deserialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct NetworkInfos {
        pub validators: Vec<ValidatorConfig>
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ValidatorInfos {
    pub pubkey: String,
    pub workers: Vec<WorkerInfo>,
    pub primary: (String, libp2p::identity::ed25519::PublicKey),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct WorkerInfo {
    pub id: u32,
    pub pubkey: PublicKey,
    pub address: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum InstanceConfig {
    Primary(PrimaryConfig),
    Worker(WorkerConfig),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct WorkerConfig {
    pub id: u32,
    pub keypair: libp2p::identity::Keypair,
    pub address: String,
    pub primary_address: String,
    pub timeout: u64, // in milliseconds
    pub batch_size: usize,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PrimaryConfig {
    pub keypair: libp2p::identity::Keypair,
    pub address: String,
}

impl NetworkInfos {
    pub fn load_from_file(path: String) -> Result<Self, anyhow::Error> {
        let file = std::fs::File::open(path)?;
        let reader = std::io::BufReader::new(file);
        let config = serde_json::from_reader(reader)?;
        Ok(config)
    }
    
    pub fn write_to_file(&self, path: String) -> Result<(), anyhow::Error> {
        let file = std::fs::File::create(path)?;
        let writer = std::io::BufWriter::new(file);
        serde_json::to_writer_pretty(writer, self)?;
        Ok(())
    }
    
}

impl InstanceConfig {
    pub fn load_from_file(path: String) -> Result<Self, anyhow::Error> {
        let file = std::fs::File::open(path)?;
        let reader = std::io::BufReader::new(file);
        let config = serde_json::from_reader(reader)?;
        Ok(config)
    }
    
    pub fn write_to_file(&self, path: String) -> Result<(), anyhow::Error> {
        let file = std::fs::File::create(path)?;
        let writer = std::io::BufWriter::new(file);
        serde_json::to_writer_pretty(writer, self)?;
        Ok(())
    }
    
}
