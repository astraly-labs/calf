use serde::{Serialize, Deserialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct NetworkConfig {
        pub validators: Vec<ValidatorConfig>
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ValidatorConfig {
    pub pubkey: String,
    pub workers_addresses: Vec<String>,
    pub primary_address: String,
}

impl NetworkConfig {
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
