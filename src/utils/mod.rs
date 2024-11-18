use anyhow::Context as _;
use libp2p::identity::Keypair;
use std::{any, fs, path::Path};

pub fn read_keypair_from_file<P: AsRef<Path>>(path: P) -> anyhow::Result<Keypair> {
    // Read the hex-encoded key file
    let hex_key = fs::read_to_string(path).context("Failed to read key file")?;

    // Decode the hex string into bytes
    let key_bytes = hex::decode(hex_key.trim()).context("Failed to decode hex key")?;

    // Create libp2p keypair from the bytes
    Keypair::ed25519_from_bytes(key_bytes).context("Failed to create keypair from bytes")
}

#[derive(serde::Serialize, serde::Deserialize)]
struct KeyPairExport {
    pub public: Vec<u8>,
    pub secret: [u8; 32],
}

pub fn generate_keypair_and_write_to_file<P: AsRef<Path>>(path: P) -> anyhow::Result<()> {
    // Generate a new keypair
    let keypair = Keypair::generate_ed25519();

    let export = KeyPairExport {
        public: keypair.public().encode_protobuf(),
        secret: keypair.derive_secret(b"calf").unwrap(), // Safe unwrap as its not RSA
    };

    // Serialize the keypair to a file
    let serialized = serde_json::to_string_pretty(&export)?;
    fs::write(path, serialized)?;

    Ok(())
}
