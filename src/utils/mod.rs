use anyhow::Context as _;
use libp2p::{identity::Keypair, PeerId};
use std::{fs, path::Path};

#[derive(serde::Serialize, serde::Deserialize)]
struct KeyPairExport {
    pub public: String,
    pub secret: String,
    pub peer_id: PeerId,
}

pub fn read_keypair_from_file<P: AsRef<Path>>(path: P) -> anyhow::Result<Keypair> {
    // Read the hex-encoded key file
    let keypair = serde_json::from_str::<KeyPairExport>(
        &fs::read_to_string(path).context("Failed to read key file")?,
    )?;

    // Decode the hex string into bytes
    let pk_bytes = hex::decode(keypair.secret.trim()).context("Failed to decode secret hex key")?;

    // Create libp2p keypair from the bytes
    Keypair::ed25519_from_bytes(pk_bytes).context("Failed to create keypair from bytes")
}

pub fn generate_keypair_and_write_to_file<P: AsRef<Path>>(path: P) -> anyhow::Result<()> {
    // Generate a new keypair
    let keypair = Keypair::generate_ed25519();

    let export = KeyPairExport {
        public: hex::encode(keypair.public().encode_protobuf()),
        secret: hex::encode(keypair.derive_secret(b"calf").unwrap()), // Safe unwrap as its not RSA
        peer_id: keypair.public().to_peer_id(),
    };

    // Serialize the keypair to a file
    let serialized = serde_json::to_string_pretty(&export)?;
    fs::write(path, serialized)?;

    Ok(())
}
