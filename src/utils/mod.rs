use anyhow::Context as _;
use libp2p::identity::Keypair;
use std::{fs, path::Path};

pub fn read_keypair_from_file<P: AsRef<Path>>(path: P) -> anyhow::Result<Keypair> {
    // Read the hex-encoded key file
    let hex_key = fs::read_to_string(path).context("Failed to read key file")?;

    // Decode the hex string into bytes
    let key_bytes = hex::decode(hex_key.trim()).context("Failed to decode hex key")?;

    // Create libp2p keypair from the bytes
    Keypair::ed25519_from_bytes(key_bytes).context("Failed to create keypair from bytes")
}
