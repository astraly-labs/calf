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

pub struct CircularBuffer<T> {
    buffer: Vec<Option<T>>,
    size: usize,
    head: usize,
    tail: usize,
    count: usize,
}

impl<T: Clone> CircularBuffer<T> {
    pub fn new(size: usize) -> Self {
        Self {
            buffer: vec![None; size],
            size,
            head: 0,
            tail: 0,
            count: 0,
        }
    }

    pub fn push(&mut self, item: T) {
        self.buffer[self.tail] = Some(item);
        if self.count == self.size {
            self.head = (self.head + 1) % self.size;
        } else {
            self.count += 1;
        }
        self.tail = (self.tail + 1) % self.size;
    }

    pub fn drain(&mut self) -> Vec<T> {
        let mut result = Vec::with_capacity(self.count);
        while self.count > 0 {
            if let Some(item) = self.buffer[self.head].take() {
                result.push(item);
            }
            self.head = (self.head + 1) % self.size;
            self.count -= 1;
        }
        result
    }
}
