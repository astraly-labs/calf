use libp2p::identity::ed25519::Keypair;

use super::{signing::Signature, Digest};

pub trait Hash {
    fn digest(&self) -> Digest;
}

pub trait Sign {
    fn sign_with(&self, keypair: &Keypair) -> anyhow::Result<Signature>;
}

pub trait AsBytes {
    fn bytes(&self) -> Vec<u8>;
}

pub trait Random {
    fn random(size: usize) -> Self;
}

pub trait AsHex {
    fn as_hex_string(&self) -> String;
}

pub trait ObjectId {
    fn id(&self) -> Digest;
}

impl<T: Hash> Sign for T {
    fn sign_with(&self, keypair: &Keypair) -> anyhow::Result<Signature> {
        Ok(keypair.sign(&self.digest()))
    }
}

impl<T> Hash for T
where
    T: AsBytes,
{
    fn digest(&self) -> Digest {
        blake3::hash(&self.bytes()).into()
    }
}

impl AsBytes for [u8; 32] {
    fn bytes(&self) -> Vec<u8> {
        self.to_vec()
    }
}

impl AsBytes for Vec<u8> {
    fn bytes(&self) -> Vec<u8> {
        self.clone()
    }
}

impl<T> AsHex for T
where
    T: AsBytes,
{
    fn as_hex_string(&self) -> String {
        hex::encode(self.bytes())
    }
}

impl AsBytes for i32 {
    fn bytes(&self) -> Vec<u8> {
        self.to_le_bytes().to_vec()
    }
}
