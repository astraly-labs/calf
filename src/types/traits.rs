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
