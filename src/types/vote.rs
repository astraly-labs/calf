use super::{
    block_header::BlockHeader, signing::Signature, traits::AsBytes, Digest, PublicKey, Sign,
};
use libp2p::identity::ed25519::{self, Keypair};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash)]
pub struct Vote {
    pub authority: PublicKey,
    pub signature: Signature,
}

// Vote: Signed Hash of the BlockHeader + PublicKey of the authority
impl Vote {
    pub fn from_header(header: BlockHeader, keypair: &Keypair) -> anyhow::Result<Self> {
        let signature = header.sign_with(&keypair)?;
        Ok(Self {
            authority: keypair.public().to_bytes(),
            signature,
        })
    }
    pub fn verify(&self, header_hash: &Digest) -> anyhow::Result<bool> {
        let pubkey = ed25519::PublicKey::try_from_bytes(&self.authority)?;
        Ok(pubkey.verify(header_hash, &self.signature))
    }
    pub fn as_bytes(&self) -> Vec<u8> {
        self.authority
            .iter()
            .chain(self.signature.iter())
            .copied()
            .collect()
    }
}

impl AsBytes for Vote {
    fn bytes(&self) -> Vec<u8> {
        self.authority
            .iter()
            .chain(self.signature.iter())
            .copied()
            .collect()
    }
}
