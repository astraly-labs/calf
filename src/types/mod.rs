pub mod agents;
pub mod batch;
pub mod block_header;
pub mod certificate;
pub mod dag;
pub mod network;
pub mod signing;
pub mod traits;
pub mod transaction;
pub mod vote;

use block_header::BlockHeader;
use serde::{Deserialize, Serialize};
use signing::SignedType;
use traits::{Hash, Sign};

pub type Digest = [u8; 32];
pub type PublicKey = [u8; 32];
pub type WorkerId = u32;
pub type Stake = u64;
pub type Round = u64;
pub type SignedBlockHeader = SignedType<BlockHeader>;
pub type HeaderId = Digest;
pub type RequestId = Digest;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Acknowledgment(Digest);

impl Acknowledgment {
    pub fn from<T: Hash>(data: &T) -> Self {
        Self(data.digest())
    }
    pub fn from_digest(digest: &Digest) -> Self {
        Self(*digest)
    }
    pub fn verify(&self, digest: &[u8; 32]) -> bool {
        *digest == self.0
    }
}
