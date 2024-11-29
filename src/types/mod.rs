pub mod agents;
pub mod signing;

use std::{
    collections::HashMap,
    time::{SystemTime, UNIX_EPOCH},
};

use libp2p::{
    identity::ed25519::{self, Keypair},
    PeerId,
};
use serde::{Deserialize, Serialize};
use signing::{Sign, Signable, Signature, SignedType};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Transaction {
    pub data: Vec<u8>,
}

impl Transaction {
    pub fn new(data: Vec<u8>) -> Self {
        Self { data }
    }
    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }
    pub fn random(size: usize) -> Self {
        Self {
            data: (0..size).map(|_| rand::random::<u8>()).collect(),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum NetworkRequest {
    Broadcast(RequestPayload),
    SendTo(PeerId, RequestPayload),
    SendToPrimary(RequestPayload),
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum RequestPayload {
    Batch(TxBatch),
    Acknowledgment(Digest),
    Digest(Digest),
    Header(BlockHeader),
    Certificate(Certificate),
    Vote(Vote),
}

#[derive(Clone, Debug)]
pub struct ReceivedObject<T>
where
    T: Clone,
{
    pub object: T,
    pub sender: PeerId,
}

impl<T> ReceivedObject<T>
where
    T: Clone,
{
    pub fn new(object: T, sender: PeerId) -> Self {
        Self { object, sender }
    }
}

pub struct ReceivedBatch {
    pub batch: TxBatch,
    pub sender: PeerId,
}

#[derive(Debug, Clone)]
pub struct ReceivedAcknowledgment {
    pub acknowledgement: Digest,
    pub sender: PeerId,
}

pub type TxBatch = Vec<Transaction>;
pub type Digest = [u8; 32];
pub type PublicKey = [u8; 32];
pub type WorkerId = u32;
pub type Stake = u64;
pub type Round = u64;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct BlockHeader {
    pub author: PublicKey,
    pub round: Round,
    pub timestamp_ms: u128,
    pub digests: Vec<Digest>,
    pub certificates: Vec<Certificate>,
}

impl BlockHeader {
    pub fn new(
        author: PublicKey,
        digests: Vec<Digest>,
        certificates: Vec<Certificate>,
        round: Round,
    ) -> Self {
        Self {
            author,
            round,
            timestamp_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("critical error: time is broken")
                .as_millis(),
            digests,
            certificates,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct Certificate {
    author: PublicKey,
    votes: Vec<Vote>,
    header: BlockHeader,
}

impl Certificate {
    pub fn new(votes: Vec<Vote>, header: BlockHeader, author: PublicKey) -> Self {
        Self {
            author,
            votes,
            header,
        }
    }
}

impl Signable for BlockHeader {}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct Vote {
    authority: PublicKey,
    signature: Signature,
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
}

pub type SignedBlockHeader = SignedType<BlockHeader>;

pub type Dag = HashMap<Round, (PeerId, Certificate)>;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum IdentifyInfo {
    Worker(WorkerId),
    Primary(PrimaryInfo),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct WorkerInfo {
    pub id: WorkerId,
    pub signature: SignedType<PeerId>,
    pub authority_pubkey: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PrimaryInfo {
    pub signature: SignedType<PeerId>,
    pub authority_pubkey: String,
}

pub trait Hash {
    fn digest(&self) -> anyhow::Result<Digest>;
}

impl<T> Hash for T
where
    T: Serialize,
{
    fn digest(&self) -> anyhow::Result<Digest> {
        let ser = bincode::serialize(&self)?;
        Ok(*blake3::hash(&ser).as_bytes())
    }
}
