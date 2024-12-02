use crate::{
    settings::parser::Committee,
    types::{
        block_header::BlockHeader, certificate::Certificate, Digest, NetworkRequest,
        ReceivedObject, RequestPayload, Vote,
    },
};
use async_trait::async_trait;
use libp2p::{Multiaddr, PeerId, Swarm};
use std::collections::HashMap;
use tokio::sync::broadcast;

use super::{
    swarm_actions, CalfBehavior, Connect, HandleEvent, ManagePeers, Peer, PeerIdentifyInfos,
    PrimaryNetwork,
};

pub struct PrimaryConnector {
    digest_tx: broadcast::Sender<ReceivedObject<Digest>>,
    headers_tx: broadcast::Sender<ReceivedObject<BlockHeader>>,
    vote_tx: broadcast::Sender<ReceivedObject<Vote>>,
    certificates_tx: broadcast::Sender<ReceivedObject<Certificate>>,
}

impl PrimaryConnector {
    pub fn new(
        buffer: usize,
    ) -> (
        Self,
        broadcast::Receiver<ReceivedObject<Digest>>,
        broadcast::Receiver<ReceivedObject<BlockHeader>>,
        broadcast::Receiver<ReceivedObject<Vote>>,
        broadcast::Receiver<ReceivedObject<Certificate>>,
    ) {
        let (digest_tx, digest_rx) = broadcast::channel(buffer);
        let (headers_tx, headers_rx) = broadcast::channel(buffer);
        let (vote_tx, vote_rx) = broadcast::channel(buffer);
        let (certificates_tx, certificates_rx) = broadcast::channel(buffer);

        (
            Self {
                digest_tx,
                headers_tx,
                vote_tx,
                certificates_tx,
            },
            digest_rx,
            headers_rx,
            vote_rx,
            certificates_rx,
        )
    }
}

pub struct PrimaryPeers {
    pub authority_pubkey: String,
    pub workers: Vec<(PeerId, Multiaddr)>,
    pub primaries: HashMap<PeerId, Multiaddr>,
    pub established: HashMap<PeerId, Multiaddr>,
}

#[async_trait]
impl Connect for PrimaryConnector {
    async fn dispatch(&self, payload: RequestPayload, sender: PeerId) -> anyhow::Result<()> {
        match payload {
            RequestPayload::Digest(digest) => {
                tracing::info!("received digest: {}", hex::encode(digest.clone()));
                self.digest_tx.send(ReceivedObject::new(digest, sender))?;
            }
            RequestPayload::Header(header) => {
                self.headers_tx.send(ReceivedObject::new(header, sender))?;
            }
            RequestPayload::Vote(vote) => {
                self.vote_tx.send(ReceivedObject::new(vote, sender))?;
            }
            RequestPayload::Certificate(certificate) => {
                self.certificates_tx
                    .send(ReceivedObject::new(certificate, sender))?;
            }
            _ => {}
        }
        Ok(())
    }
}

impl ManagePeers for PrimaryPeers {
    fn add_peer(&mut self, id: Peer, authority_pubkey: String) -> bool {
        match id {
            Peer::Primary(id, addr) => {
                if !self.primaries.contains_key(&id) {
                    self.primaries.insert(id, addr);
                    tracing::info!("primary {id} added to peers");
                }
                true
            }
            Peer::Worker(id, addr, index) => {
                if authority_pubkey == self.authority_pubkey {
                    if !self.workers.iter().any(|(peer_id, _)| peer_id == &id) {
                        self.workers.push((id, addr));
                        tracing::info!("worker {id} added to peers");
                    }
                    true
                } else {
                    false
                }
            }
        }
    }
    fn remove_peer(&mut self, id: PeerId) -> bool {
        self.primaries.remove(&id).is_some() || {
            let index = self.workers.iter().position(|(peer_id, _)| peer_id == &id);
            if let Some(index) = index {
                self.workers.remove(index);
                tracing::info!("worker {id} removed from peers");
                true
            } else {
                false
            }
        }
    }
    fn identify(&self) -> PeerIdentifyInfos {
        PeerIdentifyInfos::Primary(self.authority_pubkey.clone())
    }
    fn get_broadcast_peers(&self) -> Vec<(PeerId, Multiaddr)> {
        self.primaries
            .iter()
            .map(|(id, addr)| (*id, addr.clone()))
            .collect()
    }
    fn get_send_peer(&self, id: PeerId) -> Option<(PeerId, Multiaddr)> {
        self.primaries.get(&id).map(|addr| (id, addr.clone()))
    }
    fn contains_peer(&self, id: PeerId) -> bool {
        self.primaries.contains_key(&id)
            || self.workers.iter().any(|(peer_id, _)| peer_id == &id)
            || self.established.contains_key(&id)
    }
    fn get_to_dial_peers(committee: &Committee) -> Vec<(PeerId, Multiaddr)> {
        todo!()
    }
    fn add_established(&mut self, id: PeerId, addr: Multiaddr) {
        self.established.insert(id, addr);
    }
    fn established(&self) -> &HashMap<PeerId, Multiaddr> {
        &self.established
    }
}

#[async_trait]
impl HandleEvent<PrimaryPeers, PrimaryConnector> for PrimaryNetwork {
    fn handle_request(
        swarm: &mut Swarm<CalfBehavior>,
        request: NetworkRequest,
        peers: &PrimaryPeers,
    ) -> anyhow::Result<()> {
        match request {
            NetworkRequest::Broadcast(req) => {
                swarm_actions::broadcast(swarm, peers, req)?;
            }
            NetworkRequest::SendTo(id, req) => {
                swarm_actions::send(swarm, id, req)?;
            }
            _ => {}
        };
        Ok(())
    }
}
