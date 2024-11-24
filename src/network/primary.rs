use crate::{
    settings::parser::Committee,
    types::{Digest, NetworkRequest, RequestPayload, SignedBlockHeader, Vote},
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
    digest_tx: broadcast::Sender<Digest>,
    headers_tx: broadcast::Sender<SignedBlockHeader>,
    vote_tx: broadcast::Sender<Vote>,
}

impl PrimaryConnector {
    pub fn new(
        buffer: usize,
    ) -> (
        Self,
        broadcast::Receiver<Digest>,
        broadcast::Receiver<SignedBlockHeader>,
        broadcast::Receiver<Vote>,
    ) {
        let (digest_tx, digest_rx) = broadcast::channel(buffer);
        let (headers_tx, headers_rx) = broadcast::channel(buffer);
        let (vote_tx, vote_rx) = broadcast::channel(buffer);

        (
            Self {
                digest_tx,
                headers_tx,
                vote_tx,
            },
            digest_rx,
            headers_rx,
            vote_rx,
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
    async fn dispatch(&self, payload: RequestPayload, _sender: PeerId) -> anyhow::Result<()> {
        match payload {
            RequestPayload::Digest(digest) => {
                tracing::info!("received batch digest: {:?}", digest);
                self.digest_tx.send(digest)?;
            }
            RequestPayload::Header(header) => {
                self.headers_tx.send(header)?;
            }
            RequestPayload::Vote(vote) => {
                self.vote_tx.send(vote)?;
            }
            _ => {}
        }
        Ok(())
    }
}

impl ManagePeers for PrimaryPeers {
    fn add_peer(&mut self, id: Peer, authority_pubkey: String) -> bool {
        match id {
            Peer::Primary(id, addr) => self.primaries.insert(id, addr).is_none(),
            Peer::Worker(id, addr, index) => {
                if authority_pubkey == self.authority_pubkey {
                    self.workers.push((id, addr));
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
        self.primaries.contains_key(&id) || self.workers.iter().any(|(peer_id, _)| peer_id == &id)
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
