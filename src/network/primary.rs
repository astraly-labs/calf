use crate::types::{Digest, NetworkRequest, RequestPayload, SignedBlockHeader, Vote};
use async_trait::async_trait;
use libp2p::{Multiaddr, PeerId, Swarm};
use std::collections::HashMap;
use tokio::sync::mpsc;

use super::{
    CalfBehavior, CalfBehaviorEvent, Connect, HandleEvent, ManagePeers, Peer, PeerIdentifyInfos,
    PrimaryNetwork,
};

pub struct PrimaryConnector {
    digest_tx: mpsc::Sender<Digest>,
    headers_tx: mpsc::Sender<SignedBlockHeader>,
    vote_tx: mpsc::Sender<Vote>,
}

struct PrimaryPeers {
    pub authority_publey: String,
    pub workers: Vec<(PeerId, Multiaddr)>,
    pub primaries: HashMap<PeerId, Multiaddr>,
}

#[async_trait]
impl Connect for PrimaryConnector {
    async fn dispatch(&self, payload: RequestPayload, sender: PeerId) -> anyhow::Result<()> {
        todo!()
    }
}

impl ManagePeers for PrimaryPeers {
    fn add_peer(&mut self, id: Peer) -> bool {
        match id {
            Peer::Primary(id, addr) => self.primaries.insert(id, addr).is_none(),
            Peer::Worker(id, addr) => {
                self.workers.push((id, addr));
                true
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
        PeerIdentifyInfos::Primary(self.authority_publey.clone())
    }
    fn get_broadcast_peers(&self) -> Vec<(PeerId, Multiaddr)> {
        self.primaries
            .iter()
            .map(|(id, addr)| (*id, addr.clone()))
            .collect()
    }
    fn get_send_peer(&self, id: PeerId) -> Option<(PeerId, Multiaddr)> {
        self.primaries
            .get(&id)
            .map(|addr| (id.clone(), addr.clone()))
    }
    fn contains_peer(&self, id: PeerId) -> bool {
        self.primaries.contains_key(&id) || self.workers.iter().any(|(peer_id, _)| peer_id == &id)
    }
    fn get_to_dial_peers(
        &self,
        committee: crate::settings::parser::Committee,
    ) -> Vec<(PeerId, Multiaddr)> {
        todo!()
    }
}

#[async_trait]
impl HandleEvent<PrimaryPeers, PrimaryConnector> for PrimaryNetwork {
    async fn handle_event(
        event: libp2p::swarm::SwarmEvent<CalfBehaviorEvent>,
        swarm: &mut libp2p::Swarm<CalfBehavior>,
        peers: &mut PrimaryPeers,
        connector: &mut PrimaryConnector,
    ) -> anyhow::Result<()> {
        todo!()
    }
    fn handle_request(
        swarm: &mut Swarm<CalfBehavior>,
        request: NetworkRequest,
        peers: &PrimaryPeers,
    ) -> anyhow::Result<()> {
        todo!()
    }
}
