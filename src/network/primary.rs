use crate::types::{Digest, NetworkRequest, RequestPayload, SignedBlockHeader, Vote};
use libp2p::{Multiaddr, PeerId};
use std::{collections::HashMap, future::Future};
use tokio::sync::mpsc;

use super::{Connect, HandleEvent, ManagePeers, PrimaryNetwork, Relay};

pub struct PrimaryConnector {
    digest_tx: mpsc::Sender<Digest>,
    headers_tx: mpsc::Sender<SignedBlockHeader>,
    vote_tx: mpsc::Sender<Vote>,
}

pub struct PrimaryPeers {
    pub workers: Vec<(PeerId, Multiaddr)>,
    pub primaries: HashMap<PeerId, Multiaddr>,
}

impl Connect for PrimaryConnector {
    async fn dispatch(&self, payload: RequestPayload) -> anyhow::Result<()> {
        todo!()
    }
}

impl Relay for PrimaryPeers {
    fn get_broadcast_peers(&self) -> Vec<(PeerId, Multiaddr)> {
        todo!()
    }
    fn get_primary_peer(&self) -> Option<(PeerId, Multiaddr)> {
        todo!()
    }
    fn get_send_peer(&self) -> Option<(PeerId, Multiaddr)> {
        todo!()
    }
}

impl HandleEvent for PrimaryNetwork {
    async fn handle_event<P: ManagePeers, C: Connect>(
        event: libp2p::swarm::SwarmEvent<super::CalfBehaviorEvent>,
        swarm: &mut libp2p::Swarm<super::CalfBehavior>,
        peers: &mut P,
        connector: &mut C,
    ) -> anyhow::Result<()> {
        todo!()
    }
    async fn handle_request(
        swarm: &mut libp2p::Swarm<super::CalfBehavior>,
        request: NetworkRequest,
    ) -> anyhow::Result<()> {
        todo!()
    }
}
