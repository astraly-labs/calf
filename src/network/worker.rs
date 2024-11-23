use std::{collections::HashMap, future::Future};

use crate::types::{NetworkRequest, ReceivedAcknowledgment, ReceivedBatch, RequestPayload};
use async_trait::async_trait;
use libp2p::{Multiaddr, PeerId};
use tokio::sync::mpsc;

use super::{Connect, HandleEvent, ManagePeers, Relay, WorkerNetwork};

pub struct WorkerConnector {
    acks_tx: mpsc::Sender<ReceivedAcknowledgment>,
    batches_tx: mpsc::Sender<ReceivedBatch>,
}

impl WorkerConnector {
    pub fn new(
        buffer: usize,
    ) -> (
        Self,
        mpsc::Receiver<ReceivedAcknowledgment>,
        mpsc::Receiver<ReceivedBatch>,
    ) {
        let (acks_tx, acks_rx) = mpsc::channel(buffer);
        let (batches_tx, batches_rx) = mpsc::channel(buffer);
        (
            Self {
                acks_tx,
                batches_tx,
            },
            acks_rx,
            batches_rx,
        )
    }
}

pub struct WorkerPeers {
    pub primary: (PeerId, Multiaddr),
    pub wokers: HashMap<PeerId, Multiaddr>,
}

#[async_trait]
impl Connect for WorkerConnector {
    async fn dispatch(&self, payload: RequestPayload) -> anyhow::Result<()> {
        todo!()
    }
}

impl Relay for WorkerPeers {
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

#[async_trait]
impl HandleEvent for WorkerPeers {
    async fn handle_event<P: super::ManagePeers, C: Connect>(
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

#[async_trait]
impl HandleEvent for WorkerNetwork {
    async fn handle_event<P: super::ManagePeers, C: Connect>(
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

impl ManagePeers for WorkerPeers {
    fn add_peer(&mut self, id: PeerId, addr: Multiaddr) -> bool {
        todo!()
    }
    fn remove_peer(&mut self, id: PeerId) -> bool {
        todo!()
    }
}
