use std::{collections::HashMap, default};

use crate::{
    network::PeerIdentifyInfos,
    settings::parser::Committee,
    types::{NetworkRequest, ReceivedAcknowledgment, ReceivedBatch, RequestPayload},
};
use async_trait::async_trait;
use libp2p::{
    core::ConnectedPoint, identify, identity::ed25519::PublicKey, mdns, request_response,
    swarm::SwarmEvent, Multiaddr, PeerId, Swarm,
};
use tokio::sync::mpsc;

use super::{
    broadcast, dial_peer, send, CalfBehavior, CalfBehaviorEvent, Connect, HandleEvent, ManagePeers,
    Peer, WorkerNetwork, MAIN_PROTOCOL,
};

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
    pub this_id: (u32, String),
    pub primary: Option<(PeerId, Multiaddr)>,
    pub workers: HashMap<PeerId, Multiaddr>,
}

impl WorkerPeers {
    pub fn new(id: u32, pubkey: String) -> Self {
        Self {
            this_id: (id, pubkey),
            primary: None,
            workers: HashMap::new(),
        }
    }
}

#[async_trait]
impl Connect for WorkerConnector {
    async fn dispatch(&self, payload: RequestPayload, sender: PeerId) -> anyhow::Result<()> {
        match payload {
            RequestPayload::Acknowledgment(ack) => {
                self.acks_tx
                    .send(ReceivedAcknowledgment {
                        sender,
                        acknowledgement: ack,
                    })
                    .await?;
            }
            RequestPayload::Batch(batch) => {
                self.batches_tx
                    .send(ReceivedBatch { batch, sender })
                    .await?;
            }
            _ => {}
        }
        Ok(())
    }
}

#[async_trait]
impl HandleEvent<WorkerPeers, WorkerConnector> for WorkerNetwork {
    async fn handle_event(
        event: libp2p::swarm::SwarmEvent<CalfBehaviorEvent>,
        swarm: &mut libp2p::Swarm<super::CalfBehavior>,
        peers: &mut WorkerPeers,
        connector: &mut WorkerConnector,
    ) -> anyhow::Result<()> {
        match event {
            SwarmEvent::Behaviour(CalfBehaviorEvent::RequestResponse(
                request_response::Event::Message { peer, message },
            )) => match message {
                request_response::Message::Request { request, .. } => {
                    connector.dispatch(request, peer).await?;
                }
                _ => {}
            },
            SwarmEvent::ConnectionClosed { peer_id, .. } => {
                peers.remove_peer(peer_id);
                tracing::info!("connection to {peer_id} closed");
            }
            SwarmEvent::ConnectionEstablished {
                peer_id, endpoint, ..
            } => {
                match endpoint {
                    ConnectedPoint::Dialer { address, .. } => {
                        tracing::info!("dialed to {peer_id}: {address}");
                    }
                    ConnectedPoint::Listener { send_back_addr, .. } => {
                        tracing::info!("received dial from {peer_id}: {send_back_addr}");
                    }
                };
            }
            SwarmEvent::Behaviour(CalfBehaviorEvent::Identify(identify::Event::Received {
                peer_id,
                info,
            })) => {
                tracing::info!("received identify info from {peer_id}");
                if info.protocol_version != MAIN_PROTOCOL {
                    tracing::info!("{peer_id} doesn't speak our protocol")
                }
                match serde_json::from_str::<PeerIdentifyInfos>(&info.agent_version) {
                    Ok(infos) => match infos {
                        PeerIdentifyInfos::Worker(id, pubkey) => {}
                        PeerIdentifyInfos::Primary(signature) => {}
                    },
                    Err(_) => {
                        tracing::warn!(
                            "Failed to parse identify infos for {peer_id}, disconnecting"
                        );
                        swarm
                            .disconnect_peer_id(peer_id)
                            .map_err(|_| anyhow::anyhow!("failed to disconned from peer"))?;
                    }
                }
            }
            SwarmEvent::Behaviour(CalfBehaviorEvent::Mdns(event)) => match event {
                mdns::Event::Discovered(list) => {
                    tracing::info!("Discovered peers: {:?}", list);
                    let to_dial = list
                        .into_iter()
                        .filter(|(peer_id, _)| !peers.contains_peer(*peer_id));
                    to_dial.for_each(|(id, addr)| {
                        //TODO: handle error ?
                        let _res = dial_peer(swarm, id, addr);
                    });
                }
                _ => {}
            },
            _ => {}
        };
        Ok(())
    }

    fn handle_request(
        swarm: &mut Swarm<CalfBehavior>,
        request: NetworkRequest,
        peers: &WorkerPeers,
    ) -> anyhow::Result<()> {
        match request {
            NetworkRequest::Broadcast(req) => {
                broadcast(swarm, peers, req)?;
            }
            NetworkRequest::SendTo(id, req) => {
                send(swarm, id, req)?;
            }
            NetworkRequest::SendToPrimary(req) => match peers.primary {
                Some((id, _)) => {
                    send(swarm, id, req)?;
                }
                None => {
                    tracing::error!("No primary peer, unable to send request");
                }
            },
        };
        Ok(())
    }
}

impl ManagePeers for WorkerPeers {
    fn add_peer(&mut self, peer: Peer) -> bool {
        match peer {
            Peer::Worker(id, addr) => self.workers.insert(id, addr).is_none(),
            Peer::Primary(id, addr) => match self.primary {
                Some(_) => false,
                None => {
                    self.primary = Some((id, addr));
                    true
                }
            },
        }
    }
    fn remove_peer(&mut self, peer: PeerId) -> bool {
        if let Some(primary) = &self.primary {
            if primary.0 == peer {
                self.primary = None;
                return true;
            } else {
                return self.workers.remove(&peer).is_some();
            }
        } else {
            return self.workers.remove(&peer).is_some();
        }
    }
    fn identify(&self) -> PeerIdentifyInfos {
        PeerIdentifyInfos::Worker(self.this_id.0, self.this_id.1.clone())
    }
    fn get_broadcast_peers(&self) -> Vec<(PeerId, Multiaddr)> {
        self.workers
            .iter()
            .map(|(id, addr)| (*id, addr.clone()))
            .collect()
    }

    fn get_send_peer(&self, id: PeerId) -> Option<(PeerId, Multiaddr)> {
        match self.workers.get(&id) {
            Some(addr) => Some((id.clone(), addr.clone())),
            None => None,
        }
    }
    fn contains_peer(&self, id: PeerId) -> bool {
        self.workers.contains_key(&id)
            || self
                .primary
                .clone()
                .map(|(pid, _)| pid == id)
                .unwrap_or(false)
    }
    fn get_to_dial_peers(&self, committee: Committee) -> Vec<(PeerId, Multiaddr)> {
        todo!()
    }
}
