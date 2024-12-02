use std::collections::HashMap;

use crate::{
    network::PeerIdentifyInfos,
    settings::parser::Committee,
    types::{NetworkRequest, ReceivedAcknowledgment, ReceivedBatch, RequestPayload},
};
use async_trait::async_trait;
use libp2p::{Multiaddr, PeerId, Swarm};
use tokio::sync::mpsc;

use super::{swarm_actions, CalfBehavior, Connect, HandleEvent, ManagePeers, Peer, WorkerNetwork};

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
    pub established: HashMap<PeerId, Multiaddr>,
}

impl WorkerPeers {
    pub fn new(id: u32, pubkey: String) -> Self {
        Self {
            this_id: (id, pubkey),
            primary: None,
            workers: HashMap::new(),
            established: HashMap::new(),
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
    fn handle_request(
        swarm: &mut Swarm<CalfBehavior>,
        request: NetworkRequest,
        peers: &WorkerPeers,
    ) -> anyhow::Result<()> {
        match request {
            NetworkRequest::Broadcast(req) => {
                swarm_actions::broadcast(swarm, peers, req)?;
            }
            NetworkRequest::SendTo(id, req) => {
                swarm_actions::send(swarm, id, req)?;
            }
            NetworkRequest::SendToPrimary(req) => match peers.primary {
                Some((id, _)) => {
                    if let RequestPayload::Digest(dgst) = req.clone() {}
                    swarm_actions::send(swarm, id, req)?;
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
    fn add_peer(&mut self, peer: Peer, pubkey: String) -> bool {
        match peer {
            Peer::Worker(id, addr, index) => {
                if pubkey != self.this_id.1
                    && index == self.this_id.0
                    && !self.workers.contains_key(&id)
                {
                    self.workers.insert(id, addr);
                    tracing::info!("worker {id} added to peers");
                    true
                } else {
                    false
                }
            }
            Peer::Primary(id, addr) => match self.primary {
                Some(_) => false,
                None => {
                    if pubkey == self.this_id.1 && self.primary.is_none() {
                        self.primary = Some((id, addr));
                        tracing::info!("primary {id} added to peers");
                        true
                    } else {
                        false
                    }
                }
            },
        }
    }
    fn remove_peer(&mut self, peer: PeerId) -> bool {
        if let Some(primary) = &self.primary {
            if primary.0 == peer {
                self.primary = None;
                true
            } else {
                self.workers.remove(&peer).is_some()
            }
        } else {
            self.workers.remove(&peer).is_some()
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
        self.workers.get(&id).map(|addr| (id, addr.clone()))
    }
    fn contains_peer(&self, id: PeerId) -> bool {
        self.workers.contains_key(&id)
            || self
                .primary
                .clone()
                .map(|(pid, _)| pid == id)
                .unwrap_or(false)
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
