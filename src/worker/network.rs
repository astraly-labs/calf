use std::{
    collections::{BTreeMap, BTreeSet},
    str::FromStr,
    time::Duration,
};

use futures::StreamExt as _;
use libp2p::{
    core::{multiaddr::Multiaddr, ConnectedPoint},
    identify::{self},
    identity::Keypair,
    mdns,
    multiaddr::Protocol,
    request_response::{self, ProtocolSupport},
    swarm::{
        behaviour,
        dial_opts::{DialOpts, PeerCondition},
        DialError, NetworkBehaviour, SwarmEvent,
    },
    PeerId, StreamProtocol,
};
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_util::sync::CancellationToken;

use crate::{
    settings::parser::{AuthorityInfo, Committee},
    types::{IdentifyInfo, NetworkRequest, ReceivedAcknowledgment, ReceivedBatch, RequestPayload},
};

use super::WorkerMetadata;

const MAIN_PROTOCOL: &str = "/calf/0/";

#[derive(NetworkBehaviour)]
struct WorkerBehaviour {
    identify: identify::Behaviour,
    mdns: mdns::tokio::Behaviour,
    request_response: request_response::cbor::Behaviour<RequestPayload, ()>,
}

pub(crate) struct Network {
    commitee: Committee,
    swarm: libp2p::Swarm<WorkerBehaviour>,
    my_addr: Multiaddr,
    peers: BTreeMap<PeerId, Multiaddr>,
    local_peer_id: PeerId,
    network_rx: mpsc::Receiver<NetworkRequest>,
    received_ack_tx: mpsc::Sender<ReceivedAcknowledgment>,
    received_batches_tx: mpsc::Sender<ReceivedBatch>,
    validator_key: Keypair,
}

impl Network {
    #[must_use]
    pub fn spawn(
        network_rx: mpsc::Receiver<NetworkRequest>,
        received_ack_tx: mpsc::Sender<ReceivedAcknowledgment>,
        received_batches_tx: mpsc::Sender<ReceivedBatch>,
        local_key: Keypair,
        validator_key: Keypair,
        commitee: Committee,
        cancellation_token: CancellationToken,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let agent_infos = serde_json::to_string(&IdentifyInfo::Worker(0)).unwrap();
            let local_peer_id = PeerId::from(local_key.public());
            let mdns = match mdns::tokio::Behaviour::new(
                mdns::Config::default(),
                local_key.public().to_peer_id(),
            ) {
                Ok(mdns) => mdns,
                Err(e) => {
                    tracing::error!("failed to create mdns behaviour: exiting {e}");
                    cancellation_token.cancel();
                    return;
                }
            };

            let identify_config = identify::Config::new(MAIN_PROTOCOL.into(), local_key.public())
                .with_agent_version(agent_infos)
                .with_push_listen_addr_updates(true);

            let swarm = libp2p::SwarmBuilder::with_existing_identity(local_key.clone())
                .with_tokio()
                .with_quic()
                .with_behaviour(|_| WorkerBehaviour {
                    identify: identify::Behaviour::new(identify_config),
                    mdns,
                    request_response: {
                        let cfg =
                            request_response::Config::default().with_max_concurrent_streams(10);

                        request_response::cbor::Behaviour::<RequestPayload, ()>::new(
                            [(StreamProtocol::new(MAIN_PROTOCOL), ProtocolSupport::Full)],
                            cfg,
                        )
                    },
                })
                .unwrap()
                .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(15)))
                .build();

            let mut this = Self {
                commitee,
                swarm,
                my_addr: Multiaddr::empty(),
                peers: BTreeMap::new(),
                local_peer_id,
                network_rx,
                received_ack_tx: received_ack_tx,
                received_batches_tx: received_batches_tx,
                validator_key,
            };
            let run = this.run();
            let res = cancellation_token.run_until_cancelled(run).await;

            match res {
                Some(res) => {
                    match res {
                        Ok(_) => {
                            tracing::info!("worker network finished successfully");
                        }
                        Err(e) => {
                            tracing::error!("worker network finished with an error: {:#?}", e);
                        }
                    };
                    cancellation_token.cancel();
                }
                None => {
                    tracing::info!("worker network has been cancelled");
                }
            }
        })
    }

    pub async fn run(&mut self) -> anyhow::Result<()> {
        loop {
            tokio::select! {
                event = self.swarm.select_next_some() => {
                    self.handle_event(event).await?;
                },
                Some(message) = self.network_rx.recv() => {
                    match message {
                        NetworkRequest::Broadcast(message) => {
                            self.broadcast(vec![], message).unwrap();
                        },
                        NetworkRequest::SendTo(peer_id, message) => {
                            self.send(peer_id, message).unwrap();
                        },
                        NetworkRequest::SendToPrimary(message) => {
                            todo!();
                        }
                    }
                }
            }
        }
    }

    async fn dial_peer(&mut self, peer_id: PeerId, multiaddr: Multiaddr) -> Result<(), DialError> {
        let dial_opts = DialOpts::peer_id(peer_id)
            .condition(PeerCondition::DisconnectedAndNotDialing)
            .addresses(vec![multiaddr.clone()])
            .build();
        tracing::info!("Dialing {} -> {peer_id}", self.my_addr);
        self.swarm.dial(dial_opts)
    }

    /// Sends a message to a specific peer.
    pub fn send(&mut self, peer_id: PeerId, message: RequestPayload) -> anyhow::Result<()> {
        self.swarm
            .behaviour_mut()
            .request_response
            .send_request(&peer_id, message);
        Ok(())
    }

    /// Broadcasts a message to all connected peers.
    pub fn broadcast(
        &mut self,
        _peers: Vec<String>,
        message: RequestPayload,
    ) -> anyhow::Result<()> {
        let connected_peers: Vec<PeerId> = self.swarm.connected_peers().cloned().collect();
        for peer_id in connected_peers {
            self.send(peer_id, message.clone())?;
        }
        Ok(())
    }

    async fn handle_event(
        &mut self,
        event: SwarmEvent<WorkerBehaviourEvent>,
    ) -> anyhow::Result<()> {
        match event {
            SwarmEvent::Behaviour(WorkerBehaviourEvent::Identify(identify::Event::Received {
                peer_id,
                info,
            })) => {
                tracing::info!("received identify info from {peer_id}: {:#?}", info);
            }
            SwarmEvent::Behaviour(WorkerBehaviourEvent::RequestResponse(
                request_response::Event::Message { peer: peer_id, message },
            )) => match message {
                request_response::Message::Request {
                    request, channel: _, ..
                } => {
                    match request {
                        RequestPayload::Batch(batch) => {
                            self.received_batches_tx
                                .send(ReceivedBatch {
                                    batch,
                                    sender: peer_id,
                                })
                                .await?;
                        }
                        RequestPayload::Acknoledgment(ack) => {
                            self.received_ack_tx
                                .send(ReceivedAcknowledgment {
                                    acknoledgement: ack,
                                    sender: peer_id,
                                })
                                .await?;
                        }
                        _ => {
                            tracing::warn!("Received unknown request, ignoring");
                        }
                    }
                },
                _ => {}
            },

            // Swarm Events
            SwarmEvent::ConnectionClosed { peer_id, .. } => {
                tracing::info!("connection to {peer_id} closed");
            }
            SwarmEvent::ConnectionEstablished {
                peer_id, endpoint, ..
            } => match endpoint {
                ConnectedPoint::Dialer { address, .. } => {
                    tracing::info!("dialed to {peer_id}: {address}");
                    self.peers.insert(peer_id, address.clone());
                }
                ConnectedPoint::Listener { send_back_addr, .. } => {
                    tracing::info!("received dial from {peer_id}: {send_back_addr}");
                    self.peers.insert(peer_id, send_back_addr.clone());
                }
            },
            SwarmEvent::OutgoingConnectionError { peer_id, error, .. } => {
                if let Some(_) = peer_id {
                    match error {
                        DialError::WrongPeerId { obtained, endpoint } => {
                            let addr: Multiaddr = endpoint.get_remote_address().clone();
                            let pos = addr
                                .iter()
                                .position(|p| matches!(p, Protocol::P2p(_)))
                                .unwrap();
                            let new_addr = addr
                                .replace(pos, |_| Some(Protocol::P2p(obtained)))
                                .unwrap();
                            match self.dial_peer(obtained, new_addr.clone()).await {
                                Ok(_) => {},
                                Err(_) => {},
                            }
                        }
                        _ => {}
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }
}
