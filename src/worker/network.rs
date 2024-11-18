use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};

use futures::StreamExt as _;
use libp2p::{
    core::{multiaddr::Multiaddr, ConnectedPoint},
    identify::{self},
    identity::Keypair,
    mdns,
    request_response::{self, ProtocolSupport},
    swarm::{
        dial_opts::{DialOpts, PeerCondition},
        NetworkBehaviour, SwarmEvent,
    },
    PeerId, StreamProtocol,
};
use tokio::{
    sync::mpsc,
    task::JoinHandle,
};

use crate::{safe_send, types::{
    NetworkRequest, ReceivedAcknoledgement, ReceivedBatch, RequestPayload,
}};

/// Agent version
const AGENT_VERSION: &str = "peer/0.0.1";
/// Protocol
const MAIN_PROTOCOL: &str = "/calf/1";
const COMPONENT_PROTOCOL: &str = "/worker/0.1";
const VALIDATOR_KEY: &str = "/validator/11111";

#[derive(NetworkBehaviour)]
struct WorkerBehaviour {
    identify: identify::Behaviour,
    mdns: mdns::tokio::Behaviour,
    request_response: request_response::cbor::Behaviour<Vec<u8>, ()>,
}

pub(crate) struct Network {
    swarm: libp2p::Swarm<WorkerBehaviour>,
    my_addr: Multiaddr,
    seen: BTreeSet<PeerId>,
    in_peers: BTreeMap<PeerId, Multiaddr>,
    out_peers: BTreeMap<PeerId, Multiaddr>,
    local_peer_id: PeerId,
    to_dial_send: mpsc::Sender<(PeerId, Multiaddr)>,
    to_dial_recv: mpsc::Receiver<(PeerId, Multiaddr)>,
    network_rx: mpsc::Receiver<NetworkRequest>,
    received_ack_tx: mpsc::Sender<ReceivedAcknoledgement>,
    received_batches_tx: mpsc::Sender<ReceivedBatch>,
}

impl Network {
    #[must_use]
    pub fn spawn(
        network_rx: mpsc::Receiver<NetworkRequest>,
        received_ack_tx: mpsc::Sender<ReceivedAcknoledgement>,
        received_batches_tx: mpsc::Sender<ReceivedBatch>,
        local_key: Keypair,
    ) -> JoinHandle<()> {
        let local_peer_id = PeerId::from(local_key.public());
        println!("local peer id: {local_peer_id}");

        let (to_dial_send, to_dial_recv) = mpsc::channel::<(PeerId, Multiaddr)>(100);

        let swarm = libp2p::SwarmBuilder::with_existing_identity(local_key.clone())
            .with_tokio()
            .with_quic()
            .with_behaviour(|key| WorkerBehaviour {
                identify: {
                    let cfg = identify::Config::new(MAIN_PROTOCOL.to_string(), key.public())
                        .with_push_listen_addr_updates(true)
                        .with_agent_version(AGENT_VERSION.to_string());
                    identify::Behaviour::new(cfg)
                },
                mdns: {
                    let cfg = mdns::Config::default();
                    mdns::tokio::Behaviour::new(cfg, key.public().to_peer_id()).unwrap()
                },
                request_response: {
                    let cfg = request_response::Config::default().with_max_concurrent_streams(10);
                    request_response::cbor::Behaviour::<Vec<u8>, ()>::new(
                        [(StreamProtocol::new(MAIN_PROTOCOL), ProtocolSupport::Full),
                        (StreamProtocol::new(COMPONENT_PROTOCOL), ProtocolSupport::Full),
                        (StreamProtocol::new(VALIDATOR_KEY), ProtocolSupport::Full)],
                        cfg,
                    )
                },
            })
            .unwrap()
            .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(60)))
            .build();

        tokio::spawn(async move {
            Self {
                swarm,
                my_addr: Multiaddr::empty(),
                seen: BTreeSet::new(),
                in_peers: BTreeMap::new(),
                out_peers: BTreeMap::new(),
                local_peer_id,
                network_rx,
                received_ack_tx: received_ack_tx,
                received_batches_tx: received_batches_tx,
                to_dial_send,
                to_dial_recv,
            }
            .run()
            .await;
        })
    }

    pub async fn run(&mut self) {
        self.swarm
            .listen_on("/ip4/0.0.0.0/udp/0/quic-v1".parse().unwrap())
            .unwrap();

        // TODO: handle gracefully the shutdown of the network
        loop {
            tokio::select! {
                Some((peer_id, multiaddr)) = self.to_dial_recv.recv() => {
                    self.dial_peer(peer_id, multiaddr).await;
                },
                event = self.swarm.select_next_some() => {
                    self.handle_event(event).await;
                },
                Some(message) = self.network_rx.recv() => {
                    match message {
                        NetworkRequest::Broadcast(message) => {
                            self.broadcast(vec![], message).unwrap();
                        },
                        NetworkRequest::SendTo(peer_id, message) => {
                            self.send(peer_id, message).unwrap();
                        }
                    }
                }
            }
        }
    }

    async fn dial_peer(&mut self, peer_id: PeerId, multiaddr: Multiaddr) {
        let dial_opts = DialOpts::peer_id(peer_id)
            .condition(PeerCondition::DisconnectedAndNotDialing)
            .addresses(vec![multiaddr.clone()])
            .build();
        println!("Dialing {} -> {peer_id}", self.my_addr);
        let _ = self.swarm.dial(dial_opts);
    }

    /// Sends a message to a specific peer.
    pub fn send(&mut self, peer_id: PeerId, message: RequestPayload) -> anyhow::Result<()> {
        let serialized = bincode::serialize(&message)?;
        self.swarm
            .behaviour_mut()
            .request_response
            .send_request(&peer_id, serialized);
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

    async fn handle_event(&mut self, event: SwarmEvent<WorkerBehaviourEvent>) {
        match event {
            SwarmEvent::Behaviour(WorkerBehaviourEvent::Mdns(event)) => match event {
                mdns::Event::Discovered(list) => {
                    for (peer_id, multiaddr) in list {
                        if peer_id != self.local_peer_id && self.seen.insert(peer_id) {
                            println!("mDNS discovered a new peer: {peer_id}");
                            if !self.out_peers.contains_key(&peer_id) {
                                safe_send!(self.to_dial_send, (peer_id, multiaddr.clone()), "failed to send to dial rx");
                            }
                        }
                    }
                }
                mdns::Event::Expired(list) => {
                    for (peer_id, _multiaddr) in list {
                        if peer_id != self.local_peer_id {
                            println!("mDNS peer has expired: {peer_id}");
                            self.seen.remove(&peer_id);
                            self.swarm
                                .disconnect_peer_id(peer_id)
                                .expect(&format!("failed to disconnect from {peer_id}"));
                        }
                    }
                }
            },
            SwarmEvent::Behaviour(WorkerBehaviourEvent::RequestResponse(
                request_response::Event::Message { peer, message },
            )) => match message {
                request_response::Message::Request {
                    request, channel, ..
                } => {
                    let peer_id = peer;
                    let req: Vec<u8> = request;
                    tracing::info!("request from {peer_id}: \"{:#?}\"", req);
                    let decoded = match bincode::deserialize::<RequestPayload>(&req) {
                        Ok(decoded) => decoded,
                        Err(e) => {
                            tracing::error!("failed to decode request: {e}");
                            return;
                        }
                    };
                    tracing::info!("decoded request: {:#?}", decoded);
                    match decoded {
                        RequestPayload::Batch(batch) => {
                            safe_send!(self.received_batches_tx, ReceivedBatch {
                                batch,
                                sender: peer_id,
                            }, "failed to send received batch from network");
                        }
                        RequestPayload::Acknoledgment(ack) => {
                            safe_send!(self.received_ack_tx, ReceivedAcknoledgement {
                                acknoledgement: ack,
                                sender: peer_id,
                            }, "failed to send acknoledgment from network");

                        }
                    }
                }
                request_response::Message::Response { response, .. } => {
                    let peer_id = peer;
                    println!("response from {peer_id}: \"{:#?}\"", response);
                }
            },

            // Swarm Events
            SwarmEvent::ConnectionClosed { peer_id, .. } => {
                println!("connection to {peer_id} closed");
                self.in_peers.remove(&peer_id);
                self.out_peers.remove(&peer_id);
            }
            SwarmEvent::ConnectionEstablished {
                peer_id, endpoint, ..
            } => match endpoint {
                ConnectedPoint::Dialer { address, .. } => {
                    println!("dialed to {peer_id}: {address}");
                    self.out_peers.insert(peer_id, address.clone());
                }
                ConnectedPoint::Listener { send_back_addr, .. } => {
                    println!("received dial from {peer_id}: {send_back_addr}");
                    self.in_peers.insert(peer_id, send_back_addr.clone());
                }
            },
            SwarmEvent::Dialing { peer_id, .. } => {
                if let Some(peer_id) = peer_id {
                    println!("dialing {peer_id}...");
                }
            }
            SwarmEvent::ExternalAddrConfirmed { address } => {
                println!("external address confirmed as {address}");
                self.my_addr = address;
            }
            SwarmEvent::NewListenAddr { address, .. } => {
                println!("local peer is listening on {address}");
                self.my_addr = address;
            }
            SwarmEvent::OutgoingConnectionError { peer_id, .. } => {
                if let Some(peer_id) = peer_id {
                    println!("failed to dial {peer_id}...");
                }
            }
            _ => {}
        }
    }
}
