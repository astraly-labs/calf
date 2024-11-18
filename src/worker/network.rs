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
use std::sync::Arc;
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_util::sync::CancellationToken;

use crate::types::{NetworkRequest, ReceivedAcknowledgment, ReceivedBatch, RequestPayload};

/// Agent version
const AGENT_VERSION: &str = "peer/0.0.1";
/// Protocol
// Main protocol
const MAIN_PROTOCOL: &str = "/calf/1";
// Componenet Protocol /{worker/primary}/{id}
const COMPONENT_PROTOCOL: &str = "/worker/1";
// Handshake key
// local peer id => encode avec ta keypair =>
const VALIDATOR_KEY: &str = "/key/{}";

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
    received_ack_tx: mpsc::Sender<ReceivedAcknowledgment>,
    received_batches_tx: mpsc::Sender<ReceivedBatch>,
    local_key: Keypair,
}

impl Network {
    #[must_use]
    pub fn spawn(
        network_rx: mpsc::Receiver<NetworkRequest>,
        received_ack_tx: mpsc::Sender<ReceivedAcknowledgment>,
        received_batches_tx: mpsc::Sender<ReceivedBatch>,
        local_key: Keypair,
        cancellation_token: CancellationToken,
    ) -> JoinHandle<()> {
        let local_peer_id = PeerId::from(local_key.public());
        println!("local peer id: {local_peer_id}");
        let signature = format!(
            "/key/{:?}",
            hex::encode(local_key.sign(&local_peer_id.to_bytes()).unwrap()).as_str()
        );

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
                    mdns::tokio::Behaviour::new(cfg, key.public().to_peer_id())
                        .expect("failed to convert publickey to peer id: shuting down")
                },
                request_response: {
                    let cfg = request_response::Config::default().with_max_concurrent_streams(10);

                    request_response::cbor::Behaviour::<Vec<u8>, ()>::new(
                        [
                            (StreamProtocol::new(MAIN_PROTOCOL), ProtocolSupport::Full),
                            (
                                StreamProtocol::new(COMPONENT_PROTOCOL),
                                ProtocolSupport::Full,
                            ),
                            (
                                StreamProtocol::try_from_owned(signature).unwrap(),
                                ProtocolSupport::Full,
                            ),
                        ],
                        cfg,
                    )
                },
            })
            .unwrap()
            .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(60)))
            .build();

        tokio::spawn(async move {
            let mut this = Self {
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
                local_key,
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
                            tracing::error!("worker network finished with an error: {e}");
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

    async fn handle_event(
        &mut self,
        event: SwarmEvent<WorkerBehaviourEvent>,
    ) -> anyhow::Result<()> {
        match event {
            SwarmEvent::Behaviour(WorkerBehaviourEvent::Identify(identify::Event::Received {
                peer_id,
                info,
            })) => {
                if peer_id != self.local_peer_id {
                    match info.protocols.get(2) {
                        Some(key) => {
                            let auth_key = key.to_string();
                            if !auth_key.starts_with("/key/") {
                                tracing::info!("key not found, disconnecting from {}", peer_id);
                                self.swarm
                                    .disconnect_peer_id(peer_id)
                                    .expect(&format!("failed to disconnect from {peer_id}"));
                            }
                            // safe unwrap since we check befort that the string start with /key/
                            let key = auth_key.strip_prefix("/key/").unwrap().to_string();
                            if key.is_empty() {
                                tracing::info!("key not found, disconnecting from {}", peer_id);
                                self.swarm
                                    .disconnect_peer_id(peer_id)
                                    .expect(&format!("failed to disconnect from {peer_id}"));
                            }
                            // decode key
                            let key = hex::decode(key).unwrap();
                            let is_verified =
                                self.local_key.public().verify(&peer_id.to_bytes(), &key);
                            tracing::info!(
                                "is verified {}, is {:?}, should be {:?}",
                                is_verified,
                                key,
                                self.local_key.sign(&peer_id.to_bytes()).unwrap()
                            );
                        }
                        None => {
                            tracing::info!("key not found, disconnecting from {}", peer_id);
                            self.swarm
                                .disconnect_peer_id(peer_id)
                                .expect(&format!("failed to disconnect from {peer_id}"));
                        }
                    }
                }
            }
            SwarmEvent::Behaviour(WorkerBehaviourEvent::Mdns(event)) => match event {
                mdns::Event::Discovered(list) => {
                    for (peer_id, multiaddr) in list {
                        if peer_id != self.local_peer_id && self.seen.insert(peer_id) {
                            println!("mDNS discovered a new peer: {peer_id}");
                            if !self.out_peers.contains_key(&peer_id) {
                                self.to_dial_send.send((peer_id, multiaddr.clone())).await?;
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
                    let decoded = bincode::deserialize::<RequestPayload>(&req)?;
                    tracing::info!("decoded request: {:#?}", decoded);
                    match decoded {
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
        Ok(())
    }
}
