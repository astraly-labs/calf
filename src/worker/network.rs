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
        dial_opts::{DialOpts, PeerCondition},
        DialError, NetworkBehaviour, SwarmEvent,
    },
    PeerId, StreamProtocol,
};
use std::sync::Arc;
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_util::sync::CancellationToken;

use crate::{
    settings::parser::{AuthorityInfo, WorkerAddresses},
    types::{NetworkRequest, ReceivedAcknowledgment, ReceivedBatch, RequestPayload},
};

use super::WorkerMetadata;

/// Agent version
const AGENT_VERSION: &str = "narwals/0.0.1";
/// Protocol
// Main protocol
const MAIN_PROTOCOL: &str = "/calf/1";
// Componenet Protocol /{worker/primary}/{id}

#[derive(NetworkBehaviour)]
struct WorkerBehaviour {
    identify: identify::Behaviour,
    mdns: mdns::tokio::Behaviour,
    request_response: request_response::cbor::Behaviour<Vec<u8>, ()>,
}

pub(crate) struct Network {
    element_metadata: WorkerMetadata,
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
    validator_key: Keypair,
}

impl Network {
    #[must_use]
    pub fn spawn(
        element_metadata: WorkerMetadata,
        network_rx: mpsc::Receiver<NetworkRequest>,
        received_ack_tx: mpsc::Sender<ReceivedAcknowledgment>,
        received_batches_tx: mpsc::Sender<ReceivedBatch>,
        local_key: Keypair,
        validator_key: Keypair,
        cancellation_token: CancellationToken,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let local_peer_id = PeerId::from(local_key.public());
            println!("local peer id: {local_peer_id}");
            let signature = format!(
                "/key/{}",
                hex::encode(validator_key.sign(&local_peer_id.to_bytes()).unwrap())
            );

            let component = format!("/worker/{}", element_metadata.id,);

            // initializing libp2p utilities
            let (to_dial_send, to_dial_recv) = mpsc::channel::<(PeerId, Multiaddr)>(100);

            let mdns_config = match mdns::tokio::Behaviour::new(
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
                    mdns: mdns_config,
                    request_response: {
                        let cfg =
                            request_response::Config::default().with_max_concurrent_streams(10);

                        request_response::cbor::Behaviour::<Vec<u8>, ()>::new(
                            [
                                (StreamProtocol::new(MAIN_PROTOCOL), ProtocolSupport::Full),
                                (
                                    StreamProtocol::try_from_owned(component).unwrap(),
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
                .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(15)))
                .build();

            let mut this = Self {
                element_metadata,
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

    pub async fn try_connect_to_peer(&mut self) -> anyhow::Result<()> {
        let peer_addr = Multiaddr::from_str("/ip4/127.0.0.1/udp/3019/quic-v1").unwrap();
        match self.dial_peer(PeerId::random(), peer_addr.clone()).await {
            Ok(_) => tracing::info!("succesfully connected to {}", peer_addr),
            Err(e) => tracing::error!("Error {:?}", e),
        };
        Ok(())
    }

    pub fn start_listening(&mut self) -> anyhow::Result<()> {
        let worker_addr =
            self.element_metadata.authority.workers[&self.element_metadata.id].clone();
        let (addr, port) = worker_addr.worker_to_worker.rsplit_once(":").unwrap();
        let w2w_addr = Multiaddr::empty()
            .with(Protocol::Ip4(addr.parse()?))
            .with(Protocol::Udp(port.parse()?))
            .with(Protocol::QuicV1);
        self.swarm.listen_on(w2w_addr)?;
        let (addr, port) = worker_addr.primary_to_worker.rsplit_once(":").unwrap();
        let p2w_addr = Multiaddr::empty()
            .with(Protocol::Ip4(addr.parse()?))
            .with(Protocol::Udp(port.parse()?))
            .with(Protocol::QuicV1);
        self.swarm.listen_on(p2w_addr)?;
        let (addr, port) = worker_addr.transactions.rsplit_once(":").unwrap();
        let tx_addr = Multiaddr::empty()
            .with(Protocol::Ip4(addr.parse()?))
            .with(Protocol::Udp(port.parse()?))
            .with(Protocol::QuicV1);
        self.swarm.listen_on(tx_addr)?;
        Ok(())
    }

    pub async fn run(&mut self) -> anyhow::Result<()> {
        // listening for worker to worker, worker to primary, and foreign worker to connect
        self.start_listening()?;

        // connect to know peers
        // connect to other workers from same validator
        self.try_connect_to_peer().await;
        // connect to our primary
        // connect to external workers

        self.try_connect_to_peer().await;

        // TODO: handle gracefully the shutdown of the network
        loop {
            tokio::select! {
                Some((peer_id, multiaddr)) = self.to_dial_recv.recv() => {
                    let _ = self.dial_peer(peer_id, multiaddr).await;
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

    async fn dial_peer(&mut self, peer_id: PeerId, multiaddr: Multiaddr) -> Result<(), DialError> {
        let dial_opts = DialOpts::peer_id(peer_id)
            .condition(PeerCondition::DisconnectedAndNotDialing)
            .addresses(vec![multiaddr.clone()])
            .build();
        println!("Dialing {} -> {peer_id}", self.my_addr);
        self.swarm.dial(dial_opts)
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
                    if info.protocol_version != MAIN_PROTOCOL {
                        tracing::info!(
                            "protocol missmatch, disconnecting from {}",
                            info.protocol_version
                        );
                        self.swarm
                            .disconnect_peer_id(peer_id)
                            .map_err(|_| anyhow::anyhow!("failed to disconnect from peer"))?;
                        return Ok(());
                    }
                    tracing::info!("🚨🚨 info.protocols : {:?}", info.protocols);
                    match info
                        .protocols
                        .iter()
                        .find(|s| s.to_string().starts_with("/key/"))
                    {
                        Some(key) => {
                            let auth_key = key.to_string();
                            if !auth_key.starts_with("/key/") {
                                tracing::info!("key not found, disconnecting from {}", peer_id);
                                self.swarm.disconnect_peer_id(peer_id).map_err(|_| {
                                    anyhow::anyhow!("failed to disconnect from peer")
                                })?;
                                return Ok(());
                            }
                            // safe unwrap since we check befort that the string start with /key/
                            tracing::info!("auth_key {}", auth_key);
                            let key = auth_key.strip_prefix("/key/").unwrap().to_string();
                            if key.is_empty() {
                                tracing::info!("key not found, disconnecting from {}", peer_id);
                                self.swarm.disconnect_peer_id(peer_id).map_err(|_| {
                                    anyhow::anyhow!("failed to disconnect from peer")
                                })?;
                                return Ok(());
                            }
                            // decode key
                            tracing::info!("key => {:?}", key);
                            let key = hex::decode(key).unwrap();
                            let is_verified = self
                                .validator_key
                                .public()
                                .verify(&peer_id.to_bytes(), &key);
                            tracing::info!(
                                "🎉 is verified {}, is {:?}, should be {:?}",
                                is_verified,
                                key,
                                self.validator_key.sign(&peer_id.to_bytes()).unwrap()
                            );
                            if !is_verified {}
                        }
                        None => {
                            tracing::info!("key not found, disconnecting from {}", peer_id);
                            self.swarm
                                .disconnect_peer_id(peer_id)
                                .map_err(|_| anyhow::anyhow!("failed to disconnect from peer"))?;
                        }
                    }
                }
            }
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
                        _ => {
                            tracing::warn!("Received unknown request, ignoring");
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
                println!("connection to {peer_id} closed, trying to reconnect");
                self.in_peers.remove(&peer_id);
                self.out_peers.remove(&peer_id);
                // match self.dial_peer(peer_id,  self.in_peers[&peer_id].clone()).await {
                //     Ok(_) => tracing::info!("reconnect succesfully !"),
                //     Err(_) => {
                //         println!("reconnection to {peer_id} failed => Closing connection");
                //     },
                // }
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
            SwarmEvent::OutgoingConnectionError { peer_id, error, .. } => {
                if let Some(peer_id) = peer_id {
                    println!("failed to dial {peer_id} : {:?}...", error);
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
                                Ok(_) => tracing::info!("tried to dial to {}", new_addr),
                                Err(e) => tracing::error!("Retry failed"),
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
