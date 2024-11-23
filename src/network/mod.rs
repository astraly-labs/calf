use crate::{
    settings::parser::Committee,
    types::{NetworkRequest, RequestPayload},
};
use async_trait::async_trait;
use futures::StreamExt;
use libp2p::{
    core::multiaddr::Multiaddr,
    identify::{self},
    identity::Keypair,
    mdns,
    request_response::{self, ProtocolSupport},
    swarm::{NetworkBehaviour, SwarmEvent},
    PeerId, StreamProtocol, Swarm,
};
use std::{marker::PhantomData, time::Duration};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

pub mod primary;
pub mod worker;

pub struct WorkerNetwork;
pub struct PrimaryNetwork;

const MAIN_PROTOCOL: &str = "/calf/0/";

#[derive(NetworkBehaviour)]
pub struct CalfBehavior {
    identify: identify::Behaviour,
    mdns: mdns::tokio::Behaviour,
    request_response: request_response::cbor::Behaviour<RequestPayload, ()>,
}

pub trait Relay {
    fn get_broadcast_peers(&self) -> Vec<(PeerId, Multiaddr)>;
    fn get_send_peer(&self) -> Option<(PeerId, Multiaddr)>;
    fn get_primary_peer(&self) -> Option<(PeerId, Multiaddr)>;
}

pub trait ManagePeers {
    fn add_peer(&mut self, id: PeerId, addr: Multiaddr) -> bool;
    fn remove_peer(&mut self, id: PeerId) -> bool;
}

#[async_trait]
pub trait Connect {
    async fn dispatch(&self, payload: RequestPayload) -> anyhow::Result<()>;
}

#[async_trait]
pub trait HandleEvent {
    async fn handle_event<P: ManagePeers, C: Connect>(
        event: SwarmEvent<CalfBehaviorEvent>,
        swarm: &mut Swarm<CalfBehavior>,
        peers: &mut P,
        connector: &mut C,
    ) -> anyhow::Result<()>;
    async fn handle_request(
        swarm: &mut Swarm<CalfBehavior>,
        request: NetworkRequest,
    ) -> anyhow::Result<()>;
}

pub(crate) struct Network<A, C, P>
where
    A: HandleEvent,
    C: Connect,
    P: Relay + ManagePeers,
{
    committee: Committee,
    swarm: libp2p::Swarm<CalfBehavior>,
    peers: P,
    connector: C,
    requests_rx: mpsc::Receiver<NetworkRequest>,
    authority_keypair: Keypair,
    keypair: Keypair,
    _role: PhantomData<A>,
}

impl<A, C, P> Network<A, C, P>
where
    A: HandleEvent + Send,
    C: Connect + Send + 'static,
    P: Relay + ManagePeers + Send + 'static,
{
    pub fn spawn(
        committee: Committee,
        connector: C,
        authority_keypair: Keypair,
        keypair: Keypair,
        peers: P,
        requests_rx: mpsc::Receiver<NetworkRequest>,
        cancellation_token: CancellationToken,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mdns = match mdns::tokio::Behaviour::new(
                mdns::Config::default(),
                keypair.public().to_peer_id(),
            ) {
                Ok(mdns) => mdns,
                Err(e) => {
                    tracing::error!("failed to create mdns behaviour: exiting {e}");
                    cancellation_token.cancel();
                    return;
                }
            };

            let identify_config = identify::Config::new(MAIN_PROTOCOL.into(), keypair.public())
                .with_agent_version("TODO".into())
                .with_push_listen_addr_updates(true);

            let swarm = libp2p::SwarmBuilder::with_existing_identity(keypair.clone())
                .with_tokio()
                .with_quic()
                .with_behaviour(|_| CalfBehavior {
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
                committee,
                swarm,
                peers,
                connector,
                requests_rx,
                authority_keypair,
                keypair,
                _role: PhantomData,
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

    async fn run(&mut self) -> anyhow::Result<()> {
        loop {
            tokio::select! {
                event = self.swarm.select_next_some() => {
                    A::handle_event(event, &mut self.swarm, &mut self.peers, &mut self.connector).await?;
                },
                Some(message) = self.requests_rx.recv() => {
                    A::handle_request(&mut self.swarm, message).await?;
                }
            }
        }
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
    pub fn broadcast(&mut self, message: RequestPayload) -> anyhow::Result<()> {
        let peers = self.peers.get_broadcast_peers();
        for (id, _) in peers {
            self.send(id, message.clone())?;
        }
        Ok(())
    }
}
