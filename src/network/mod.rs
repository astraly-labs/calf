use crate::{
    settings::parser::Committee,
    types::network::{NetworkRequest, RequestPayload},
};
use async_trait::async_trait;
use futures::StreamExt;
use libp2p::{
    core::multiaddr::Multiaddr,
    identify::{self},
    identity::{ed25519, Keypair},
    mdns,
    request_response::{self, ProtocolSupport},
    swarm::NetworkBehaviour,
    PeerId, StreamProtocol, Swarm,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    marker::PhantomData,
    sync::Arc,
    time::Duration,
};
use tokio::sync::{mpsc, RwLock};
use tokio_util::sync::CancellationToken;

pub mod primary;
pub mod swarm_actions;
pub mod swarm_events;
pub mod worker;

pub struct WorkerNetwork;
pub struct PrimaryNetwork;

const MAIN_PROTOCOL: &str = "/calf/0/";
const MAX_CONCURENT_STREAMS: usize = 100;
const CONNECTION_TIMEOUT: u64 = 1000;

#[derive(NetworkBehaviour)]
pub struct CalfBehavior {
    identify: identify::Behaviour,
    mdns: mdns::tokio::Behaviour,
    request_response: request_response::cbor::Behaviour<RequestPayload, ()>,
}

pub enum Peer {
    Primary(PeerId, Multiaddr),
    Worker(PeerId, Multiaddr, u32),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum PeerIdentifyInfos {
    //ID, ValidatorPubkey
    Worker(u32, String),
    //ValidatorPubkey
    Primary(String),
}

pub trait ManagePeers {
    fn add_peer(&mut self, id: Peer, authority_pubkey: String) -> bool;
    fn remove_peer(&mut self, id: PeerId) -> bool;
    fn add_established(&mut self, id: PeerId, addr: Multiaddr);
    fn established(&self) -> &HashMap<PeerId, Multiaddr>;
    fn contains_peer(&self, id: PeerId) -> bool;
    fn identify(&self) -> PeerIdentifyInfos;
    fn get_broadcast_peers_counterparts(&self) -> HashSet<(PeerId, Multiaddr)>;
    fn get_broadcast_peers_same_node(&self) -> HashSet<(PeerId, Multiaddr)>;
    fn get_send_peer(&self, id: PeerId) -> Option<(PeerId, Multiaddr)>;
    fn get_to_dial_peers(&self, committee: &Committee) -> Vec<(PeerId, Multiaddr)>;
}

#[async_trait]
pub trait Connect {
    async fn dispatch(&self, payload: &RequestPayload, sender: PeerId) -> anyhow::Result<()>;
}

#[async_trait]
pub trait HandleEvent<P, C>
where
    P: ManagePeers + Send + Sync,
    C: Connect + Send,
{
    async fn handle_request(
        swarm: &mut Swarm<CalfBehavior>,
        request: NetworkRequest,
        peers: Arc<RwLock<P>>,
    ) -> anyhow::Result<()>;
}

pub(crate) struct Network<A, C, P>
where
    C: Connect + Send,
    P: ManagePeers + Send + Sync,
    A: HandleEvent<P, C>,
{
    _committee: Committee,
    swarm: libp2p::Swarm<CalfBehavior>,
    peers: Arc<RwLock<P>>,
    connector: C,
    requests_rx: mpsc::Receiver<NetworkRequest>,
    _authority_keypair: ed25519::Keypair,
    _keypair: ed25519::Keypair,
    _role: PhantomData<A>,
}

impl<A, C, P> Network<A, C, P>
where
    C: Connect + Send + 'static,
    P: ManagePeers + Send + Sync + 'static,
    A: HandleEvent<P, C> + Send,
{
    pub fn spawn(
        _committee: Committee,
        connector: C,
        authority_keypair: ed25519::Keypair,
        keypair: ed25519::Keypair,
        peers: Arc<RwLock<P>>,
        requests_rx: mpsc::Receiver<NetworkRequest>,
        cancellation_token: CancellationToken,
    ) -> tokio::task::JoinHandle<()> {
        let token_clone = cancellation_token.clone();
        tokio::spawn(async move {
            let identify_infos = serde_json::to_string(&peers.read().await.identify());
            if identify_infos.is_err() {
                token_clone.cancel();
            }
            //safe unwrap, checked --^
            let identify_infos = identify_infos.unwrap();
            let keypair_lib = Keypair::from(keypair.clone());
            let mdns = match mdns::tokio::Behaviour::new(
                mdns::Config::default(),
                keypair_lib.public().to_peer_id(),
            ) {
                Ok(mdns) => mdns,
                Err(e) => {
                    tracing::error!("failed to create mdns behaviour: exiting {e}");
                    cancellation_token.cancel();
                    return;
                }
            };

            let identify_config = identify::Config::new(MAIN_PROTOCOL.into(), keypair_lib.public())
                .with_agent_version(identify_infos)
                .with_push_listen_addr_updates(true);

            let swarm = libp2p::SwarmBuilder::with_existing_identity(keypair_lib.clone())
                .with_tokio()
                .with_quic()
                .with_behaviour(|_| CalfBehavior {
                    identify: identify::Behaviour::new(identify_config),
                    mdns,
                    request_response: {
                        let cfg = request_response::Config::default()
                            .with_max_concurrent_streams(MAX_CONCURENT_STREAMS);

                        request_response::cbor::Behaviour::<RequestPayload, ()>::new(
                            [(StreamProtocol::new(MAIN_PROTOCOL), ProtocolSupport::Full)],
                            cfg,
                        )
                    },
                })
                .unwrap()
                .with_swarm_config(|c| {
                    c.with_idle_connection_timeout(Duration::from_secs(CONNECTION_TIMEOUT))
                })
                .build();

            let mut this = Self {
                _committee,
                swarm,
                peers,
                connector,
                requests_rx,
                _authority_keypair: authority_keypair,
                _keypair: keypair,
                _role: PhantomData,
            };
            let run = this.run();
            let res = cancellation_token.run_until_cancelled(run).await;

            match res {
                Some(res) => {
                    match res {
                        Ok(_) => {
                            tracing::info!("network finished successfully");
                        }
                        Err(e) => {
                            tracing::error!("network finished with an error: {:#?}", e);
                        }
                    };
                    cancellation_token.cancel();
                }
                None => {
                    tracing::info!("network has been cancelled");
                }
            }
        })
    }

    async fn run(&mut self) -> anyhow::Result<()> {
        self.swarm
            .listen_on("/ip4/0.0.0.0/udp/0/quic-v1".parse()?)?;

        loop {
            tokio::select! {
                event = self.swarm.select_next_some() => {
                    swarm_events::handle_event(event, &mut self.swarm, self.peers.clone(), &mut self.connector).await?;
                },
                Some(message) = self.requests_rx.recv() => {
                    A::handle_request(&mut self.swarm, message, self.peers.clone()).await?;
                }
            }
        }
    }
}
