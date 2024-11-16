use std::time::Duration;

use futures::StreamExt as _;
use futures_util::future::try_join_all;
use libp2p::{
    core::multiaddr::Multiaddr,
    identify::{self, Behaviour as IdentifyBehaviour},
    noise,
    swarm::SwarmEvent,
    tcp, yamux,
};
use tokio::task::JoinHandle;
use tracing::Instrument;

pub(crate) struct Network {}

impl Network {
    pub fn new() -> Self {
        Self {}
    }

    pub fn spawn(self) -> JoinHandle<()> {
        tokio::spawn(
            self.run()
                .instrument(tracing::info_span!("batch_broadcaster")),
        )
    }

    pub async fn run(self) {
        let mut swarm = libp2p::SwarmBuilder::with_new_identity()
            .with_tokio()
            .with_tcp(
                tcp::Config::default(),
                noise::Config::new,
                yamux::Config::default,
            )
            .unwrap()
            .with_behaviour(|key| {
                identify::Behaviour::new(identify::Config::new(
                    "/ipfs/id/1.0.0".to_string(),
                    key.public(),
                ))
            })
            .unwrap()
            .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(60)))
            .build();

        // Tell the swarm to listen on all interfaces and a random, OS-assigned
        // port.
        swarm
            .listen_on("/ip4/0.0.0.0/tcp/0".parse().unwrap())
            .expect("Failed to bind");

        // Dial the peer identified by the multi-address given as the second
        // command-line argument, if any.
        if let Some(addr) = std::env::args().nth(1) {
            let remote: Multiaddr = addr.parse().expect("Failed to parse multiaddr");
            swarm.dial(remote).expect("Failed to dial");
            println!("Dialed {addr}")
        }

        let tasks = vec![tokio::spawn(network_task(swarm))];

        if let Err(e) = try_join_all(tasks).await {
            tracing::error!("Error in BatchBroadcaster: {:?}", e);
        }
    }
}

#[tracing::instrument(skip_all)]
async fn network_task(mut swarm: libp2p::Swarm<IdentifyBehaviour>) {
    loop {
        match swarm.select_next_some().await {
            SwarmEvent::NewListenAddr { address, .. } => {
                println!("Listening on {address:?}")
            }
            // Prints peer id identify info is being sent to.
            SwarmEvent::Behaviour(identify::Event::Sent { peer_id, .. }) => {
                println!("Sent identify info to {peer_id:?}")
            }
            // Prints out the info received via the identify event
            SwarmEvent::Behaviour(identify::Event::Received { info, .. }) => {
                println!("Received {info:?}")
            }
            _ => {}
        }
    }
}
