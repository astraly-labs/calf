use std::{collections::HashSet, sync::Arc};

use libp2p::{
    swarm::{
        dial_opts::{DialOpts, PeerCondition},
        DialError,
    },
    Multiaddr, PeerId, Swarm,
};
use tokio::sync::RwLock;

use crate::types::network::RequestPayload;

use super::{CalfBehavior, ManagePeers};

/// Sends a message to a specific peer.
pub(crate) fn send(
    swarm: &mut Swarm<CalfBehavior>,
    peer_id: PeerId,
    message: RequestPayload,
) -> anyhow::Result<()> {
    swarm
        .behaviour_mut()
        .request_response
        .send_request(&peer_id, message);
    Ok(())
}

/// Broadcasts a message to all connected peers.
pub(crate) fn broadcast(
    //TODO: call get_bracast_peers_counterparts etc. in the swarm events instead of here: one broadcast for all cases
    swarm: &mut Swarm<CalfBehavior>,
    peers: HashSet<(PeerId, Multiaddr)>,
    message: RequestPayload,
) -> anyhow::Result<()> {
    for (id, _) in peers {
        send(swarm, id, message.clone())?;
    }
    Ok(())
}

pub(crate) fn dial_peer(
    swarm: &mut Swarm<CalfBehavior>,
    peer_id: PeerId,
    multiaddr: Multiaddr,
) -> Result<(), DialError> {
    let dial_opts = DialOpts::peer_id(peer_id)
        .condition(PeerCondition::DisconnectedAndNotDialing)
        .addresses(vec![multiaddr.clone()])
        .build();
    tracing::info!("trying to connect to {peer_id} / {multiaddr}");
    swarm.dial(dial_opts)
}
