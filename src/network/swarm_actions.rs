use libp2p::{
    swarm::{
        dial_opts::{DialOpts, PeerCondition},
        DialError,
    },
    Multiaddr, PeerId, Swarm,
};

use crate::types::RequestPayload;

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
pub(crate) fn broadcast<P: ManagePeers + Send>(
    swarm: &mut Swarm<CalfBehavior>,
    peers: &P,
    message: RequestPayload,
) -> anyhow::Result<()> {
    let peers = peers.get_broadcast_peers();
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
    tracing::info!("Dialing -> {peer_id}");
    swarm.dial(dial_opts)
}
