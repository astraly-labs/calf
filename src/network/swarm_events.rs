use libp2p::{core::ConnectedPoint, identify, mdns, request_response, swarm::SwarmEvent};

use crate::network::{swarm_actions, Peer, PeerIdentifyInfos, MAIN_PROTOCOL};

use super::{CalfBehavior, CalfBehaviorEvent, Connect, ManagePeers};

pub async fn handle_event<P, C>(
    event: libp2p::swarm::SwarmEvent<CalfBehaviorEvent>,
    swarm: &mut libp2p::Swarm<CalfBehavior>,
    peers: &mut P,
    connector: &mut C,
) -> anyhow::Result<()>
where
    P: ManagePeers + Send,
    C: Connect + Send,
{
    match event {
        SwarmEvent::Behaviour(CalfBehaviorEvent::RequestResponse(
            request_response::Event::Message { peer, message },
        )) => {
            if let request_response::Message::Request { request, .. } = message {
                connector.dispatch(request, peer).await?;
            }
        }
        SwarmEvent::ConnectionClosed { peer_id, .. } => {
            peers.remove_peer(peer_id);
            tracing::info!("connection to {peer_id} closed");
        }
        SwarmEvent::ConnectionEstablished {
            peer_id, endpoint, ..
        } => {
            match endpoint {
                ConnectedPoint::Dialer { address, .. } => {
                    tracing::info!("dialed to {peer_id}: {address}");
                    peers.add_established(peer_id, address);
                }
                ConnectedPoint::Listener { send_back_addr, .. } => {
                    tracing::info!("received dial from {peer_id}: {send_back_addr}");
                }
            };
        }
        SwarmEvent::Behaviour(CalfBehaviorEvent::Identify(identify::Event::Received {
            peer_id,
            info,
        })) => {
            tracing::info!("received identify info from {peer_id}");
            if info.protocol_version != MAIN_PROTOCOL {
                tracing::info!("{peer_id} doesn't speak our protocol")
            }
            match serde_json::from_str::<PeerIdentifyInfos>(&info.agent_version) {
                Ok(infos) => match infos {
                    PeerIdentifyInfos::Worker(id, pubkey) => {
                        tracing::info!("Identified {peer_id} as worker {id}");
                        if let Some(addr) = peers.established().get(&peer_id) {
                            if peers.add_peer(Peer::Worker(peer_id, addr.clone(), id), pubkey) {
                                tracing::info!("worker added to peers");
                            }
                        }
                    }
                    PeerIdentifyInfos::Primary(authority_pubkey) => {
                        tracing::info!("Identified {peer_id} as primary {authority_pubkey}");
                        if let Some(addr) = peers.established().get(&peer_id) {
                            if peers
                                .add_peer(Peer::Primary(peer_id, addr.clone()), authority_pubkey)
                            {
                                tracing::info!("worker added to peers");
                            }
                        }
                    }
                },
                Err(_) => {
                    tracing::warn!("Failed to parse identify infos for {peer_id}, disconnecting");
                    swarm
                        .disconnect_peer_id(peer_id)
                        .map_err(|_| anyhow::anyhow!("failed to disconned from peer"))?;
                }
            }
        }
        SwarmEvent::Behaviour(CalfBehaviorEvent::Mdns(event)) => {
            if let mdns::Event::Discovered(list) = event {
                tracing::info!("Discovered peers: {:?}", list);
                let to_dial = list
                    .into_iter()
                    .filter(|(peer_id, _)| !peers.contains_peer(*peer_id));
                to_dial.for_each(|(id, addr)| {
                    //TODO: handle error ?
                    let _res = swarm_actions::dial_peer(swarm, id, addr);
                });
            }
        }
        _ => {}
    };
    Ok(())
}
