//! Candidate exchange: trading `NatInfo` with a peer through the rendezvous
//! server.

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use protocol::{ClientMessage, NatInfo, PeerId, ServerMessage};
use tokio::time::{interval_at, Instant};

use crate::endpoint::{describe, Endpoint};
use crate::rendezvous::Rendezvous;
use crate::stun::Retry;

/// Well inside the server's 60s idle timeout.
pub const PING_INTERVAL: Duration = Duration::from_secs(20);

pub struct Exchanged {
    pub peer_id: PeerId,
    /// The candidates we sent the peer.
    pub my_nat: NatInfo,
    /// The candidates the peer sent us.
    pub peer_nat: NatInfo,
    /// Still registered, so the peer stays visible to the server.
    pub rendezvous: Rendezvous,
}

/// Waits for a peer under the same token and trades candidates with the first
/// one that completes an exchange.
///
/// The newer peer (higher id) sends `PunchStart` to each older one, and the
/// older one answers with `PunchReply`, so two peers never both start. NAT
/// detection reruns right before our candidates are sent, so the mapping we
/// hand over is fresh even if we waited a long time for the peer.
pub async fn exchange(
    endpoint: &mut Endpoint,
    mut rendezvous: Rendezvous,
    my_id: PeerId,
    stun_servers: &[SocketAddr],
    retry: Retry,
) -> io::Result<Exchanged> {
    // What we offered each peer: the candidates that peer will punch toward.
    let mut offered: HashMap<PeerId, NatInfo> = HashMap::new();
    let mut ping = interval_at(Instant::now() + PING_INTERVAL, PING_INTERVAL);
    loop {
        let msg = tokio::select! {
            _ = ping.tick() => {
                rendezvous.send(&ClientMessage::Ping).await?;
                continue;
            }
            msg = rendezvous.recv() => msg?,
        };
        match msg {
            ServerMessage::PeerList { peers } => {
                log::info!("peers under this token: {peers:?}");
                for peer in peers {
                    if peer >= my_id || offered.contains_key(&peer) {
                        continue;
                    }
                    let nat_info = endpoint.detect_nat(stun_servers, retry).await?;
                    log::info!(
                        "offering candidates to peer {peer}: {}",
                        describe(&nat_info)
                    );
                    rendezvous
                        .send(&ClientMessage::PunchStart {
                            to: peer,
                            nat_info: nat_info.clone(),
                        })
                        .await?;
                    offered.insert(peer, nat_info);
                }
            }
            ServerMessage::PunchStart { from, nat_info } => {
                log::info!("peer {from} offered candidates: {}", describe(&nat_info));
                let my_nat = endpoint.detect_nat(stun_servers, retry).await?;
                rendezvous
                    .send(&ClientMessage::PunchReply {
                        to: from,
                        nat_info: my_nat.clone(),
                    })
                    .await?;
                log::info!("answered peer {from}: {}", describe(&my_nat));
                return Ok(Exchanged {
                    peer_id: from,
                    my_nat,
                    peer_nat: nat_info,
                    rendezvous,
                });
            }
            ServerMessage::PunchReply { from, nat_info } if offered.contains_key(&from) => {
                log::info!("peer {from} answered: {}", describe(&nat_info));
                return Ok(Exchanged {
                    peer_id: from,
                    my_nat: offered.remove(&from).unwrap(),
                    peer_nat: nat_info,
                    rendezvous,
                });
            }
            ServerMessage::PunchReply { from, .. } => {
                log::warn!("ignoring PunchReply from peer {from}: we made it no offer");
            }
            ServerMessage::Error { reason } => log::warn!("rendezvous server: {reason}"),
            ServerMessage::Pong => log::debug!("pong"),
            ServerMessage::Registered { .. } => log::warn!("ignoring repeated Registered"),
        }
    }
}
