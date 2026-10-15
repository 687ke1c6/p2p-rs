//! Rendezvous server: groups peers by share/device token and passes punch
//! candidates (`NatInfo`) between peers in the same group. It never forwards
//! anything else, and it never uses a connection's remote address as a
//! candidate, because behind a TCP proxy (e.g. Railway) that address belongs
//! to the proxy, not the peer.

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use protocol::{read_frame, write_frame, ClientMessage, PeerId, ServerMessage, MAX_TOKEN_LEN};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::time::timeout;

/// A connection that sends nothing for this long is dropped. Clients should
/// `Ping` well within it.
pub const IDLE_TIMEOUT: Duration = Duration::from_secs(60);

/// Outgoing messages queued per peer. A peer that falls this far behind
/// starts losing messages rather than growing server memory.
const OUTBOX_LEN: usize = 32;

type Outbox = mpsc::Sender<ServerMessage>;

#[derive(Default)]
struct Registry {
    next_id: AtomicU64,
    groups: Mutex<HashMap<String, HashMap<PeerId, Outbox>>>,
}

impl Registry {
    fn register(&self, token: &str, outbox: Outbox) -> PeerId {
        let peer_id = self.next_id.fetch_add(1, Ordering::Relaxed) + 1;
        let mut groups = self.groups.lock().unwrap();
        groups
            .entry(token.to_owned())
            .or_default()
            .insert(peer_id, outbox);
        peer_id
    }

    fn unregister(&self, token: &str, peer_id: PeerId) {
        let mut groups = self.groups.lock().unwrap();
        if let Some(group) = groups.get_mut(token) {
            group.remove(&peer_id);
            if group.is_empty() {
                groups.remove(token);
            }
        }
    }

    /// Sends every member of the group the list of the other members.
    fn announce_peers(&self, token: &str) {
        let groups = self.groups.lock().unwrap();
        let Some(group) = groups.get(token) else {
            return;
        };
        for (&id, outbox) in group {
            let peers = group.keys().copied().filter(|&other| other != id).collect();
            deliver(id, outbox, ServerMessage::PeerList { peers });
        }
    }

    /// Delivers `msg` to peer `to` if it is in the same group. Returns false
    /// if there is no such peer, so tokens stay isolated from each other.
    fn send_within_group(&self, token: &str, to: PeerId, msg: ServerMessage) -> bool {
        let groups = self.groups.lock().unwrap();
        match groups.get(token).and_then(|group| group.get(&to)) {
            Some(outbox) => {
                deliver(to, outbox, msg);
                true
            }
            None => false,
        }
    }
}

fn deliver(peer_id: PeerId, outbox: &Outbox, msg: ServerMessage) {
    if outbox.try_send(msg).is_err() {
        log::warn!("peer {peer_id}: outbox full or closed, message dropped");
    }
}

/// Accepts connections until the listener fails.
pub async fn serve(listener: TcpListener) -> io::Result<()> {
    let registry = Arc::new(Registry::default());
    loop {
        let (stream, addr) = listener.accept().await?;
        let registry = registry.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_connection(&registry, stream, addr).await {
                log::info!("conn {addr}: closed with error: {e}");
            }
        });
    }
}

async fn handle_connection(
    registry: &Registry,
    stream: TcpStream,
    addr: SocketAddr,
) -> io::Result<()> {
    let (mut reader, mut writer) = stream.into_split();

    let token = match timeout(IDLE_TIMEOUT, read_frame(&mut reader)).await {
        Ok(Ok(Some(ClientMessage::Register { token }))) => token,
        Ok(Ok(Some(_))) => {
            let reason = "first message must be Register".to_owned();
            return write_frame(&mut writer, &ServerMessage::Error { reason }).await;
        }
        Ok(Ok(None)) => return Ok(()),
        Ok(Err(e)) => return Err(e),
        Err(_) => {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "no Register received",
            ))
        }
    };
    if token.is_empty() || token.len() > MAX_TOKEN_LEN {
        let reason = format!("token must be 1 to {MAX_TOKEN_LEN} bytes");
        return write_frame(&mut writer, &ServerMessage::Error { reason }).await;
    }

    let (outbox, mut inbox) = mpsc::channel(OUTBOX_LEN);
    let peer_id = registry.register(&token, outbox.clone());
    log::info!("peer {peer_id}: registered (conn {addr})");
    // Queued before the group announcement, so the client always learns its
    // own id before it sees a peer list.
    deliver(peer_id, &outbox, ServerMessage::Registered { peer_id });
    registry.announce_peers(&token);

    let writer_task = tokio::spawn(async move {
        while let Some(msg) = inbox.recv().await {
            if write_frame(&mut writer, &msg).await.is_err() {
                break;
            }
        }
    });

    let result = relay_signals(registry, &token, peer_id, &outbox, &mut reader).await;

    registry.unregister(&token, peer_id);
    registry.announce_peers(&token);
    log::info!("peer {peer_id}: unregistered");
    // Let queued replies (e.g. an Error) flush before the socket closes.
    drop(outbox);
    let _ = timeout(Duration::from_secs(1), writer_task).await;
    result
}

/// Handles a registered peer's messages until it disconnects or misbehaves.
async fn relay_signals(
    registry: &Registry,
    token: &str,
    peer_id: PeerId,
    outbox: &Outbox,
    reader: &mut tokio::net::tcp::OwnedReadHalf,
) -> io::Result<()> {
    loop {
        let msg = match timeout(IDLE_TIMEOUT, read_frame(reader)).await {
            Ok(Ok(Some(msg))) => msg,
            Ok(Ok(None)) => return Ok(()),
            Ok(Err(e)) => return Err(e),
            Err(_) => return Err(io::Error::new(io::ErrorKind::TimedOut, "idle timeout")),
        };
        match msg {
            ClientMessage::PunchStart { to, nat_info } => {
                log::info!("peer {peer_id}: punch start -> peer {to}");
                let msg = ServerMessage::PunchStart {
                    from: peer_id,
                    nat_info,
                };
                if !registry.send_within_group(token, to, msg) {
                    deliver(peer_id, outbox, unknown_peer(to));
                }
            }
            ClientMessage::PunchReply { to, nat_info } => {
                log::info!("peer {peer_id}: punch reply -> peer {to}");
                let msg = ServerMessage::PunchReply {
                    from: peer_id,
                    nat_info,
                };
                if !registry.send_within_group(token, to, msg) {
                    deliver(peer_id, outbox, unknown_peer(to));
                }
            }
            ClientMessage::Ping => deliver(peer_id, outbox, ServerMessage::Pong),
            ClientMessage::Register { .. } => {
                let reason = "already registered".to_owned();
                deliver(peer_id, outbox, ServerMessage::Error { reason });
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "duplicate Register",
                ));
            }
        }
    }
}

fn unknown_peer(peer_id: PeerId) -> ServerMessage {
    ServerMessage::Error {
        reason: format!("no peer {peer_id} under this token"),
    }
}
