//! A punched connection to one peer: test payloads, acknowledgements and
//! keepalives over the endpoint's UDP socket.

use std::collections::HashSet;
use std::fmt;
use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use protocol::{
    decode_peer, encode_peer, ClientMessage, PeerId, PeerMessage, ServerMessage, MAX_NAME_LEN,
    MAX_PAYLOAD_LEN,
};
use rust_p2p_core::tunnel::udp::UdpTunnel;
use sha2::{Digest, Sha256};
use tokio::time::{interval_at, sleep_until, Instant};

use crate::endpoint::Endpoint;
use crate::exchange::PING_INTERVAL;
use crate::rendezvous::Rendezvous;
use crate::stun::Transport;

#[derive(Clone, Copy, Debug)]
pub struct SessionConfig {
    /// How often to send a `Keepalive` while waiting. NATs commonly drop idle
    /// UDP mappings after 30s or more; some after as little as 20s.
    pub keepalive: Duration,
    /// The peer counts as gone after this long without a datagram from it.
    pub peer_timeout: Duration,
    /// Wait for an acknowledgement before resending a payload.
    pub retransmit: Duration,
    /// Payload sends before giving up.
    pub attempts: u32,
}

impl SessionConfig {
    pub const DEFAULT: Self = Self {
        keepalive: Duration::from_secs(10),
        peer_timeout: Duration::from_secs(45),
        retransmit: Duration::from_millis(500),
        attempts: 10,
    };
}

#[derive(Debug)]
pub enum SessionError {
    TooLarge(&'static str),
    /// No acknowledgement after every retransmission.
    NoAck,
    /// The peer acknowledged the payload but its hash didn't match.
    Corrupted,
    /// Nothing heard from the peer for `peer_timeout`.
    PeerGone,
    Io(io::Error),
}

impl fmt::Display for SessionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooLarge(what) => write!(f, "{what}"),
            Self::NoAck => write!(f, "peer never acknowledged the payload"),
            Self::Corrupted => write!(f, "peer reports the payload arrived corrupted"),
            Self::PeerGone => write!(f, "peer went silent"),
            Self::Io(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for SessionError {}

impl From<io::Error> for SessionError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

#[derive(Debug)]
pub struct Payload {
    /// Set for a file, unset for a text message.
    pub name: Option<String>,
    pub data: Vec<u8>,
    pub sha256: [u8; 32],
}

pub fn sha256(data: &[u8]) -> [u8; 32] {
    Sha256::digest(data).into()
}

pub struct Session<'a> {
    endpoint: &'a mut Endpoint,
    /// Pinged while we wait, so we stay registered for later peers. `None`
    /// when there is no rendezvous connection to keep (tests).
    rendezvous: Option<Rendezvous>,
    my_id: PeerId,
    peer_id: PeerId,
    peer_addr: SocketAddr,
    config: SessionConfig,
    next_seq: u64,
    /// Payload seqs already delivered, so retransmissions aren't delivered
    /// twice.
    delivered: HashSet<u64>,
    last_heard: Instant,
}

impl<'a> Session<'a> {
    pub fn new(
        endpoint: &'a mut Endpoint,
        rendezvous: Option<Rendezvous>,
        my_id: PeerId,
        peer_id: PeerId,
        peer_addr: SocketAddr,
        config: SessionConfig,
    ) -> Self {
        Self {
            endpoint,
            rendezvous,
            my_id,
            peer_id,
            peer_addr,
            config,
            next_seq: 0,
            delivered: HashSet::new(),
            last_heard: Instant::now(),
        }
    }

    /// Sends one payload and waits for the peer to acknowledge it intact.
    pub async fn send_payload(
        &mut self,
        name: Option<String>,
        data: Vec<u8>,
    ) -> Result<(), SessionError> {
        if data.len() > MAX_PAYLOAD_LEN {
            return Err(SessionError::TooLarge("payload is over 1024 bytes"));
        }
        if name.as_ref().is_some_and(|n| n.len() > MAX_NAME_LEN) {
            return Err(SessionError::TooLarge("file name is over 100 bytes"));
        }
        let seq = self.next_seq;
        self.next_seq += 1;
        let payload = encode_peer(&PeerMessage::Payload {
            from: self.my_id,
            seq,
            name,
            sha256: sha256(&data),
            data,
        });

        let mut buf = [0u8; 1500];
        for _ in 0..self.config.attempts {
            self.endpoint
                .tunnel
                .send_to(&payload, self.peer_addr)
                .await?;
            let resend_at = Instant::now() + self.config.retransmit;
            loop {
                let msg = tokio::select! {
                    msg = self.recv_from_peer(&mut buf) => msg?,
                    _ = sleep_until(resend_at) => break,
                };
                match msg {
                    PeerMessage::PayloadAck {
                        seq: acked, intact, ..
                    } if acked == seq => {
                        return if intact {
                            Ok(())
                        } else {
                            Err(SessionError::Corrupted)
                        };
                    }
                    other => self.handle_common(other).await?,
                }
            }
        }
        Err(SessionError::NoAck)
    }

    /// Waits for the peer's next payload, acknowledging it. Returns `None`
    /// once the peer says `Bye`. Sends keepalives meanwhile.
    pub async fn recv_payload(&mut self) -> Result<Option<Payload>, SessionError> {
        let mut keepalive = interval_at(Instant::now(), self.config.keepalive);
        let mut ping = interval_at(Instant::now() + PING_INTERVAL, PING_INTERVAL);
        let mut buf = [0u8; 1500];
        loop {
            let gone_at = self.last_heard + self.config.peer_timeout;
            let msg = tokio::select! {
                msg = recv_from_peer(
                    &mut self.endpoint.tunnel,
                    self.peer_addr,
                    self.peer_id,
                    &mut self.last_heard,
                    &mut buf,
                ) => msg?,
                _ = keepalive.tick() => {
                    self.send(&PeerMessage::Keepalive { from: self.my_id }).await?;
                    continue;
                }
                _ = ping.tick(), if self.rendezvous.is_some() => {
                    self.rendezvous.as_mut().unwrap().send(&ClientMessage::Ping).await?;
                    continue;
                }
                msg = recv_rendezvous(&mut self.rendezvous) => {
                    log_while_busy(msg?, self.peer_id);
                    continue;
                }
                _ = sleep_until(gone_at) => return Err(SessionError::PeerGone),
            };
            match msg {
                PeerMessage::Payload {
                    seq,
                    name,
                    data,
                    sha256: claimed,
                    ..
                } => {
                    let intact = sha256(&data) == claimed;
                    let ack = PeerMessage::PayloadAck {
                        from: self.my_id,
                        seq,
                        intact,
                    };
                    self.send(&ack).await?;
                    if intact && self.delivered.insert(seq) {
                        return Ok(Some(Payload {
                            name,
                            data,
                            sha256: claimed,
                        }));
                    }
                    if !intact {
                        log::warn!("payload {seq} from peer {} failed its hash", self.peer_id);
                    }
                }
                PeerMessage::Bye { .. } => return Ok(None),
                other => self.handle_common(other).await?,
            }
        }
    }

    /// Tells the peer we're done and hands back the rendezvous connection.
    /// `Bye` isn't acknowledged, so it goes out a few times in case of loss;
    /// a peer that misses all of them notices via `peer_timeout`.
    pub async fn close(mut self) -> io::Result<Option<Rendezvous>> {
        for _ in 0..3 {
            self.send(&PeerMessage::Bye { from: self.my_id }).await?;
        }
        Ok(self.rendezvous.take())
    }

    async fn send(&mut self, msg: &PeerMessage) -> io::Result<()> {
        self.endpoint
            .tunnel
            .send_to(&encode_peer(msg), self.peer_addr)
            .await
    }

    async fn recv_from_peer(&mut self, buf: &mut [u8]) -> io::Result<PeerMessage> {
        recv_from_peer(
            &mut self.endpoint.tunnel,
            self.peer_addr,
            self.peer_id,
            &mut self.last_heard,
            buf,
        )
        .await
    }

    /// Messages that can arrive at any point in a session.
    async fn handle_common(&mut self, msg: PeerMessage) -> io::Result<()> {
        match msg {
            // The peer is still confirming the punch: our earlier response
            // may have been lost.
            PeerMessage::PunchRequest { nonce, .. } => {
                let response = PeerMessage::PunchResponse {
                    from: self.my_id,
                    nonce,
                };
                self.send(&response).await
            }
            PeerMessage::Keepalive { .. } | PeerMessage::PunchResponse { .. } => Ok(()),
            other => {
                log::debug!("ignoring {other:?} from peer {}", self.peer_id);
                Ok(())
            }
        }
    }
}

/// The next message from the peer: the right id, from the punched address.
/// Everything else on the socket is dropped. A free function borrowing only
/// what it needs, so it can run alongside the rendezvous connection.
async fn recv_from_peer(
    tunnel: &mut UdpTunnel,
    peer_addr: SocketAddr,
    peer_id: PeerId,
    last_heard: &mut Instant,
    buf: &mut [u8],
) -> io::Result<PeerMessage> {
    loop {
        let (len, from) = Transport::recv_from(tunnel, buf).await?;
        match decode_peer(&buf[..len]) {
            Some(msg) if from == peer_addr && sender(&msg) == peer_id => {
                *last_heard = Instant::now();
                return Ok(msg);
            }
            Some(msg) => log::debug!("ignoring {msg:?} from {from}"),
            None => log::trace!("ignoring {len}-byte datagram from {from}"),
        }
    }
}

/// Waits for a rendezvous message, or forever without a connection.
async fn recv_rendezvous(rendezvous: &mut Option<Rendezvous>) -> io::Result<ServerMessage> {
    match rendezvous {
        Some(rendezvous) => rendezvous.recv().await,
        None => std::future::pending().await,
    }
}

fn log_while_busy(msg: ServerMessage, peer_id: PeerId) {
    match msg {
        ServerMessage::PeerList { peers } => log::info!("peers under this token: {peers:?}"),
        ServerMessage::PunchStart { from, .. } => {
            log::warn!("ignoring offer from peer {from}: busy with peer {peer_id}")
        }
        ServerMessage::Error { reason } => log::warn!("rendezvous server: {reason}"),
        other => log::debug!("rendezvous: {other:?}"),
    }
}

fn sender(msg: &PeerMessage) -> PeerId {
    match *msg {
        PeerMessage::PunchRequest { from, .. }
        | PeerMessage::PunchResponse { from, .. }
        | PeerMessage::Payload { from, .. }
        | PeerMessage::PayloadAck { from, .. }
        | PeerMessage::Keepalive { from }
        | PeerMessage::Bye { from } => from,
    }
}
