//! Direct-only connection: hole punching toward the peer's candidates, then a
//! request/response round trip over the punched socket. There is no relay
//! fallback: if no direct path opens in time, we give up.

use std::fmt;
use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use protocol::{decode_peer, encode_peer, NatInfo, PeerId, PeerMessage};
use rust_p2p_core::punch::{PunchInfo, PunchModel};
use tokio::task::AbortHandle;
use tokio::time::{sleep, sleep_until, Instant};

use crate::endpoint::Endpoint;
use crate::stun::Transport;

#[derive(Clone, Copy, Debug)]
pub struct PunchConfig {
    /// Give up if the round trip hasn't completed both ways by then.
    pub timeout: Duration,
    /// Pause between rounds of punch packets.
    pub interval: Duration,
    /// After connecting, keep answering the peer's requests this long, in
    /// case our last response to it was lost.
    pub linger: Duration,
}

impl PunchConfig {
    pub const DEFAULT: Self = Self {
        timeout: Duration::from_secs(15),
        interval: Duration::from_millis(500),
        linger: Duration::from_secs(1),
    };
}

#[derive(Debug)]
pub enum PunchError {
    /// Both NATs pick a new public port per destination, so neither side can
    /// predict where to send. Detected up front instead of timing out.
    BothSymmetric,
    TimedOut(Duration),
    Io(io::Error),
}

impl fmt::Display for PunchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BothSymmetric => write!(
                f,
                "direct connection not possible: both peers are behind symmetric NATs \
                 (no relay fallback)"
            ),
            Self::TimedOut(after) => write!(
                f,
                "no direct path to the peer within {after:?}; giving up (no relay fallback)"
            ),
            Self::Io(e) => write!(f, "punching socket: {e}"),
        }
    }
}

impl std::error::Error for PunchError {}

impl From<io::Error> for PunchError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

#[derive(Debug)]
pub struct Connected {
    /// Where the peer answered from: its end of the punched path.
    pub peer_addr: SocketAddr,
    /// From the first punch until the round trip had worked both ways.
    pub elapsed: Duration,
}

/// Punches toward `peer_nat` and returns once a `PunchRequest`/`PunchResponse`
/// round trip has succeeded in both directions: the peer answered our
/// request, and we answered one of theirs. Each peer needs the other to have
/// answered it, so stopping after only our own round trip could strand the
/// peer.
///
/// Call it on both peers at about the same time. Needs a multi-threaded
/// runtime: `rust-p2p-core`'s puncher blocks a worker thread in places.
pub async fn punch(
    endpoint: &mut Endpoint,
    my_id: PeerId,
    peer_id: PeerId,
    my_nat: &NatInfo,
    peer_nat: &NatInfo,
    config: PunchConfig,
) -> Result<Connected, PunchError> {
    if my_nat.nat_type.is_symmetric() && peer_nat.nat_type.is_symmetric() {
        return Err(PunchError::BothSymmetric);
    }
    let nonce: u64 = rand::random();
    let started = Instant::now();
    let deadline = started + config.timeout;

    // Spray requests at the peer's candidates until it answers. The puncher
    // sends from the endpoint's socket, the one our candidates describe.
    let sprayer = {
        let puncher = endpoint.puncher.clone();
        let request = encode_peer(&PeerMessage::PunchRequest { from: my_id, nonce });
        let info = PunchInfo::new(PunchModel::all(), peer_nat.clone());
        tokio::spawn(async move {
            loop {
                if let Err(e) = puncher.punch_now(None, &request, info.clone()).await {
                    log::warn!("punch: {e}");
                }
                sleep(config.interval).await;
            }
        })
    };
    let _stop_spraying = AbortOnDrop(sprayer.abort_handle());

    let mut answered_by: Option<SocketAddr> = None;
    let mut answered_them = false;
    let mut connected: Option<(Connected, Instant)> = None;
    let mut buf = [0u8; 1500];
    loop {
        let wake = connected.as_ref().map_or(deadline, |(_, until)| *until);
        let (len, from) = tokio::select! {
            received = Transport::recv_from(&mut endpoint.tunnel, &mut buf) => received?,
            _ = sleep_until(wake) => {
                return match connected {
                    Some((done, _)) => Ok(done),
                    None => Err(PunchError::TimedOut(config.timeout)),
                };
            }
        };
        match decode_peer(&buf[..len]) {
            Some(PeerMessage::PunchRequest { from: id, nonce }) if id == peer_id => {
                let response = encode_peer(&PeerMessage::PunchResponse { from: my_id, nonce });
                endpoint.tunnel.send_to(&response, from).await?;
                if !answered_them {
                    log::info!("peer {peer_id} reached us from {from}");
                    answered_them = true;
                }
            }
            Some(PeerMessage::PunchResponse {
                from: id,
                nonce: echoed,
            }) if id == peer_id && echoed == nonce => {
                if answered_by.is_none() {
                    log::info!("peer {peer_id} answered from {from}");
                    answered_by = Some(from);
                    sprayer.abort();
                }
            }
            Some(other) => log::debug!("ignoring {other:?} from {from}"),
            None => log::trace!("ignoring {len}-byte datagram from {from}"),
        }
        if let (None, Some(peer_addr), true) = (&connected, answered_by, answered_them) {
            let done = Connected {
                peer_addr,
                elapsed: started.elapsed(),
            };
            connected = Some((done, Instant::now() + config.linger));
        }
    }
}

struct AbortOnDrop(AbortHandle);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}
