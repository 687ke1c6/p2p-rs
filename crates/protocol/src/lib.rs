//! Wire protocol shared by the client and the rendezvous server.
//!
//! Messages are typed by direction: the server only ever decodes
//! [`ClientMessage`], so anything outside that allowlist fails to decode and
//! the connection is dropped. No variant carries an arbitrary payload for the
//! server to forward, which is what keeps the server from being usable as a
//! data relay.
//!
//! Rendezvous traffic is TCP, framed as a big-endian `u32` length followed by
//! a `postcard`-encoded message. Peer-to-peer [`PeerMessage`]s travel as single
//! UDP datagrams with no length prefix.

use std::io;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub use rust_p2p_core::nat::{NatInfo, NatType};

/// Server-assigned identifier for a registered peer, unique per server run.
pub type PeerId = u64;

/// Largest accepted frame. A `NatInfo` is well under 1 KiB, so this is generous.
pub const MAX_FRAME_LEN: usize = 64 * 1024;

/// Longest accepted share/device token, in bytes.
pub const MAX_TOKEN_LEN: usize = 128;

/// Client to rendezvous server.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClientMessage {
    /// Join the group of peers sharing `token`. Must be the first message.
    Register { token: String },
    /// Open a punch with peer `to`, offering our own candidates.
    PunchStart { to: PeerId, nat_info: NatInfo },
    /// Answer a `PunchStart` from peer `to` with our own candidates.
    PunchReply { to: PeerId, nat_info: NatInfo },
    /// Keepalive. Proxies such as Railway's close idle connections.
    Ping,
}

/// Rendezvous server to client.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ServerMessage {
    /// Registration accepted.
    Registered {
        peer_id: PeerId,
    },
    /// The other peers currently registered under the same token. Sent after
    /// registering and again whenever the group changes.
    PeerList {
        peers: Vec<PeerId>,
    },
    /// Peer `from` wants to punch, and these are its candidates.
    PunchStart {
        from: PeerId,
        nat_info: NatInfo,
    },
    /// Peer `from` answered our `PunchStart` with its candidates.
    PunchReply {
        from: PeerId,
        nat_info: NatInfo,
    },
    Pong,
    Error {
        reason: String,
    },
}

/// Sent directly between peers over a punched UDP socket. The server never
/// sees these.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PeerMessage {
    PunchRequest { from: PeerId },
    PunchResponse { from: PeerId },
}

/// Encodes a message without framing, e.g. for a UDP datagram.
pub fn encode<T: Serialize>(msg: &T) -> io::Result<Vec<u8>> {
    postcard::to_allocvec(msg).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

/// Decodes a message produced by [`encode`].
pub fn decode<T: DeserializeOwned>(bytes: &[u8]) -> io::Result<T> {
    postcard::from_bytes(bytes).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

/// Writes one length-prefixed frame.
pub async fn write_frame<W, T>(writer: &mut W, msg: &T) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
    T: Serialize,
{
    let body = encode(msg)?;
    if body.len() > MAX_FRAME_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "frame too large",
        ));
    }
    writer.write_u32(body.len() as u32).await?;
    writer.write_all(&body).await?;
    writer.flush().await
}

/// Reads one length-prefixed frame. Returns `Ok(None)` if the stream closed
/// cleanly between frames.
pub async fn read_frame<R, T>(reader: &mut R) -> io::Result<Option<T>>
where
    R: AsyncRead + Unpin,
    T: DeserializeOwned,
{
    let len = match reader.read_u32().await {
        Ok(len) => len as usize,
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    };
    if len > MAX_FRAME_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "frame too large",
        ));
    }
    let mut body = vec![0; len];
    reader.read_exact(&mut body).await?;
    decode(&body).map(Some)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    fn sample_nat_info() -> NatInfo {
        NatInfo {
            nat_type: NatType::Symmetric,
            public_ips: vec![Ipv4Addr::new(203, 0, 113, 7)],
            public_udp_ports: vec![40001, 40002],
            mapping_tcp_addr: vec![],
            mapping_udp_addr: vec!["198.51.100.1:5000".parse().unwrap()],
            public_port_range: 12,
            local_ipv4: Ipv4Addr::new(192, 168, 1, 20),
            local_ipv4s: vec![Ipv4Addr::new(192, 168, 1, 20)],
            ipv6: Some("2001:db8::1".parse().unwrap()),
            local_udp_ports: vec![50000],
            local_tcp_port: 0,
            public_tcp_port: 0,
        }
    }

    // NatInfo has no PartialEq, so compare through Debug output.
    fn assert_round_trip<T: Serialize + DeserializeOwned + std::fmt::Debug>(msg: T) {
        let decoded: T = decode(&encode(&msg).unwrap()).unwrap();
        assert_eq!(format!("{decoded:?}"), format!("{msg:?}"));
    }

    #[test]
    fn client_messages_round_trip() {
        assert_round_trip(ClientMessage::Register {
            token: "share-abc".into(),
        });
        assert_round_trip(ClientMessage::PunchStart {
            to: 7,
            nat_info: sample_nat_info(),
        });
        assert_round_trip(ClientMessage::PunchReply {
            to: 7,
            nat_info: sample_nat_info(),
        });
        assert_round_trip(ClientMessage::Ping);
    }

    #[test]
    fn server_messages_round_trip() {
        assert_round_trip(ServerMessage::Registered { peer_id: 1 });
        assert_round_trip(ServerMessage::PeerList { peers: vec![2, 3] });
        assert_round_trip(ServerMessage::PunchStart {
            from: 2,
            nat_info: sample_nat_info(),
        });
        assert_round_trip(ServerMessage::PunchReply {
            from: 2,
            nat_info: sample_nat_info(),
        });
        assert_round_trip(ServerMessage::Pong);
        assert_round_trip(ServerMessage::Error {
            reason: "nope".into(),
        });
    }

    #[test]
    fn peer_messages_round_trip() {
        assert_round_trip(PeerMessage::PunchRequest { from: 1 });
        assert_round_trip(PeerMessage::PunchResponse { from: 2 });
    }

    #[test]
    fn garbage_is_rejected() {
        assert!(decode::<ClientMessage>(&[0xff, 0xff, 0xff]).is_err());
    }

    #[tokio::test]
    async fn frames_round_trip_over_a_stream() {
        let (mut a, mut b) = tokio::io::duplex(1024);
        write_frame(&mut a, &ClientMessage::Ping).await.unwrap();
        write_frame(&mut a, &ClientMessage::Register { token: "t".into() })
            .await
            .unwrap();
        drop(a);
        assert!(matches!(
            read_frame(&mut b).await.unwrap(),
            Some(ClientMessage::Ping)
        ));
        assert!(matches!(
            read_frame(&mut b).await.unwrap(),
            Some(ClientMessage::Register { token }) if token == "t"
        ));
        assert!(read_frame::<_, ClientMessage>(&mut b)
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn oversized_frame_is_rejected() {
        let (mut a, mut b) = tokio::io::duplex(1024);
        a.write_u32(MAX_FRAME_LEN as u32 + 1).await.unwrap();
        let err = read_frame::<_, ClientMessage>(&mut b).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }
}
