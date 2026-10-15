use std::net::{Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

use protocol::{read_frame, write_frame, ClientMessage, NatInfo, NatType, PeerId, ServerMessage};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::time::timeout;

const WAIT: Duration = Duration::from_secs(5);

/// Set to a `host:port` to run these tests against an already-running server
/// (a local container, or the Railway TCP proxy) instead of an in-process one.
const EXTERNAL_ADDR_VAR: &str = "RENDEZVOUS_TEST_ADDR";

/// Starts (or locates) a server and returns its address with a token
/// namespace unique to the calling test.
async fn start_server() -> (SocketAddr, Tokens) {
    let tokens = Tokens::new();
    if let Ok(target) = std::env::var(EXTERNAL_ADDR_VAR) {
        let addr = tokio::net::lookup_host(&target)
            .await
            .unwrap_or_else(|e| panic!("{EXTERNAL_ADDR_VAR}={target}: {e}"))
            .next()
            .unwrap_or_else(|| panic!("{EXTERNAL_ADDR_VAR}={target}: no addresses"));
        return (addr, tokens);
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(rendezvous_server::serve(listener));
    (addr, tokens)
}

/// Token namespace for one test. Tests sharing an external server, within a
/// run or across runs, never land in each other's groups.
struct Tokens(String);

impl Tokens {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let run_id = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        Self(format!("{run_id}-{}", NEXT.fetch_add(1, Ordering::Relaxed)))
    }

    fn get(&self, name: &str) -> String {
        format!("{name}-{}", self.0)
    }
}

struct Client {
    stream: TcpStream,
}

impl Client {
    async fn connect(addr: SocketAddr) -> Self {
        Self {
            stream: TcpStream::connect(addr).await.unwrap(),
        }
    }

    /// Registers and returns this client's id, consuming the `Registered`
    /// reply and the first peer list.
    async fn register(addr: SocketAddr, token: &str) -> (Self, PeerId, Vec<PeerId>) {
        let mut client = Self::connect(addr).await;
        client
            .send(&ClientMessage::Register {
                token: token.into(),
            })
            .await;
        let ServerMessage::Registered { peer_id } = client.recv().await else {
            panic!("expected Registered");
        };
        let peers = client.recv_peer_list().await;
        (client, peer_id, peers)
    }

    async fn send(&mut self, msg: &ClientMessage) {
        write_frame(&mut self.stream, msg).await.unwrap();
    }

    async fn recv(&mut self) -> ServerMessage {
        timeout(WAIT, read_frame(&mut self.stream))
            .await
            .expect("timed out waiting for server")
            .unwrap()
            .expect("server closed the connection")
    }

    async fn recv_peer_list(&mut self) -> Vec<PeerId> {
        match self.recv().await {
            ServerMessage::PeerList { mut peers } => {
                peers.sort();
                peers
            }
            other => panic!("expected PeerList, got {other:?}"),
        }
    }

    /// Asserts that nothing arrives for a short while.
    async fn expect_silence(&mut self) {
        let res = timeout(
            Duration::from_millis(200),
            read_frame::<_, ServerMessage>(&mut self.stream),
        )
        .await;
        assert!(res.is_err(), "unexpected message: {res:?}");
    }

    /// Asserts that the server closes the connection.
    async fn expect_closed(&mut self) {
        let res = timeout(WAIT, read_frame::<_, ServerMessage>(&mut self.stream))
            .await
            .expect("server did not close the connection");
        assert!(
            matches!(res, Ok(None) | Err(_)),
            "expected close, got {res:?}"
        );
    }
}

fn nat_info(last_octet: u8) -> NatInfo {
    NatInfo {
        nat_type: NatType::Cone,
        public_ips: vec![Ipv4Addr::new(203, 0, 113, last_octet)],
        public_udp_ports: vec![40000 + last_octet as u16],
        mapping_tcp_addr: vec![],
        mapping_udp_addr: vec![],
        public_port_range: 0,
        local_ipv4: Ipv4Addr::new(192, 168, 1, last_octet),
        local_ipv4s: vec![],
        ipv6: None,
        local_udp_ports: vec![50000],
        local_tcp_port: 0,
        public_tcp_port: 0,
    }
}

#[tokio::test]
async fn two_peers_exchange_nat_info() {
    let (addr, tokens) = start_server().await;

    let (mut a, a_id, a_peers) = Client::register(addr, &tokens.get("share-1")).await;
    assert!(a_peers.is_empty());

    let (mut b, b_id, b_peers) = Client::register(addr, &tokens.get("share-1")).await;
    assert_eq!(b_peers, vec![a_id]);
    assert_eq!(a.recv_peer_list().await, vec![b_id]);

    b.send(&ClientMessage::PunchStart {
        to: a_id,
        nat_info: nat_info(2),
    })
    .await;
    match a.recv().await {
        ServerMessage::PunchStart {
            from,
            nat_info: info,
        } => {
            assert_eq!(from, b_id);
            assert_eq!(format!("{info:?}"), format!("{:?}", nat_info(2)));
        }
        other => panic!("expected PunchStart, got {other:?}"),
    }

    a.send(&ClientMessage::PunchReply {
        to: b_id,
        nat_info: nat_info(1),
    })
    .await;
    match b.recv().await {
        ServerMessage::PunchReply {
            from,
            nat_info: info,
        } => {
            assert_eq!(from, a_id);
            assert_eq!(format!("{info:?}"), format!("{:?}", nat_info(1)));
        }
        other => panic!("expected PunchReply, got {other:?}"),
    }
}

#[tokio::test]
async fn tokens_are_isolated() {
    let (addr, tokens) = start_server().await;
    let (mut a, a_id, _) = Client::register(addr, &tokens.get("share-1")).await;
    let (mut c, _, c_peers) = Client::register(addr, &tokens.get("share-2")).await;

    assert!(c_peers.is_empty(), "peer lists must not cross tokens");
    a.expect_silence().await;

    c.send(&ClientMessage::PunchStart {
        to: a_id,
        nat_info: nat_info(3),
    })
    .await;
    assert!(matches!(c.recv().await, ServerMessage::Error { .. }));
    a.expect_silence().await;
}

#[tokio::test]
async fn peer_list_updates_when_a_peer_leaves() {
    let (addr, tokens) = start_server().await;
    let (mut a, _, _) = Client::register(addr, &tokens.get("share-1")).await;
    let (b, b_id, _) = Client::register(addr, &tokens.get("share-1")).await;
    assert_eq!(a.recv_peer_list().await, vec![b_id]);

    drop(b);
    assert!(a.recv_peer_list().await.is_empty());
}

#[tokio::test]
async fn ping_gets_pong() {
    let (addr, tokens) = start_server().await;
    let (mut a, _, _) = Client::register(addr, &tokens.get("share-1")).await;
    a.send(&ClientMessage::Ping).await;
    assert!(matches!(a.recv().await, ServerMessage::Pong));
}

#[tokio::test]
async fn first_message_must_be_register() {
    let (addr, _) = start_server().await;
    let mut client = Client::connect(addr).await;
    client.send(&ClientMessage::Ping).await;
    assert!(matches!(client.recv().await, ServerMessage::Error { .. }));
    client.expect_closed().await;
}

#[tokio::test]
async fn invalid_token_is_rejected() {
    let (addr, _) = start_server().await;
    let mut client = Client::connect(addr).await;
    client
        .send(&ClientMessage::Register {
            token: String::new(),
        })
        .await;
    assert!(matches!(client.recv().await, ServerMessage::Error { .. }));
    client.expect_closed().await;
}

#[tokio::test]
async fn malformed_frame_closes_connection() {
    let (addr, tokens) = start_server().await;
    let (mut a, _, _) = Client::register(addr, &tokens.get("share-1")).await;
    // A well-formed frame whose body is not a valid ClientMessage.
    a.stream.write_u32(3).await.unwrap();
    a.stream.write_all(&[0xff, 0xff, 0xff]).await.unwrap();
    a.expect_closed().await;
}

#[tokio::test]
async fn duplicate_register_closes_connection() {
    let (addr, tokens) = start_server().await;
    let (mut a, _, _) = Client::register(addr, &tokens.get("share-1")).await;
    a.send(&ClientMessage::Register {
        token: tokens.get("share-1"),
    })
    .await;
    assert!(matches!(a.recv().await, ServerMessage::Error { .. }));
    a.expect_closed().await;
}
