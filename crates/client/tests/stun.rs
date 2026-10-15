//! `stun::query` and `stun::probe` against fake STUN servers on loopback:
//! real sockets, no internet.

use std::net::{SocketAddr, UdpSocket as StdUdpSocket};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use p2p_client::stun::{self, Retry, TransactionId};
use protocol::NatType;
use tokio::net::UdpSocket;

/// Short timeouts for tests that expect them to fire: gives up after 350ms.
const FAST: Retry = Retry {
    initial_rto: Duration::from_millis(50),
    attempts: 3,
};

struct FakeServer {
    addr: SocketAddr,
    /// Every datagram received, in order.
    requests: Arc<Mutex<Vec<Vec<u8>>>>,
}

impl FakeServer {
    /// Starts a server that calls `handler(n, request, from)` for the n-th
    /// datagram (0-based) and sends back whatever datagrams it returns.
    async fn start<F>(mut handler: F) -> Self
    where
        F: FnMut(usize, &[u8], SocketAddr) -> Vec<Vec<u8>> + Send + 'static,
    {
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let addr = socket.local_addr().unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let log = requests.clone();
        tokio::spawn(async move {
            let mut buf = [0; 1500];
            loop {
                let (len, from) = socket.recv_from(&mut buf).await.unwrap();
                let request = &buf[..len];
                let n = {
                    let mut log = log.lock().unwrap();
                    log.push(request.to_vec());
                    log.len() - 1
                };
                for reply in handler(n, request, from) {
                    socket.send_to(&reply, from).await.unwrap();
                }
            }
        });
        Self { addr, requests }
    }

    /// A well-behaved server: reports each request's source address.
    async fn echo() -> Self {
        Self::start(|_, request, from| vec![success(request, from)]).await
    }

    /// A server that receives requests but never answers.
    async fn silent() -> Self {
        Self::start(|_, _, _| vec![]).await
    }

    fn requests(&self) -> Vec<Vec<u8>> {
        self.requests.lock().unwrap().clone()
    }
}

fn tid_of(request: &[u8]) -> TransactionId {
    request[8..20].try_into().unwrap()
}

/// A Binding success response to `request` carrying `mapped` as an IPv4
/// XOR-MAPPED-ADDRESS.
fn success(request: &[u8], mapped: SocketAddr) -> Vec<u8> {
    let SocketAddr::V4(mapped) = mapped else {
        panic!("IPv4 only");
    };
    let cookie = 0x2112_A442u32.to_be_bytes();
    let mut msg = request[..20].to_vec();
    msg[0..2].copy_from_slice(&[0x01, 0x01]); // Binding success
    msg[2..4].copy_from_slice(&12u16.to_be_bytes());
    msg.extend([0x00, 0x20, 0x00, 0x08, 0x00, 0x01]); // XOR-MAPPED-ADDRESS, IPv4
    msg.extend((mapped.port() ^ 0x2112).to_be_bytes());
    msg.extend(mapped.ip().octets().iter().zip(cookie).map(|(b, k)| b ^ k));
    msg
}

async fn client() -> (UdpSocket, SocketAddr) {
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let addr = socket.local_addr().unwrap();
    (socket, addr)
}

/// An address with nothing listening on it.
fn closed_port() -> SocketAddr {
    StdUdpSocket::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}

#[tokio::test]
async fn reports_the_sockets_own_address() {
    let server = FakeServer::echo().await;
    let (socket, local) = client().await;

    let mapped = stun::query(&socket, server.addr, Retry::DEFAULT).await;

    assert_eq!(mapped.unwrap(), local);
    let requests = server.requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(
        requests[0][..8],
        [0x00, 0x01, 0x00, 0x00, 0x21, 0x12, 0xa4, 0x42]
    );
}

#[tokio::test]
async fn retransmits_until_answered() {
    let server = FakeServer::start(|n, request, from| match n {
        0 => vec![],
        _ => vec![success(request, from)],
    })
    .await;
    let (socket, local) = client().await;

    assert_eq!(
        stun::query(&socket, server.addr, FAST).await.unwrap(),
        local
    );

    let requests = server.requests();
    assert_eq!(requests.len(), 2);
    assert_eq!(
        tid_of(&requests[0]),
        tid_of(&requests[1]),
        "a retransmission must reuse the transaction id"
    );
}

#[tokio::test]
async fn times_out_when_the_server_never_answers() {
    let server = FakeServer::silent().await;
    let (socket, _) = client().await;

    let started = Instant::now();
    let err = stun::query(&socket, server.addr, FAST).await.unwrap_err();
    let elapsed = started.elapsed();

    assert_eq!(err.kind(), std::io::ErrorKind::TimedOut);
    assert_eq!(server.requests().len(), 3);
    // 50 + 100 + 200ms of waiting, plus scheduling slack.
    assert!(
        elapsed >= Duration::from_millis(350) && elapsed < Duration::from_secs(2),
        "{elapsed:?}"
    );
}

#[tokio::test]
async fn ignores_noise_before_the_real_answer() {
    let decoy = StdUdpSocket::bind("127.0.0.1:0").unwrap();
    let server = FakeServer::start(move |_, request, from| {
        let wrong: SocketAddr = "203.0.113.66:1".parse().unwrap();
        // A valid answer, but from an address we never queried.
        decoy.send_to(&success(request, wrong), from).unwrap();

        let mut wrong_tid = success(request, wrong);
        wrong_tid[19] ^= 0xff;
        let mut error = success(request, wrong);
        error[1] = 0x11; // Binding error response
        let mut bad_cookie = success(request, wrong);
        bad_cookie[4] ^= 0xff;
        vec![
            b"not stun at all".to_vec(),
            vec![],
            wrong_tid,
            error,
            bad_cookie,
            success(request, from),
        ]
    })
    .await;
    let (socket, local) = client().await;

    let mapped = stun::query(&socket, server.addr, Retry::DEFAULT).await;

    assert_eq!(mapped.unwrap(), local);
    assert_eq!(server.requests().len(), 1);
}

#[tokio::test]
async fn closed_port_times_out_rather_than_erroring() {
    // On Windows the ICMP port-unreachable surfaces as a receive error, which
    // must not end the query early.
    let (socket, _) = client().await;
    let err = stun::query(&socket, closed_port(), FAST).await.unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::TimedOut);
}

#[tokio::test]
async fn probe_reports_each_server_in_order_past_failures() {
    let silent = FakeServer::silent().await;
    let live = FakeServer::echo().await;
    let (socket, local) = client().await;

    let results = stun::probe(&socket, &[silent.addr, closed_port(), live.addr], FAST).await;

    assert_eq!(results.len(), 3);
    assert!(results[0].is_err());
    assert!(results[1].is_err());
    assert_eq!(*results[2].as_ref().unwrap(), local);
}

#[tokio::test]
async fn same_mapping_from_every_server_is_cone() {
    let a = FakeServer::echo().await;
    let b = FakeServer::echo().await;
    let (socket, _) = client().await;

    let results = stun::probe(&socket, &[a.addr, b.addr], FAST).await;
    let mappings: Vec<_> = results.into_iter().map(Result::unwrap).collect();

    assert_eq!(stun::nat_type(&mappings), Some(NatType::Cone));
}

#[tokio::test]
async fn different_mappings_are_symmetric() {
    let a = FakeServer::echo().await;
    // Simulates a NAT that picked a different external port for this server.
    let b = FakeServer::start(|_, request, mut from| {
        from.set_port(from.port().wrapping_add(1));
        vec![success(request, from)]
    })
    .await;
    let (socket, _) = client().await;

    let results = stun::probe(&socket, &[a.addr, b.addr], FAST).await;
    let mappings: Vec<_> = results.into_iter().map(Result::unwrap).collect();

    assert_eq!(stun::nat_type(&mappings), Some(NatType::Symmetric));
}
