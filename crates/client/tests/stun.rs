//! `stun::query` and `stun::probe` against fake STUN servers on loopback:
//! real sockets, no internet.

mod common;

use std::net::{SocketAddr, UdpSocket as StdUdpSocket};
use std::time::{Duration, Instant};

use common::{success, tid_of, FakeServer, FAST};
use p2p_client::stun::{self, Retry};
use protocol::NatType;
use tokio::net::UdpSocket;

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
    let (mut socket, local) = client().await;

    let mapped = stun::query(&mut socket, server.addr, Retry::DEFAULT).await;

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
    let (mut socket, local) = client().await;

    assert_eq!(
        stun::query(&mut socket, server.addr, FAST).await.unwrap(),
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
    let (mut socket, _) = client().await;

    let started = Instant::now();
    let err = stun::query(&mut socket, server.addr, FAST)
        .await
        .unwrap_err();
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
    let (mut socket, local) = client().await;

    let mapped = stun::query(&mut socket, server.addr, Retry::DEFAULT).await;

    assert_eq!(mapped.unwrap(), local);
    assert_eq!(server.requests().len(), 1);
}

#[tokio::test]
async fn closed_port_times_out_rather_than_erroring() {
    // On Windows the ICMP port-unreachable surfaces as a receive error, which
    // must not end the query early.
    let (mut socket, _) = client().await;
    let err = stun::query(&mut socket, closed_port(), FAST)
        .await
        .unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::TimedOut);
}

#[tokio::test]
async fn probe_reports_each_server_in_order_past_failures() {
    let silent = FakeServer::silent().await;
    let live = FakeServer::echo().await;
    let (mut socket, local) = client().await;

    let results = stun::probe(&mut socket, &[silent.addr, closed_port(), live.addr], FAST).await;

    assert_eq!(results.len(), 3);
    assert!(results[0].is_err());
    assert!(results[1].is_err());
    assert_eq!(*results[2].as_ref().unwrap(), local);
}

#[tokio::test]
async fn same_mapping_from_every_server_is_cone() {
    let a = FakeServer::echo().await;
    let b = FakeServer::echo().await;
    let (mut socket, _) = client().await;

    let results = stun::probe(&mut socket, &[a.addr, b.addr], FAST).await;
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
    let (mut socket, _) = client().await;

    let results = stun::probe(&mut socket, &[a.addr, b.addr], FAST).await;
    let mappings: Vec<_> = results.into_iter().map(Result::unwrap).collect();

    assert_eq!(stun::nat_type(&mappings), Some(NatType::Symmetric));
}
