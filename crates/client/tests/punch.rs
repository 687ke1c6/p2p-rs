//! Punching on loopback: two real clients end to end, and one client against
//! scripted or silent peers.

mod common;

use std::net::{Ipv4Addr, SocketAddr};
use std::time::{Duration, Instant};

use common::{FakeServer, FAST};
use p2p_client::endpoint::{nat_info, Endpoint};
use p2p_client::exchange::exchange;
use p2p_client::punch::{punch, Connected, PunchConfig, PunchError};
use p2p_client::rendezvous::Rendezvous;
use protocol::{decode_peer, encode_peer, NatInfo, NatType, PeerId, PeerMessage};
use tokio::net::{TcpListener, UdpSocket};
use tokio::time::timeout;

const QUICK: PunchConfig = PunchConfig {
    timeout: Duration::from_secs(1),
    interval: Duration::from_millis(50),
    linger: Duration::from_millis(100),
};

/// Candidates for a socket on loopback port `port`.
fn loopback_nat(port: u16, nat_type: NatType) -> NatInfo {
    let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, port));
    let mut info = nat_info(&[addr], Ipv4Addr::LOCALHOST, port).unwrap();
    info.nat_type = nat_type;
    info
}

/// Registers, exchanges candidates and punches, like `p2p-client connect`.
async fn run_client(
    server: SocketAddr,
    stun: &[SocketAddr],
) -> (PeerId, u16, Result<Connected, PunchError>) {
    let mut endpoint = Endpoint::bind().await.unwrap();
    let mut rendezvous = Rendezvous::connect(server).await.unwrap();
    let my_id = rendezvous.register("share-1").await.unwrap();
    let done = exchange(&mut endpoint, rendezvous, my_id, stun, FAST)
        .await
        .unwrap();
    let result = punch(
        &mut endpoint,
        my_id,
        done.peer_id,
        &done.my_nat,
        &done.peer_nat,
        PunchConfig::DEFAULT,
    )
    .await;
    (my_id, endpoint.local_port(), result)
}

#[tokio::test(flavor = "multi_thread")]
async fn two_clients_connect_directly() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server = listener.local_addr().unwrap();
    tokio::spawn(rendezvous_server::serve(listener));
    let stun_a = FakeServer::echo().await;
    let stun_b = FakeServer::echo().await;
    let stun = [stun_a.addr, stun_b.addr];

    let ((_, a_port, a), (_, b_port, b)) = timeout(Duration::from_secs(20), async {
        tokio::join!(run_client(server, &stun), run_client(server, &stun))
    })
    .await
    .expect("clients hung");

    let a = a.expect("client a failed to connect");
    let b = b.expect("client b failed to connect");
    assert_eq!(a.peer_addr, SocketAddr::from((Ipv4Addr::LOCALHOST, b_port)));
    assert_eq!(b.peer_addr, SocketAddr::from((Ipv4Addr::LOCALHOST, a_port)));
}

#[tokio::test(flavor = "multi_thread")]
async fn both_symmetric_aborts_without_punching() {
    let mut endpoint = Endpoint::bind().await.unwrap();
    let peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let peer_port = peer.local_addr().unwrap().port();
    let mine = loopback_nat(endpoint.local_port(), NatType::Symmetric);
    let theirs = loopback_nat(peer_port, NatType::Symmetric);

    let started = Instant::now();
    let err = punch(&mut endpoint, 1, 2, &mine, &theirs, QUICK)
        .await
        .unwrap_err();

    assert!(matches!(err, PunchError::BothSymmetric), "{err}");
    assert!(started.elapsed() < Duration::from_millis(100));
    let mut buf = [0; 64];
    assert!(
        peer.try_recv_from(&mut buf).is_err(),
        "nothing should have been sent"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn silent_peer_times_out_after_punching_from_the_endpoint_socket() {
    let mut endpoint = Endpoint::bind().await.unwrap();
    let my_port = endpoint.local_port();
    let peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let mine = loopback_nat(my_port, NatType::Cone);
    let theirs = loopback_nat(peer.local_addr().unwrap().port(), NatType::Cone);

    let started = Instant::now();
    let err = punch(&mut endpoint, 7, 9, &mine, &theirs, QUICK)
        .await
        .unwrap_err();

    assert!(matches!(err, PunchError::TimedOut(_)), "{err}");
    assert!(started.elapsed() >= QUICK.timeout);
    // The punches came from the socket our candidates describe.
    let mut buf = [0; 1500];
    let (len, from) = peer.recv_from(&mut buf).await.unwrap();
    assert_eq!(from.port(), my_port);
    assert!(matches!(
        decode_peer(&buf[..len]),
        Some(PeerMessage::PunchRequest { from: 7, .. })
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn response_with_the_wrong_nonce_is_not_a_connection() {
    let mut endpoint = Endpoint::bind().await.unwrap();
    let peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let mine = loopback_nat(endpoint.local_port(), NatType::Cone);
    let theirs = loopback_nat(peer.local_addr().unwrap().port(), NatType::Cone);

    // A peer that sends its own request (so we answer it) but answers ours
    // with a nonce it made up.
    tokio::spawn(async move {
        let mut buf = [0; 1500];
        loop {
            let (len, from) = peer.recv_from(&mut buf).await.unwrap();
            if let Some(PeerMessage::PunchRequest { nonce, .. }) = decode_peer(&buf[..len]) {
                let forged = PeerMessage::PunchResponse {
                    from: 9,
                    nonce: nonce.wrapping_add(1),
                };
                let request = PeerMessage::PunchRequest { from: 9, nonce: 1 };
                peer.send_to(&encode_peer(&forged), from).await.unwrap();
                peer.send_to(&encode_peer(&request), from).await.unwrap();
            }
        }
    });

    let err = punch(&mut endpoint, 7, 9, &mine, &theirs, QUICK)
        .await
        .unwrap_err();
    assert!(matches!(err, PunchError::TimedOut(_)), "{err}");
}

#[tokio::test(flavor = "multi_thread")]
async fn well_behaved_scripted_peer_connects() {
    let mut endpoint = Endpoint::bind().await.unwrap();
    let peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let peer_addr = peer.local_addr().unwrap();
    let mine = loopback_nat(endpoint.local_port(), NatType::Cone);
    // Cone: a symmetric peer would make the puncher guess ports, spraying
    // hundreds of random loopback ports.
    let theirs = loopback_nat(peer_addr.port(), NatType::Cone);

    tokio::spawn(async move {
        let mut buf = [0; 1500];
        loop {
            let (len, from) = peer.recv_from(&mut buf).await.unwrap();
            if let Some(PeerMessage::PunchRequest { nonce, .. }) = decode_peer(&buf[..len]) {
                let response = PeerMessage::PunchResponse { from: 9, nonce };
                let request = PeerMessage::PunchRequest { from: 9, nonce: 1 };
                peer.send_to(&encode_peer(&response), from).await.unwrap();
                peer.send_to(&encode_peer(&request), from).await.unwrap();
            }
        }
    });

    let connected = punch(&mut endpoint, 7, 9, &mine, &theirs, QUICK)
        .await
        .expect("should connect");
    assert_eq!(connected.peer_addr, peer_addr);
}
