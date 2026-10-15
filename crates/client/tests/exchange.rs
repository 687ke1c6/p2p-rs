//! Two clients trading candidates through an in-process rendezvous server,
//! with fake STUN servers on loopback. Each client's STUN runs through its
//! own `rust-p2p-core` endpoint, as in the real binary.

mod common;

use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;

use common::{FakeServer, FAST};
use p2p_client::endpoint::Endpoint;
use p2p_client::exchange::{exchange, Exchanged};
use p2p_client::rendezvous::Rendezvous;
use protocol::{NatType, PeerId};
use tokio::net::TcpListener;
use tokio::time::timeout;

async fn start_rendezvous() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(rendezvous_server::serve(listener));
    addr
}

struct Client {
    my_id: PeerId,
    local_port: u16,
    done: Exchanged,
}

async fn run_client(server: SocketAddr, token: &str, stun: &[SocketAddr]) -> Client {
    let mut endpoint = Endpoint::bind().await.unwrap();
    let mut rendezvous = Rendezvous::connect(server).await.unwrap();
    let my_id = rendezvous.register(token).await.unwrap();
    let done = exchange(&mut endpoint, rendezvous, my_id, stun, FAST)
        .await
        .unwrap();
    Client {
        my_id,
        local_port: endpoint.local_port(),
        done,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn two_clients_trade_candidates() {
    let server = start_rendezvous().await;
    let stun_a = FakeServer::echo().await;
    let stun_b = FakeServer::echo().await;
    let stun = [stun_a.addr, stun_b.addr];

    let (a, b) = timeout(Duration::from_secs(10), async {
        tokio::join!(
            run_client(server, "share-1", &stun),
            run_client(server, "share-1", &stun),
        )
    })
    .await
    .expect("candidate exchange timed out");

    assert_eq!(a.done.peer_id, b.my_id);
    assert_eq!(b.done.peer_id, a.my_id);

    for (me, peer) in [(&a, &b), (&b, &a)] {
        let got = &me.done.peer_nat;
        // What the peer sent is what we received, untouched by the server.
        assert_eq!(format!("{got:?}"), format!("{:?}", peer.done.my_nat));
        // And it describes the peer's punching socket: on loopback the STUN
        // mapping is the socket's own address.
        assert_eq!(got.nat_type, NatType::Cone);
        assert_eq!(got.public_ips, vec![Ipv4Addr::LOCALHOST]);
        assert_eq!(got.public_udp_ports, vec![peer.local_port]);
        assert_eq!(got.local_udp_ports, vec![peer.local_port]);
        assert_eq!(got.local_ipv4, Ipv4Addr::LOCALHOST);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn exchange_fails_when_no_stun_server_answers() {
    let server = start_rendezvous().await;
    let silent = FakeServer::silent().await;
    let stun = [silent.addr];

    let mut rendezvous_a = Rendezvous::connect(server).await.unwrap();
    let id_a = rendezvous_a.register("share-1").await.unwrap();
    // A second peer makes `a` (the newer one) try to offer candidates.
    let mut rendezvous_b = Rendezvous::connect(server).await.unwrap();
    let id_b = rendezvous_b.register("share-1").await.unwrap();
    assert!(id_b > id_a);

    let mut endpoint_b = Endpoint::bind().await.unwrap();
    let err = timeout(
        Duration::from_secs(5),
        exchange(&mut endpoint_b, rendezvous_b, id_b, &stun, FAST),
    )
    .await
    .expect("should fail, not hang")
    .err()
    .expect("exchange should fail");
    assert!(err.to_string().contains("no STUN server answered"), "{err}");
}
