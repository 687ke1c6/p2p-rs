//! The default public STUN servers, over the real internet. Ignored by
//! default; run with `just test-network`.

use std::net::{IpAddr, SocketAddr};

use p2p_client::resolve_v4;
use p2p_client::stun::{self, Retry, DEFAULT_SERVERS};
use tokio::net::UdpSocket;

/// Queries every default server from one socket. Panics listing every server
/// that failed to resolve or answer.
async fn query_defaults(socket: &mut UdpSocket) -> Vec<(&'static str, SocketAddr)> {
    let mut mappings = Vec::new();
    let mut failures = Vec::new();
    for &server in DEFAULT_SERVERS {
        let Some(addr) = resolve_v4("STUN", server).await else {
            failures.push(format!("{server}: did not resolve to IPv4"));
            continue;
        };
        match stun::query(socket, addr, Retry::DEFAULT).await {
            Ok(mapped) => mappings.push((server, mapped)),
            Err(e) => failures.push(format!("{server} ({addr}): {e}")),
        }
    }
    assert!(failures.is_empty(), "STUN failures: {failures:#?}");
    mappings
}

#[tokio::test]
#[ignore = "queries public STUN servers; run with `just test-network`"]
async fn default_servers_all_answer_with_one_public_ip() {
    let _ = env_logger::builder().is_test(true).try_init();
    let mut socket = UdpSocket::bind("0.0.0.0:0").await.unwrap();

    let mappings = query_defaults(&mut socket).await;
    for (server, mapped) in &mappings {
        println!("{server} -> {mapped}");
    }

    let ip = mappings[0].1.ip();
    assert!(
        mappings.iter().all(|(_, m)| m.ip() == ip),
        "servers disagree on the public IP: {mappings:#?}"
    );
    let IpAddr::V4(v4) = ip else {
        panic!("expected an IPv4 mapping, got {ip}");
    };
    assert!(
        !(v4.is_private() || v4.is_loopback() || v4.is_link_local() || v4.is_unspecified()),
        "mapped address {v4} is not public"
    );
}

#[tokio::test]
#[ignore = "queries public STUN servers; run with `just test-network`"]
async fn a_socket_keeps_its_mapping_across_queries() {
    let mut socket = UdpSocket::bind("0.0.0.0:0").await.unwrap();
    let first = query_defaults(&mut socket).await;
    let second = query_defaults(&mut socket).await;
    println!("first:  {first:?}\nsecond: {second:?}");
    assert_eq!(first, second, "the NAT remapped the socket between queries");
}
