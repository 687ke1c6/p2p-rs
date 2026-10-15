//! Test helpers shared by the client's integration tests.
#![allow(dead_code)] // Each test crate uses a different subset.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use p2p_client::stun::{Retry, TransactionId};
use tokio::net::UdpSocket;

/// Short timeouts for tests that expect them to fire: gives up after 350ms.
pub const FAST: Retry = Retry {
    initial_rto: Duration::from_millis(50),
    attempts: 3,
};

pub struct FakeServer {
    pub addr: SocketAddr,
    /// Every datagram received, in order.
    requests: Arc<Mutex<Vec<Vec<u8>>>>,
}

impl FakeServer {
    /// Starts a server that calls `handler(n, request, from)` for the n-th
    /// datagram (0-based) and sends back whatever datagrams it returns.
    pub async fn start<F>(mut handler: F) -> Self
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
    pub async fn echo() -> Self {
        Self::start(|_, request, from| vec![success(request, from)]).await
    }

    /// A server that receives requests but never answers.
    pub async fn silent() -> Self {
        Self::start(|_, _, _| vec![]).await
    }

    pub fn requests(&self) -> Vec<Vec<u8>> {
        self.requests.lock().unwrap().clone()
    }
}

pub fn tid_of(request: &[u8]) -> TransactionId {
    request[8..20].try_into().unwrap()
}

/// A Binding success response to `request` carrying `mapped` as an IPv4
/// XOR-MAPPED-ADDRESS.
pub fn success(request: &[u8], mapped: SocketAddr) -> Vec<u8> {
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
