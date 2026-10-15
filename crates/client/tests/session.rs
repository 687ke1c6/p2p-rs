//! Sessions on loopback: two real sessions, and one session against a
//! scripted peer that drops, duplicates or corrupts datagrams.

use std::net::{Ipv4Addr, SocketAddr};
use std::time::{Duration, Instant};

use p2p_client::endpoint::Endpoint;
use p2p_client::session::{sha256, Session, SessionConfig, SessionError};
use protocol::{decode_peer, encode_peer, PeerMessage, MAX_PAYLOAD_LEN};
use tokio::net::UdpSocket;

const FAST: SessionConfig = SessionConfig {
    keepalive: Duration::from_millis(50),
    peer_timeout: Duration::from_millis(300),
    retransmit: Duration::from_millis(50),
    attempts: 4,
};

const ME: u64 = 1;
const PEER: u64 = 2;

fn loopback(port: u16) -> SocketAddr {
    SocketAddr::from((Ipv4Addr::LOCALHOST, port))
}

/// Our endpoint, plus a raw socket standing in for the peer.
async fn endpoint_and_scripted_peer() -> (Endpoint, UdpSocket, SocketAddr) {
    let endpoint = Endpoint::bind().await.unwrap();
    let me = loopback(endpoint.local_port());
    let peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    (endpoint, peer, me)
}

/// Receives the next decodable peer message on the scripted peer's socket.
async fn next(peer: &UdpSocket) -> PeerMessage {
    let mut buf = [0; 1500];
    loop {
        let (len, _) = tokio::time::timeout(Duration::from_secs(2), peer.recv_from(&mut buf))
            .await
            .expect("scripted peer heard nothing")
            .unwrap();
        if let Some(msg) = decode_peer(&buf[..len]) {
            return msg;
        }
    }
}

async fn send(peer: &UdpSocket, to: SocketAddr, msg: &PeerMessage) {
    peer.send_to(&encode_peer(msg), to).await.unwrap();
}

fn payload(seq: u64, data: &[u8]) -> PeerMessage {
    PeerMessage::Payload {
        from: PEER,
        seq,
        name: None,
        data: data.to_vec(),
        sha256: sha256(data),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn payload_round_trip_between_two_sessions() {
    let mut a = Endpoint::bind().await.unwrap();
    let mut b = Endpoint::bind().await.unwrap();
    let (a_addr, b_addr) = (loopback(a.local_port()), loopback(b.local_port()));
    let file: Vec<u8> = (0..MAX_PAYLOAD_LEN).map(|i| (i * 7) as u8).collect();

    let sender = async {
        let mut session = Session::new(&mut a, None, ME, PEER, b_addr, FAST);
        session
            .send_payload(Some("data.bin".into()), file.clone())
            .await
            .unwrap();
        session.send_payload(None, b"hello".to_vec()).await.unwrap();
        session.close().await.unwrap();
    };
    let receiver = async {
        let mut session = Session::new(&mut b, None, PEER, ME, a_addr, FAST);
        let first = session.recv_payload().await.unwrap().unwrap();
        let second = session.recv_payload().await.unwrap().unwrap();
        let end = session.recv_payload().await.unwrap();
        (first, second, end)
    };
    let ((), (first, second, end)) = tokio::join!(sender, receiver);

    assert_eq!(first.name.as_deref(), Some("data.bin"));
    assert_eq!(first.data, file);
    assert_eq!(first.sha256, sha256(&file));
    assert_eq!(second.name, None);
    assert_eq!(second.data, b"hello");
    assert!(end.is_none(), "Bye should end the session");
}

#[tokio::test(flavor = "multi_thread")]
async fn lost_payload_is_retransmitted_with_the_same_seq() {
    let (mut endpoint, peer, me) = endpoint_and_scripted_peer().await;
    let peer_addr = peer.local_addr().unwrap();
    let script = tokio::spawn(async move {
        let mut seqs = Vec::new();
        loop {
            if let PeerMessage::Payload { seq, .. } = next(&peer).await {
                seqs.push(seq);
                if seqs.len() == 2 {
                    // Drop the first copy; acknowledge the second.
                    let ack = PeerMessage::PayloadAck {
                        from: PEER,
                        seq,
                        intact: true,
                    };
                    send(&peer, me, &ack).await;
                    return seqs;
                }
            }
        }
    });

    let mut session = Session::new(&mut endpoint, None, ME, PEER, peer_addr, FAST);
    session.send_payload(None, b"hi".to_vec()).await.unwrap();

    let seqs = script.await.unwrap();
    assert_eq!(seqs[0], seqs[1]);
}

#[tokio::test(flavor = "multi_thread")]
async fn unacknowledged_payload_fails_after_every_attempt() {
    let (mut endpoint, peer, _) = endpoint_and_scripted_peer().await;
    let mut session = Session::new(
        &mut endpoint,
        None,
        ME,
        PEER,
        peer.local_addr().unwrap(),
        FAST,
    );
    let err = session
        .send_payload(None, b"hi".to_vec())
        .await
        .unwrap_err();
    assert!(matches!(err, SessionError::NoAck), "{err}");
    let mut copies = 0;
    let mut buf = [0; 1500];
    while peer.try_recv_from(&mut buf).is_ok() {
        copies += 1;
    }
    assert_eq!(copies, FAST.attempts);
}

#[tokio::test(flavor = "multi_thread")]
async fn ack_reporting_corruption_is_an_error() {
    let (mut endpoint, peer, me) = endpoint_and_scripted_peer().await;
    let peer_addr = peer.local_addr().unwrap();
    tokio::spawn(async move {
        if let PeerMessage::Payload { seq, .. } = next(&peer).await {
            let ack = PeerMessage::PayloadAck {
                from: PEER,
                seq,
                intact: false,
            };
            send(&peer, me, &ack).await;
        }
    });
    let mut session = Session::new(&mut endpoint, None, ME, PEER, peer_addr, FAST);
    let err = session
        .send_payload(None, b"hi".to_vec())
        .await
        .unwrap_err();
    assert!(matches!(err, SessionError::Corrupted), "{err}");
}

#[tokio::test(flavor = "multi_thread")]
async fn oversized_payload_is_refused_before_sending() {
    let (mut endpoint, peer, _) = endpoint_and_scripted_peer().await;
    let mut session = Session::new(
        &mut endpoint,
        None,
        ME,
        PEER,
        peer.local_addr().unwrap(),
        FAST,
    );
    let err = session
        .send_payload(None, vec![0; MAX_PAYLOAD_LEN + 1])
        .await
        .unwrap_err();
    assert!(matches!(err, SessionError::TooLarge(_)), "{err}");
    let mut buf = [0; 1500];
    assert!(peer.try_recv_from(&mut buf).is_err());
}

#[tokio::test(flavor = "multi_thread")]
async fn receiver_delivers_once_rejects_corruption_and_ignores_strangers() {
    let (mut endpoint, peer, me) = endpoint_and_scripted_peer().await;
    let peer_addr = peer.local_addr().unwrap();
    let stranger = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let script = tokio::spawn(async move {
        // A Bye from the wrong address, and one with the wrong peer id.
        send(&stranger, me, &PeerMessage::Bye { from: PEER }).await;
        send(&peer, me, &PeerMessage::Bye { from: 99 }).await;

        let mut corrupted = payload(0, b"tampered");
        if let PeerMessage::Payload { data, .. } = &mut corrupted {
            data[0] ^= 1;
        }
        send(&peer, me, &corrupted).await;
        send(&peer, me, &payload(1, b"real")).await;
        send(&peer, me, &payload(1, b"real")).await; // retransmission
        send(&peer, me, &PeerMessage::Bye { from: PEER }).await;

        let mut acks = Vec::new();
        while acks.len() < 3 {
            if let PeerMessage::PayloadAck { seq, intact, .. } = next(&peer).await {
                acks.push((seq, intact));
            }
        }
        acks
    });

    let mut session = Session::new(&mut endpoint, None, ME, PEER, peer_addr, FAST);
    let got = session.recv_payload().await.unwrap().unwrap();
    assert_eq!(got.data, b"real");
    assert!(session.recv_payload().await.unwrap().is_none());

    // Every copy is acknowledged, so a lost ack gets retried, but the
    // corrupted one is acknowledged as not intact.
    let acks = script.await.unwrap();
    assert_eq!(acks, vec![(0, false), (1, true), (1, true)]);
}

#[tokio::test(flavor = "multi_thread")]
async fn silent_peer_is_gone_after_keepalives() {
    let (mut endpoint, peer, _) = endpoint_and_scripted_peer().await;
    let mut session = Session::new(
        &mut endpoint,
        None,
        ME,
        PEER,
        peer.local_addr().unwrap(),
        FAST,
    );
    let started = Instant::now();
    let err = session.recv_payload().await.unwrap_err();
    assert!(matches!(err, SessionError::PeerGone), "{err}");
    assert!(started.elapsed() >= FAST.peer_timeout);

    let mut keepalives = 0;
    let mut buf = [0; 1500];
    while let Ok((len, _)) = peer.try_recv_from(&mut buf) {
        if decode_peer(&buf[..len]) == Some(PeerMessage::Keepalive { from: ME }) {
            keepalives += 1;
        }
    }
    assert!(keepalives >= 3, "only {keepalives} keepalives");
}
