//! Minimal RFC 5389 STUN client: Binding requests only, over a socket the
//! caller owns. A mapping is only valid for the socket that produced it, so
//! the caller probes from the socket it will later punch with.
//!
//! We don't use `rust_p2p_core::stun`: it probes from throwaway sockets of its
//! own, doesn't check transaction ids, and sends the RFC 3489 CHANGE-REQUEST
//! attribute, which RFC 5389-only servers may reject.

use std::io;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::time::Duration;

use protocol::NatType;
use tokio::net::UdpSocket;
use tokio::time::timeout;

const MAGIC_COOKIE: u32 = 0x2112_A442;
const HEADER_LEN: usize = 20;
const BINDING_REQUEST: u16 = 0x0001;
const BINDING_SUCCESS: u16 = 0x0101;
const ATTR_MAPPED_ADDRESS: u16 = 0x0001;
const ATTR_XOR_MAPPED_ADDRESS: u16 = 0x0020;

/// Public STUN servers used unless the user configures their own: three
/// providers at different IPs, so NATs that remap per destination IP show up
/// as symmetric. (stun1.l.google.com is no good as a second server: it
/// resolves to the same IP as stun.l.google.com.)
pub const DEFAULT_SERVERS: &[&str] = &[
    "stun.l.google.com:19302",
    "stun.cloudflare.com:3478",
    "global.stun.twilio.com:3478",
];

/// Retransmission schedule for one Binding transaction.
#[derive(Clone, Copy, Debug)]
pub struct Retry {
    /// Wait after the first request; doubles after each unanswered one.
    pub initial_rto: Duration,
    /// Requests sent before giving up.
    pub attempts: u32,
}

impl Retry {
    /// 0.5s, 1s, 2s: gives up after 3.5s. RFC 5389 7.2.1 starts at 500ms too,
    /// but retries for far longer than an interactive tool should.
    pub const DEFAULT: Self = Self {
        initial_rto: Duration::from_millis(500),
        attempts: 3,
    };
}

pub type TransactionId = [u8; 12];

pub fn binding_request(tid: &TransactionId) -> [u8; HEADER_LEN] {
    let mut msg = [0; HEADER_LEN];
    msg[0..2].copy_from_slice(&BINDING_REQUEST.to_be_bytes());
    // Bytes 2..4: attribute length, zero.
    msg[4..8].copy_from_slice(&MAGIC_COOKIE.to_be_bytes());
    msg[8..20].copy_from_slice(tid);
    msg
}

/// Returns the mapped address from a Binding success response to `tid`, or
/// `None` if `msg` is anything else.
pub fn parse_binding_response(msg: &[u8], tid: &TransactionId) -> Option<SocketAddr> {
    if msg.len() < HEADER_LEN
        || u16::from_be_bytes([msg[0], msg[1]]) != BINDING_SUCCESS
        || u32::from_be_bytes(msg[4..8].try_into().unwrap()) != MAGIC_COOKIE
        || msg[8..20] != tid[..]
    {
        return None;
    }
    let body_len = u16::from_be_bytes([msg[2], msg[3]]) as usize;
    let mut attrs = msg.get(HEADER_LEN..HEADER_LEN + body_len)?;

    let mut mapped = None;
    while attrs.len() >= 4 {
        let typ = u16::from_be_bytes([attrs[0], attrs[1]]);
        let len = u16::from_be_bytes([attrs[2], attrs[3]]) as usize;
        let value = attrs.get(4..4 + len)?;
        match typ {
            ATTR_XOR_MAPPED_ADDRESS => return parse_address(value, Some(tid)),
            ATTR_MAPPED_ADDRESS => mapped = parse_address(value, None),
            _ => {}
        }
        // Values are padded to a multiple of 4 bytes.
        let padded = 4 + len.div_ceil(4) * 4;
        attrs = attrs.get(padded..).unwrap_or_default();
    }
    mapped
}

/// Parses a (XOR-)MAPPED-ADDRESS value. `xor_tid` is set for the XOR form.
fn parse_address(value: &[u8], xor_tid: Option<&TransactionId>) -> Option<SocketAddr> {
    let family = *value.get(1)?;
    let mut port = u16::from_be_bytes([*value.get(2)?, *value.get(3)?]);
    let cookie = MAGIC_COOKIE.to_be_bytes();
    if xor_tid.is_some() {
        port ^= (MAGIC_COOKIE >> 16) as u16;
    }
    match family {
        0x01 => {
            let mut ip: [u8; 4] = value.get(4..8)?.try_into().ok()?;
            if xor_tid.is_some() {
                ip.iter_mut().zip(cookie).for_each(|(b, k)| *b ^= k);
            }
            Some(SocketAddr::from((Ipv4Addr::from(ip), port)))
        }
        0x02 => {
            let mut ip: [u8; 16] = value.get(4..20)?.try_into().ok()?;
            if let Some(tid) = xor_tid {
                let key = cookie.iter().chain(tid);
                ip.iter_mut().zip(key).for_each(|(b, k)| *b ^= k);
            }
            Some(SocketAddr::from((Ipv6Addr::from(ip), port)))
        }
        _ => None,
    }
}

/// Sends a Binding request to `server` from `socket` and returns the public
/// address the server saw. Retransmits per `retry`, then times out.
pub async fn query(socket: &UdpSocket, server: SocketAddr, retry: Retry) -> io::Result<SocketAddr> {
    let tid: TransactionId = rand::random();
    let request = binding_request(&tid);
    let mut buf = [0; 1500];
    let mut rto = retry.initial_rto;
    for _ in 0..retry.attempts {
        socket.send_to(&request, server).await?;
        let wait = async {
            loop {
                let (len, from) = match socket.recv_from(&mut buf).await {
                    Ok(received) => received,
                    // Windows reports an ICMP port-unreachable from any
                    // earlier send as an error on the next receive. It may
                    // not concern this server, so keep waiting.
                    Err(e) if is_icmp_error(&e) => continue,
                    Err(e) => return Err(e),
                };
                if from != server {
                    continue;
                }
                if let Some(addr) = parse_binding_response(&buf[..len], &tid) {
                    return Ok(addr);
                }
            }
        };
        if let Ok(result) = timeout(rto, wait).await {
            return result;
        }
        rto *= 2;
    }
    Err(io::Error::new(io::ErrorKind::TimedOut, "no STUN response"))
}

fn is_icmp_error(e: &io::Error) -> bool {
    matches!(
        e.kind(),
        io::ErrorKind::ConnectionReset | io::ErrorKind::ConnectionRefused
    )
}

/// Queries each server in turn from the same socket, so the mappings are
/// comparable. Returns one result per server, in order.
pub async fn probe(
    socket: &UdpSocket,
    servers: &[SocketAddr],
    retry: Retry,
) -> Vec<io::Result<SocketAddr>> {
    let mut results = Vec::with_capacity(servers.len());
    for &server in servers {
        results.push(query(socket, server, retry).await);
    }
    results
}

/// Classifies the NAT from the mappings one socket got from different STUN
/// servers: the same mapping everywhere means cone, any difference means
/// symmetric. Returns `None` with fewer than two mappings to compare.
pub fn nat_type(mappings: &[SocketAddr]) -> Option<NatType> {
    match mappings {
        [] | [_] => None,
        [first, rest @ ..] if rest.iter().all(|m| m == first) => Some(NatType::Cone),
        _ => Some(NatType::Symmetric),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// RFC 5769 section 2.2: sample IPv4 response.
    const RFC5769_TID: TransactionId = [
        0xb7, 0xe7, 0xa7, 0x01, 0xbc, 0x34, 0xd6, 0x86, 0xfa, 0x87, 0xdf, 0xae,
    ];
    const RFC5769_IPV4_RESPONSE: [u8; 80] = [
        0x01, 0x01, 0x00, 0x3c, 0x21, 0x12, 0xa4, 0x42, 0xb7, 0xe7, 0xa7, 0x01, 0xbc, 0x34, 0xd6,
        0x86, 0xfa, 0x87, 0xdf, 0xae, // header
        0x80, 0x22, 0x00, 0x0b, 0x74, 0x65, 0x73, 0x74, 0x20, 0x76, 0x65, 0x63, 0x74, 0x6f, 0x72,
        0x20, // SOFTWARE "test vector" + padding
        0x00, 0x20, 0x00, 0x08, 0x00, 0x01, 0xa1, 0x47, 0xe1, 0x12, 0xa6, 0x43, // XOR-MAPPED
        0x00, 0x08, 0x00, 0x14, 0x2b, 0x91, 0xf5, 0x99, 0xfd, 0x9e, 0x90, 0x19, 0x38, 0xc0, 0xfc,
        0x3b, 0x0a, 0x8c, 0x9e, 0x5a, 0x8d, 0x1c, 0x7e, 0xd4, // MESSAGE-INTEGRITY
        0x80, 0x28, 0x00, 0x04, 0xc0, 0x7d, 0x4c, 0x96, // FINGERPRINT
    ];

    #[test]
    fn classifies_nat_type() {
        let a: SocketAddr = "203.0.113.1:40000".parse().unwrap();
        let b: SocketAddr = "203.0.113.1:40001".parse().unwrap();
        assert_eq!(nat_type(&[]), None);
        assert_eq!(nat_type(&[a]), None);
        assert_eq!(nat_type(&[a, a, a]), Some(NatType::Cone));
        assert_eq!(nat_type(&[a, a, b]), Some(NatType::Symmetric));
    }

    #[test]
    fn request_layout() {
        let tid = [7; 12];
        let msg = binding_request(&tid);
        assert_eq!(msg[..8], [0x00, 0x01, 0x00, 0x00, 0x21, 0x12, 0xa4, 0x42]);
        assert_eq!(msg[8..], tid);
    }

    #[test]
    fn parses_rfc5769_ipv4_response() {
        let addr = parse_binding_response(&RFC5769_IPV4_RESPONSE, &RFC5769_TID);
        assert_eq!(addr, Some("192.0.2.1:32853".parse().unwrap()));
    }

    #[test]
    fn rejects_wrong_transaction_id() {
        let mut tid = RFC5769_TID;
        tid[0] ^= 1;
        assert_eq!(parse_binding_response(&RFC5769_IPV4_RESPONSE, &tid), None);
    }

    #[test]
    fn rejects_error_and_truncated_responses() {
        let mut error = RFC5769_IPV4_RESPONSE;
        error[1] = 0x11; // Binding error response
        assert_eq!(parse_binding_response(&error, &RFC5769_TID), None);
        for len in [0, 19, 40] {
            let truncated = &RFC5769_IPV4_RESPONSE[..len];
            assert_eq!(parse_binding_response(truncated, &RFC5769_TID), None);
        }
    }

    #[test]
    fn parses_ipv6_xor_mapped_address() {
        let tid = [0x5a; 12];
        let ip: Ipv6Addr = "2001:db8::1".parse().unwrap();
        let mut value = vec![0, 0x02];
        value.extend((50000u16 ^ 0x2112).to_be_bytes());
        let key = MAGIC_COOKIE.to_be_bytes().into_iter().chain(tid);
        value.extend(ip.octets().iter().zip(key).map(|(b, k)| b ^ k));
        assert_eq!(
            parse_address(&value, Some(&tid)),
            Some(SocketAddr::from((ip, 50000)))
        );
    }

    #[test]
    fn falls_back_to_plain_mapped_address() {
        let tid = [1; 12];
        let mut msg = binding_request(&tid).to_vec();
        msg[0..2].copy_from_slice(&BINDING_SUCCESS.to_be_bytes());
        msg[2..4].copy_from_slice(&12u16.to_be_bytes());
        msg.extend([
            0x00, 0x01, 0x00, 0x08, 0x00, 0x01, 0x1f, 0x90, 203, 0, 113, 9,
        ]);
        assert_eq!(
            parse_binding_response(&msg, &tid),
            Some("203.0.113.9:8080".parse().unwrap())
        );
    }
}
