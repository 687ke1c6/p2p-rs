//! The UDP socket we punch from, and the `NatInfo` that describes it to peers.

use std::io;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

use protocol::{NatInfo, NatType};
use rust_p2p_core::punch::Puncher;
use rust_p2p_core::tunnel::config::{TunnelConfig, UdpTunnelConfig};
use rust_p2p_core::tunnel::udp::UdpTunnel;
use rust_p2p_core::tunnel::{new_tunnel_component, Tunnel, TunnelDispatcher};

use crate::stun::{self, Retry};

/// One IPv4 UDP socket owned by `rust-p2p-core`, so the `Puncher` sends from
/// the same socket our STUN mapping describes.
pub struct Endpoint {
    /// Owns the core's sockets; dropping it closes them.
    _dispatcher: TunnelDispatcher,
    pub puncher: Puncher,
    pub tunnel: UdpTunnel,
}

impl Endpoint {
    /// UDP only: TCP punching would need STUN over TCP to learn the public
    /// TCP port, which the prototype doesn't do.
    pub async fn bind() -> io::Result<Self> {
        let udp = UdpTunnelConfig::default().set_use_v6(false);
        let config = TunnelConfig::empty()
            .major_socket_count(1)
            .set_udp_tunnel_config(udp);
        let (mut dispatcher, puncher) = new_tunnel_component(config)?;
        let Tunnel::Udp(tunnel) = dispatcher.dispatch().await? else {
            return Err(io::Error::other("expected a UDP tunnel; TCP is disabled"));
        };
        Ok(Self {
            _dispatcher: dispatcher,
            puncher,
            tunnel,
        })
    }

    pub fn local_port(&self) -> u16 {
        self.tunnel.local_addr().port()
    }

    /// Queries the STUN servers from the punching socket and describes it.
    /// Fails only if no server answers.
    pub async fn detect_nat(
        &mut self,
        stun_servers: &[SocketAddr],
        retry: Retry,
    ) -> io::Result<NatInfo> {
        let results = stun::probe(&mut self.tunnel, stun_servers, retry).await;
        let mut mappings = Vec::new();
        for (server, result) in stun_servers.iter().zip(results) {
            match result {
                Ok(mapped) => {
                    log::debug!("STUN {server} -> {mapped}");
                    mappings.push(mapped);
                }
                Err(e) => log::warn!("STUN {server}: {e}"),
            }
        }
        let local_ipv4 = match stun_servers.first() {
            Some(&toward) => local_ipv4_toward(toward).await,
            None => Ipv4Addr::UNSPECIFIED,
        };
        nat_info(&mappings, local_ipv4, self.local_port())
            .ok_or_else(|| io::Error::other("no STUN server answered"))
    }
}

/// The local address the OS would send from to reach `toward`: our LAN
/// address, which peers behind the same NAT can use. Connecting a UDP socket
/// sends nothing. Unspecified if there is no route, which tells the puncher
/// to skip LAN candidates.
async fn local_ipv4_toward(toward: SocketAddr) -> Ipv4Addr {
    let probe = async {
        let socket = tokio::net::UdpSocket::bind("0.0.0.0:0").await?;
        socket.connect(toward).await?;
        socket.local_addr()
    };
    match probe.await {
        Ok(SocketAddr::V4(addr)) => *addr.ip(),
        Ok(_) => Ipv4Addr::UNSPECIFIED,
        Err(e) => {
            log::warn!("local IPv4 lookup failed: {e}");
            Ipv4Addr::UNSPECIFIED
        }
    }
}

/// Builds the `NatInfo` for a socket bound to `local_port` from the IPv4
/// mappings STUN servers reported for it. `None` without any IPv4 mapping.
pub fn nat_info(mappings: &[SocketAddr], local_ipv4: Ipv4Addr, local_port: u16) -> Option<NatInfo> {
    let v4: Vec<SocketAddrV4> = mappings
        .iter()
        .filter_map(|m| match m {
            SocketAddr::V4(m) => Some(*m),
            SocketAddr::V6(_) => None,
        })
        .collect();
    let first = v4.first()?;

    let as_addrs: Vec<SocketAddr> = v4.iter().copied().map(SocketAddr::V4).collect();
    let nat_type = stun::nat_type(&as_addrs).unwrap_or_else(|| {
        log::warn!("only one STUN server answered; assuming cone NAT");
        NatType::Cone
    });
    let mut public_ips: Vec<Ipv4Addr> = Vec::new();
    for m in &v4 {
        if !public_ips.contains(m.ip()) {
            public_ips.push(*m.ip());
        }
    }
    let ports = v4.iter().map(SocketAddrV4::port);
    let port_range = ports.clone().max().unwrap() - ports.min().unwrap();

    Some(NatInfo {
        nat_type,
        public_ips,
        // One socket, so one public port: the first server's view of it.
        public_udp_ports: vec![first.port()],
        mapping_tcp_addr: vec![],
        mapping_udp_addr: vec![],
        // For a symmetric NAT, how far apart the mapped ports landed; the
        // puncher guesses ports in this range.
        public_port_range: port_range,
        local_ipv4,
        local_ipv4s: vec![],
        ipv6: None,
        local_udp_ports: vec![local_port],
        local_tcp_port: 0,
        public_tcp_port: 0,
    })
}

/// One-line summary of a `NatInfo` for logs and CLI output.
pub fn describe(info: &NatInfo) -> String {
    let public: Vec<String> = info
        .public_ips
        .iter()
        .flat_map(|ip| {
            info.public_udp_ports
                .iter()
                .map(move |port| format!("{ip}:{port}"))
        })
        .collect();
    let local: Vec<String> = info
        .local_udp_ports
        .iter()
        .map(|port| format!("{}:{port}", info.local_ipv4))
        .collect();
    let mut summary = format!(
        "{:?} NAT, public {}, local {}",
        info.nat_type,
        public.join(" "),
        local.join(" ")
    );
    if info.nat_type == NatType::Symmetric {
        summary += &format!(", port range {}", info.public_port_range);
    }
    summary
}

#[cfg(test)]
mod tests {
    use super::*;

    const LAN: Ipv4Addr = Ipv4Addr::new(192, 168, 1, 10);

    fn addr(s: &str) -> SocketAddr {
        s.parse().unwrap()
    }

    #[test]
    fn no_mappings_is_none() {
        assert!(nat_info(&[], LAN, 5000).is_none());
        assert!(nat_info(&[addr("[2001:db8::1]:40000")], LAN, 5000).is_none());
    }

    #[test]
    fn matching_mappings_describe_a_cone_nat() {
        let m = addr("203.0.113.7:40000");
        let info = nat_info(&[m, m, m], LAN, 5000).unwrap();
        assert_eq!(info.nat_type, NatType::Cone);
        assert_eq!(info.public_ips, vec![Ipv4Addr::new(203, 0, 113, 7)]);
        assert_eq!(info.public_udp_ports, vec![40000]);
        assert_eq!(info.public_port_range, 0);
        assert_eq!(info.local_ipv4, LAN);
        assert_eq!(info.local_udp_ports, vec![5000]);
        assert!(info.mapping_udp_addr.is_empty() && info.mapping_tcp_addr.is_empty());
        assert_eq!(info.public_ipv4_addr(), vec![m]);
    }

    #[test]
    fn diverging_ports_describe_a_symmetric_nat() {
        let mappings = [
            addr("203.0.113.7:40010"),
            addr("203.0.113.7:40000"),
            addr("203.0.113.7:40004"),
        ];
        let info = nat_info(&mappings, LAN, 5000).unwrap();
        assert_eq!(info.nat_type, NatType::Symmetric);
        assert_eq!(info.public_udp_ports, vec![40010]);
        assert_eq!(info.public_port_range, 10);
    }

    #[test]
    fn ipv6_mappings_are_ignored() {
        let mappings = [addr("[2001:db8::1]:1"), addr("203.0.113.7:40000")];
        let info = nat_info(&mappings, LAN, 5000).unwrap();
        assert_eq!(info.public_udp_ports, vec![40000]);
    }

    #[test]
    fn single_mapping_assumes_cone() {
        let info = nat_info(&[addr("203.0.113.7:40000")], LAN, 5000).unwrap();
        assert_eq!(info.nat_type, NatType::Cone);
    }
}
