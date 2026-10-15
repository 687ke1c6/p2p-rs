//! Direct-only P2P client.

pub mod stun;

use std::net::SocketAddr;

/// Resolves `host:port` for the named service, logging every address and
/// returning the first IPv4 one. Failures are logged with the service name.
pub async fn resolve_v4(service: &str, target: &str) -> Option<SocketAddr> {
    let addrs: Vec<SocketAddr> = match tokio::net::lookup_host(target).await {
        Ok(addrs) => addrs.collect(),
        Err(e) => {
            log::error!("{service} {target}: resolve failed: {e}");
            return None;
        }
    };
    log::info!("{service} {target} resolved to {addrs:?}");
    let addr = addrs.into_iter().find(SocketAddr::is_ipv4);
    if addr.is_none() {
        log::error!("{service} {target}: no IPv4 address");
    }
    addr
}
