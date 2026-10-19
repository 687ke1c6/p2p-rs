//! What this node sees of its connections to other members: whether each
//! peer is reached directly or through a relay, the round-trip time, and how
//! many bytes went each way over direct and relay paths.
//!
//! The gate hands every admitted docs, gossip or blobs connection to
//! [`Peers::track`]. A task per connection follows its path events: it logs
//! when a peer's selected path changes, and on close folds the connection's
//! final per-path byte counts into the peer's totals, so traffic survives
//! reconnects.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use iroh::endpoint::{Connection, PathEvent, PathId, PathStats, WeakConnectionHandle};
use iroh::{EndpointId, TransportAddr};
use n0_future::StreamExt;
use serde::{Deserialize, Serialize};

use crate::member::Names;

/// How a peer is reached: the kind of the selected path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum LinkKind {
    Direct,
    Relay,
}

impl fmt::Display for LinkKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            LinkKind::Direct => "direct",
            LinkKind::Relay => "relay",
        })
    }
}

impl LinkKind {
    fn of(addr: &TransportAddr) -> Self {
        match addr {
            TransportAddr::Relay(_) => LinkKind::Relay,
            _ => LinkKind::Direct,
        }
    }
}

/// Bytes sent and received, split by path kind. UDP payload bytes, so they
/// include QUIC overhead.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Traffic {
    pub direct_sent: u64,
    pub direct_recv: u64,
    pub relay_sent: u64,
    pub relay_recv: u64,
}

impl Traffic {
    fn add_path(&mut self, kind: LinkKind, stats: &PathStats) {
        match kind {
            LinkKind::Direct => {
                self.direct_sent += stats.udp_tx.bytes;
                self.direct_recv += stats.udp_rx.bytes;
            }
            LinkKind::Relay => {
                self.relay_sent += stats.udp_tx.bytes;
                self.relay_recv += stats.udp_rx.bytes;
            }
        }
    }

    fn add(&mut self, other: &Traffic) {
        self.direct_sent += other.direct_sent;
        self.direct_recv += other.direct_recv;
        self.relay_sent += other.relay_sent;
        self.relay_recv += other.relay_recv;
    }
}

impl fmt::Display for Traffic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "direct ↑{} ↓{}, relay ↑{} ↓{}",
            bytes(self.direct_sent),
            bytes(self.direct_recv),
            bytes(self.relay_sent),
            bytes(self.relay_recv)
        )
    }
}

/// Formats a byte count with a binary unit.
pub fn bytes(n: u64) -> String {
    const UNITS: [&str; 4] = ["B", "KiB", "MiB", "GiB"];
    let mut value = n as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{n} B")
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

/// One peer as this node sees it right now.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PeerSnapshot {
    pub id: EndpointId,
    /// Open connections (one per protocol in use).
    pub connections: usize,
    /// The selected path's kind, if a connection is open.
    pub kind: Option<LinkKind>,
    /// The selected path's address: an IP and port, or a relay URL.
    pub path: Option<String>,
    pub rtt: Option<Duration>,
    /// Totals since this node started, over open and closed connections.
    pub traffic: Traffic,
}

#[derive(Debug, Clone, Default)]
pub struct Peers {
    state: Arc<Mutex<HashMap<EndpointId, PeerState>>>,
    names: Names,
}

#[derive(Debug, Default)]
struct PeerState {
    conns: HashMap<usize, TrackedConn>,
    /// Traffic of connections that have closed.
    closed: Traffic,
    /// The selected path last logged, so a change is logged once per peer
    /// rather than once per connection.
    logged_path: Option<String>,
}

#[derive(Debug)]
struct TrackedConn {
    alpn: Vec<u8>,
    handle: WeakConnectionHandle,
    kinds: HashMap<PathId, LinkKind>,
    /// Paths that closed while the connection stayed open.
    closed_paths: HashSet<PathId>,
    closed_traffic: Traffic,
}

impl Peers {
    /// Peers shown by `names` in the logs.
    pub fn new(names: Names) -> Self {
        Self {
            state: Arc::default(),
            names,
        }
    }

    /// Starts following an admitted connection.
    pub fn track(&self, conn: &Connection) {
        let remote = conn.remote_id();
        let stable_id = conn.stable_id();
        let alpn = conn.alpn().to_vec();
        let kinds = conn
            .paths()
            .iter()
            .map(|path| (path.id(), LinkKind::of(path.remote_addr())))
            .collect();
        let handle = conn.weak_handle();
        let closed = handle.closed();
        let mut events = conn.path_events();
        let first = {
            let mut peers = self.state.lock().unwrap();
            let peer = peers.entry(remote).or_default();
            let first = peer.conns.is_empty();
            peer.conns.insert(
                stable_id,
                TrackedConn {
                    alpn: alpn.clone(),
                    handle,
                    kinds,
                    closed_paths: HashSet::new(),
                    closed_traffic: Traffic::default(),
                },
            );
            first
        };
        let selected = selected_path(conn);
        if first {
            tracing::info!(
                "connected to {} ({})",
                self.names.display(remote),
                selected.as_deref().unwrap_or("no path yet")
            );
        }
        tracing::debug!(
            "{} connection {stable_id} open on {}",
            self.names.display(remote),
            String::from_utf8_lossy(&alpn)
        );
        self.note_path(remote, selected);

        let peers = self.clone();
        tokio::spawn(async move {
            tokio::pin!(closed);
            loop {
                tokio::select! {
                    final_state = &mut closed => {
                        let path_stats = final_state.map(|c| c.path_stats).unwrap_or_default();
                        peers.on_closed(remote, stable_id, path_stats);
                        break;
                    }
                    event = events.next() => match event {
                        Some(event) => peers.on_path_event(remote, stable_id, event),
                        None => {
                            // The event stream ends with the connection;
                            // the close future still delivers final stats.
                            let path_stats = closed.await.map(|c| c.path_stats).unwrap_or_default();
                            peers.on_closed(remote, stable_id, path_stats);
                            break;
                        }
                    },
                }
            }
        });
    }

    fn on_path_event(&self, remote: EndpointId, stable_id: usize, event: PathEvent) {
        let mut selected = None;
        {
            let mut peers = self.state.lock().unwrap();
            let Some(conn) = peers
                .get_mut(&remote)
                .and_then(|peer| peer.conns.get_mut(&stable_id))
            else {
                return;
            };
            match event {
                PathEvent::Opened {
                    id, remote_addr, ..
                } => {
                    conn.kinds.insert(id, LinkKind::of(&remote_addr));
                }
                PathEvent::Closed {
                    id,
                    remote_addr,
                    last_stats,
                    ..
                } => {
                    let kind = conn
                        .kinds
                        .get(&id)
                        .copied()
                        .unwrap_or(LinkKind::of(&remote_addr));
                    if conn.closed_paths.insert(id) {
                        conn.closed_traffic.add_path(kind, &last_stats);
                    }
                }
                PathEvent::Selected {
                    id, remote_addr, ..
                } => {
                    conn.kinds.insert(id, LinkKind::of(&remote_addr));
                    selected = Some(describe(&remote_addr));
                }
                PathEvent::Lagged { .. } => {
                    if let Some(strong) = conn.handle.upgrade() {
                        for path in strong.paths().iter() {
                            conn.kinds
                                .insert(path.id(), LinkKind::of(path.remote_addr()));
                        }
                    }
                }
                _ => {}
            }
        }
        if selected.is_some() {
            self.note_path(remote, selected);
        }
    }

    fn on_closed(
        &self,
        remote: EndpointId,
        stable_id: usize,
        path_stats: Vec<(PathId, PathStats)>,
    ) {
        let mut peers = self.state.lock().unwrap();
        let Some(peer) = peers.get_mut(&remote) else {
            return;
        };
        let Some(conn) = peer.conns.remove(&stable_id) else {
            return;
        };
        let mut traffic = conn.closed_traffic;
        for (id, stats) in &path_stats {
            if !conn.closed_paths.contains(id) {
                let kind = conn.kinds.get(id).copied().unwrap_or(LinkKind::Direct);
                traffic.add_path(kind, stats);
            }
        }
        peer.closed.add(&traffic);
        tracing::debug!(
            "{} connection {stable_id} on {} closed: {traffic}",
            self.names.display(remote),
            String::from_utf8_lossy(&conn.alpn)
        );
        if peer.conns.is_empty() {
            peer.logged_path = None;
            tracing::info!("disconnected from {}", self.names.display(remote));
        }
    }

    /// Logs a peer's selected path when it differs from the last one logged.
    fn note_path(&self, remote: EndpointId, path: Option<String>) {
        let Some(path) = path else { return };
        let mut peers = self.state.lock().unwrap();
        let Some(peer) = peers.get_mut(&remote) else {
            return;
        };
        if peer.logged_path.as_ref() == Some(&path) {
            return;
        }
        let who = self.names.display(remote);
        match peer.logged_path.replace(path.clone()) {
            Some(old) => tracing::info!("{who}: path {old} → {path}"),
            None => tracing::info!("{who}: path {path}"),
        }
    }

    /// The peers this node has had a connection to since it started.
    pub fn snapshot(&self) -> Vec<PeerSnapshot> {
        let peers = self.state.lock().unwrap();
        let mut out: Vec<_> = peers
            .iter()
            .map(|(id, peer)| {
                let mut traffic = peer.closed;
                let mut kind = None;
                let mut path = None;
                let mut rtt = None;
                let mut connections = 0;
                for conn in peer.conns.values() {
                    let Some(strong) = conn.handle.upgrade() else {
                        continue;
                    };
                    connections += 1;
                    traffic.add(&conn.closed_traffic);
                    for p in strong.paths().iter() {
                        let k = LinkKind::of(p.remote_addr());
                        if !conn.closed_paths.contains(&p.id()) {
                            traffic.add_path(k, &p.stats());
                        }
                        // Prefer a direct selected path if connections disagree.
                        if p.is_selected() && kind != Some(LinkKind::Direct) {
                            kind = Some(k);
                            path = Some(describe(p.remote_addr()));
                            rtt = Some(p.rtt());
                        }
                    }
                }
                PeerSnapshot {
                    id: *id,
                    connections,
                    kind,
                    path,
                    rtt,
                    traffic,
                }
            })
            .collect();
        out.sort_by_key(|peer| peer.id);
        out
    }
}

fn selected_path(conn: &Connection) -> Option<String> {
    conn.paths()
        .iter()
        .find(|path| path.is_selected())
        .map(|path| describe(path.remote_addr()))
}

fn describe(addr: &TransportAddr) -> String {
    match addr {
        TransportAddr::Relay(url) => format!("relay {url}"),
        TransportAddr::Ip(addr) => format!("direct {addr}"),
        other => format!("{other:?}"),
    }
}
