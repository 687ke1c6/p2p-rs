//! The group as this node sees it, for `sync-node status` and the logs.
//!
//! `run` holds the stores open, so another process can't read them. Instead
//! the running node rewrites the rendered status to the `status` file in the
//! config directory every presence interval, and `status` prints that file.

use std::fmt::Write as _;
use std::fs;
use std::io;
use std::path::Path;
use std::time::Duration;

use iroh::EndpointId;

use crate::folder::FolderState;
use crate::group::Group;
use crate::member::Names;
use crate::peers::{bytes, PeerSnapshot};
use crate::presence::{LinkState, View};
use crate::record::{unix_now, GroupId};

/// File name of the rendered status inside the config directory.
pub const STATUS_FILE: &str = "status";

#[derive(Debug, Clone)]
pub struct Status {
    pub node: EndpointId,
    pub group: GroupId,
    pub founder: EndpointId,
    /// Unix seconds.
    pub generated_at: u64,
    pub members: Vec<MemberStatus>,
    /// Every pair of members, each once.
    pub links: Vec<Link>,
    /// The folders this node syncs.
    pub folders: Vec<FolderState>,
}

#[derive(Debug, Clone)]
pub struct MemberStatus {
    pub id: EndpointId,
    pub name: Option<String>,
    pub added_by: EndpointId,
    pub this_node: bool,
    pub online: bool,
    /// How long ago its last presence arrived.
    pub last_heard: Option<Duration>,
    /// This node's own connection to it, if it ever had one.
    pub peer: Option<PeerSnapshot>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Link {
    pub a: EndpointId,
    pub b: EndpointId,
    pub state: LinkState,
}

impl Status {
    pub fn build(
        node: EndpointId,
        group: &Group,
        view: &View,
        peers: &[PeerSnapshot],
        names: &Names,
        folders: &[FolderState],
    ) -> Self {
        let mut records = view.members();
        // Founder first, then by when they joined.
        records.sort_by_key(|s| {
            (
                s.record.member != group.founder,
                s.record.added_at,
                s.record.member,
            )
        });
        let members: Vec<MemberStatus> = records
            .iter()
            .map(|s| {
                let id = s.record.member;
                MemberStatus {
                    id,
                    name: names.get(id),
                    added_by: s.record.added_by,
                    this_node: id == node,
                    online: id == node || view.is_online(id),
                    last_heard: view.last_heard(id),
                    peer: peers.iter().find(|p| p.id == id).cloned(),
                }
            })
            .collect();
        let mut links = Vec::new();
        for (i, a) in members.iter().enumerate() {
            for b in &members[i + 1..] {
                links.push(Link {
                    a: a.id,
                    b: b.id,
                    state: view.link(a.id, b.id),
                });
            }
        }
        Self {
            node,
            group: group.id,
            founder: group.founder,
            generated_at: unix_now(),
            members,
            links,
            folders: folders.to_vec(),
        }
    }

    pub fn member(&self, id: EndpointId) -> Option<&MemberStatus> {
        self.members.iter().find(|m| m.id == id)
    }

    pub fn link(&self, a: EndpointId, b: EndpointId) -> Option<LinkState> {
        self.links
            .iter()
            .find(|l| (l.a == a && l.b == b) || (l.a == b && l.b == a))
            .map(|l| l.state)
    }

    /// How to show `id`: its name if it has one, otherwise its short ID.
    pub fn display(&self, id: EndpointId) -> String {
        self.member(id)
            .and_then(|m| m.name.clone())
            .unwrap_or_else(|| id.fmt_short().to_string())
    }

    pub fn render(&self) -> String {
        let short = |id: EndpointId| self.display(id);
        let width = self
            .members
            .iter()
            .map(|m| short(m.id).chars().count())
            .max()
            .unwrap_or(0);
        let mut out = String::new();
        let online = self.members.iter().filter(|m| m.online).count();
        let _ = writeln!(out, "node    {}", self.node);
        let _ = writeln!(out, "group   {}", self.group);
        let _ = writeln!(out, "founder {}", short(self.founder));
        let _ = writeln!(out, "\nmembers ({}, {online} online):", self.members.len());
        for m in &self.members {
            let mut line = format!("  {:width$}", short(m.id));
            if m.this_node {
                line.push_str("  this node");
            } else if m.online {
                line.push_str("  online ");
            } else {
                match m.last_heard {
                    Some(ago) => {
                        let _ = write!(line, "  offline (last heard {}s ago)", ago.as_secs());
                    }
                    None => line.push_str("  offline (never heard)"),
                }
            }
            if m.id == self.founder {
                line.push_str("  founder");
            } else {
                let _ = write!(line, "  added by {}", short(m.added_by));
            }
            let _ = writeln!(out, "{line}");
            if let Some(peer) = &m.peer {
                let _ = writeln!(out, "      {}", peer_line(peer));
            }
        }
        if !self.links.is_empty() {
            let _ = writeln!(out, "\nlinks (as the group reports them):");
            for link in &self.links {
                let _ = writeln!(
                    out,
                    "  {} ↔ {}  {}",
                    short(link.a),
                    short(link.b),
                    link.state
                );
            }
        }
        if !self.folders.is_empty() {
            let _ = writeln!(out, "\nfolders:");
            for f in &self.folders {
                let state = if f.paused {
                    "paused (root or its marker missing)".to_string()
                } else {
                    format!(
                        "{} files, {}, synced with {} nodes",
                        f.stats.files,
                        bytes(f.stats.bytes),
                        f.peers
                    )
                };
                let _ = writeln!(
                    out,
                    "  {}  {}  {state}",
                    f.config.name,
                    f.config.path.display()
                );
            }
        }
        let _ = writeln!(out, "\nids:");
        for m in &self.members {
            let _ = writeln!(out, "  {:width$}  {}", short(m.id), m.id);
        }
        out
    }
}

/// One line about this node's connection to a peer.
pub fn peer_line(peer: &PeerSnapshot) -> String {
    let path = match (&peer.path, peer.connections) {
        (_, 0) => "not connected".to_string(),
        (Some(path), n) => format!("{path}, {n} conn"),
        (None, n) => format!("no path selected, {n} conn"),
    };
    let rtt = peer
        .rtt
        .filter(|_| peer.connections > 0)
        .map(|rtt| format!(", rtt {} ms", rtt.as_millis()))
        .unwrap_or_default();
    let t = &peer.traffic;
    format!(
        "{path}{rtt}; direct ↑{} ↓{}, relay ↑{} ↓{}",
        bytes(t.direct_sent),
        bytes(t.direct_recv),
        bytes(t.relay_sent),
        bytes(t.relay_recv)
    )
}

/// Replaces the status file in one step, so `status` never reads half of it.
pub fn write(config_dir: &Path, status: &Status) -> io::Result<()> {
    let tmp = config_dir.join(format!("{STATUS_FILE}.tmp"));
    let mut text = status.render();
    let _ = writeln!(text, "\nwritten at unix {}", status.generated_at);
    fs::write(&tmp, text)?;
    fs::rename(tmp, config_dir.join(STATUS_FILE))
}
