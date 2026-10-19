//! Version vectors and the merged view of a folder.
//!
//! Every node writes its own entries to a folder's doc, describing its own
//! disk. The merged view, what the folder should look like once synced, is
//! computed from all of them and never stored, so every node holding the same
//! entries computes the same view. For each path:
//!
//! * drop every version another version came after (its vector dominates);
//! * versions left with the same content agree;
//! * one content left: that's the result, deleted if it's a tombstone;
//! * live and deleted left: the edit wins and the file stays;
//! * several live contents left: a conflict. The newest modification time
//!   keeps the name (ties: the smaller node ID); the others are kept beside it
//!   as `<stem>.sync-conflict-<YYYYMMDD-HHMMSS>-<node>.<ext>`.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

use iroh::EndpointId;
use iroh_blobs::Hash;
use iroh_docs::NamespaceId;
use serde::{Deserialize, Serialize};

use crate::index::FileMeta;

/// How many edits each node has made to a file, as far as one version knows.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Version(pub BTreeMap<EndpointId, u64>);

impl Version {
    /// Whether this version has seen everything `other` has.
    pub fn dominates(&self, other: &Version) -> bool {
        other
            .0
            .iter()
            .all(|(node, n)| self.0.get(node).is_some_and(|m| m >= n))
    }

    /// Whether this version came after `other`.
    pub fn after(&self, other: &Version) -> bool {
        self != other && self.dominates(other)
    }

    /// Takes in everything `other` has seen.
    pub fn merge(&mut self, other: &Version) {
        for (node, n) in &other.0 {
            let mine = self.0.entry(*node).or_default();
            *mine = (*mine).max(*n);
        }
    }

    /// This version with one more edit by `node`.
    pub fn bumped(mut self, node: EndpointId) -> Self {
        *self.0.entry(node).or_default() += 1;
        self
    }
}

/// One version of a file that survives the merge.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Candidate {
    /// A node holding this version (of those that do, the one whose copy is
    /// newest).
    pub node: EndpointId,
    pub content: Hash,
    pub size: u64,
    pub mtime_ns: u64,
}

/// What a path becomes once the folder has synced.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Outcome {
    File(Candidate),
    Deleted,
    /// `winner` keeps the name; each of `others` is kept as a conflict copy.
    Conflict {
        winner: Candidate,
        others: Vec<Candidate>,
    },
}

/// Where one node stands on a path, against the outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeState {
    /// Has the version the path ends up with (or agrees it's deleted).
    Current,
    /// Doesn't have the file: will fetch it.
    Missing,
    /// Has another version: will replace it (for a conflict, keeping its
    /// own version as a conflict copy if it is one of the others).
    Outdated,
    /// Has the file, which is deleted: will delete it.
    WillDelete,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PathView {
    pub path: String,
    pub outcome: Outcome,
    /// Every node in the folder.
    pub nodes: BTreeMap<EndpointId, NodeState>,
}

impl PathView {
    pub fn in_sync(&self) -> bool {
        self.nodes.values().all(|s| *s == NodeState::Current)
    }

    fn with(&self, state: NodeState) -> Vec<EndpointId> {
        self.nodes
            .iter()
            .filter(|(_, s)| **s == state)
            .map(|(id, _)| *id)
            .collect()
    }
}

/// Merges the entries for one path, one per node. `None` if there are none.
pub fn merge(entries: &[FileMeta]) -> Option<Outcome> {
    let latest: Vec<&FileMeta> = entries
        .iter()
        .filter(|e| !entries.iter().any(|o| o.version.after(&e.version)))
        .collect();
    if latest.is_empty() {
        return None;
    }
    // One candidate per live content: its newest copy.
    let mut live: BTreeMap<Hash, Candidate> = BTreeMap::new();
    for e in &latest {
        let Some(content) = e.content else { continue };
        let candidate = Candidate {
            node: e.node,
            content,
            size: e.size,
            mtime_ns: e.mtime_ns,
        };
        live.entry(content)
            .and_modify(|c| {
                if rank(&candidate) < rank(c) {
                    *c = candidate.clone();
                }
            })
            .or_insert(candidate);
    }
    let mut candidates: Vec<Candidate> = live.into_values().collect();
    candidates.sort_by_key(rank);
    let mut candidates = candidates.into_iter();
    Some(match candidates.next() {
        None => Outcome::Deleted,
        Some(winner) => {
            let others: Vec<Candidate> = candidates.collect();
            if others.is_empty() {
                Outcome::File(winner)
            } else {
                Outcome::Conflict { winner, others }
            }
        }
    })
}

/// Orders candidates: the newest first, then the smaller node ID.
fn rank(c: &Candidate) -> (Reverse<u64>, EndpointId) {
    (Reverse(c.mtime_ns), c.node)
}

/// Where `entry` (a node's own entry, if it has one) stands against `outcome`.
pub fn node_state(entry: Option<&FileMeta>, outcome: &Outcome) -> NodeState {
    let content = entry.and_then(|e| e.content);
    let target = match outcome {
        Outcome::Deleted => {
            return match content {
                Some(_) => NodeState::WillDelete,
                None => NodeState::Current,
            };
        }
        Outcome::File(c) => c.content,
        Outcome::Conflict { winner, .. } => winner.content,
    };
    match content {
        Some(h) if h == target => match outcome {
            // Also needs the conflict copies.
            Outcome::Conflict { .. } => NodeState::Outdated,
            _ => NodeState::Current,
        },
        Some(_) => NodeState::Outdated,
        None => NodeState::Missing,
    }
}

/// The merged view of a folder.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FolderView {
    pub folder: NamespaceId,
    pub name: String,
    /// The nodes keeping the folder.
    pub nodes: BTreeSet<EndpointId>,
    /// Every path any node has an entry for, in order.
    pub paths: Vec<PathView>,
}

impl FolderView {
    /// Builds the view from every node's entries: path → (node → entry).
    /// `nodes` are the nodes keeping the folder; any node with an entry
    /// counts too.
    pub fn build(
        folder: NamespaceId,
        name: String,
        mut nodes: BTreeSet<EndpointId>,
        entries: BTreeMap<String, BTreeMap<EndpointId, FileMeta>>,
    ) -> Self {
        for per_node in entries.values() {
            nodes.extend(per_node.keys().copied());
        }
        let paths = entries
            .into_iter()
            .filter_map(|(path, per_node)| {
                let metas: Vec<FileMeta> = per_node.values().cloned().collect();
                let outcome = merge(&metas)?;
                let states = nodes
                    .iter()
                    .map(|id| (*id, node_state(per_node.get(id), &outcome)))
                    .collect();
                Some(PathView {
                    path,
                    outcome,
                    nodes: states,
                })
            })
            .collect();
        Self {
            folder,
            name,
            nodes,
            paths,
        }
    }

    pub fn path(&self, path: &str) -> Option<&PathView> {
        self.paths.iter().find(|p| p.path == path)
    }

    /// Renders the view, naming nodes with `label`. Paths already in sync
    /// are left out unless `all`.
    pub fn render(&self, label: &dyn Fn(EndpointId) -> String, all: bool) -> String {
        let list = |ids: &[EndpointId]| {
            ids.iter()
                .map(|id| label(*id))
                .collect::<Vec<_>>()
                .join(", ")
        };
        let mut out = String::new();
        let nodes: Vec<_> = self.nodes.iter().copied().collect();
        let _ = writeln!(
            out,
            "folder {}  ({} nodes: {})",
            self.name,
            nodes.len(),
            list(&nodes)
        );
        let (mut synced, mut fetch, mut delete, mut conflict, mut gone) = (0, 0, 0, 0, 0);
        for p in &self.paths {
            match (&p.outcome, p.in_sync()) {
                (Outcome::Deleted, true) => gone += 1,
                (_, true) => synced += 1,
                (Outcome::Conflict { .. }, false) => conflict += 1,
                (Outcome::Deleted, false) => delete += 1,
                (Outcome::File(_), false) => fetch += 1,
            }
        }
        let _ = writeln!(
            out,
            "{} files: {synced} in sync, {fetch} to fetch, {delete} to delete, {conflict} conflicts ({gone} deleted everywhere)",
            synced + fetch + conflict
        );
        let width = self
            .paths
            .iter()
            .map(|p| p.path.chars().count())
            .max()
            .unwrap_or(0);
        let mut first = true;
        for p in &self.paths {
            let in_sync = p.in_sync();
            if in_sync && !all {
                continue;
            }
            let line = match &p.outcome {
                Outcome::Deleted if in_sync => "deleted everywhere".to_string(),
                Outcome::Deleted => {
                    format!(
                        "deleted; {} will delete it",
                        list(&p.with(NodeState::WillDelete))
                    )
                }
                Outcome::File(_) if in_sync => "in sync".to_string(),
                Outcome::File(c) => {
                    let behind: Vec<_> = p
                        .nodes
                        .iter()
                        .filter(|(_, s)| **s != NodeState::Current)
                        .map(|(id, _)| *id)
                        .collect();
                    let mut has = p.with(NodeState::Current);
                    if has.is_empty() {
                        has.push(c.node);
                    }
                    format!("from {}; {} will fetch it", list(&has), list(&behind))
                }
                Outcome::Conflict { winner, others } => {
                    let mut line =
                        format!("conflict: {}'s version keeps the name", label(winner.node));
                    for other in others {
                        let _ = write!(
                            line,
                            "; {}'s becomes {}",
                            label(other.node),
                            conflict_name(&p.path, other, &label(other.node))
                        );
                    }
                    line
                }
            };
            if first {
                out.push('\n');
                first = false;
            }
            let _ = writeln!(out, "  {:width$}  {line}", p.path);
        }
        out
    }
}

/// The name `candidate`'s version of `path` is kept under in a conflict:
/// `<stem>.sync-conflict-<YYYYMMDD-HHMMSS>-<node>.<ext>`, in the same
/// directory, with its modification time in UTC.
pub fn conflict_name(path: &str, candidate: &Candidate, node_label: &str) -> String {
    let (dir, file) = match path.rsplit_once('/') {
        Some((dir, file)) => (format!("{dir}/"), file),
        None => (String::new(), path),
    };
    let (stem, ext) = match file.rsplit_once('.') {
        Some((stem, ext)) if !stem.is_empty() => (stem, format!(".{ext}")),
        _ => (file, String::new()),
    };
    let node: String = node_label
        .chars()
        .map(|c| if c == '/' || c.is_control() { '_' } else { c })
        .collect();
    format!(
        "{dir}{stem}.sync-conflict-{}-{node}{ext}",
        utc_stamp(candidate.mtime_ns)
    )
}

/// Renders every node's raw entries, path by path: content hash (hex, as
/// `b3sum` prints it) or `deleted`, size, modification time (UTC, to the
/// nanosecond) and version vector. Nodes are named with `label`.
pub fn render_entries(
    entries: &BTreeMap<String, BTreeMap<EndpointId, FileMeta>>,
    label: &dyn Fn(EndpointId) -> String,
) -> String {
    let mut out = String::new();
    let count: usize = entries.values().map(BTreeMap::len).sum();
    let _ = writeln!(out, "{count} entries for {} paths", entries.len());
    let width = entries
        .values()
        .flat_map(BTreeMap::keys)
        .map(|id| label(*id).chars().count())
        .max()
        .unwrap_or(0);
    for (path, per_node) in entries {
        let _ = writeln!(out, "\n{path}");
        for (id, meta) in per_node {
            let content = match meta.content {
                Some(hash) => hash.to_string(),
                None => "deleted".to_string(),
            };
            let version: Vec<_> = meta
                .version
                .0
                .iter()
                .map(|(node, n)| format!("{}:{n}", label(*node)))
                .collect();
            let _ = writeln!(
                out,
                "  {:width$}  {content:64}  {:>10} B  {}.{:09}  {{{}}}",
                label(*id),
                meta.size,
                utc_stamp(meta.mtime_ns),
                meta.mtime_ns % 1_000_000_000,
                version.join(", ")
            );
        }
    }
    out
}

/// `YYYYMMDD-HHMMSS` in UTC for unix nanoseconds.
fn utc_stamp(ns: u64) -> String {
    let secs = ns / 1_000_000_000;
    let (days, rem) = (secs / 86_400, secs % 86_400);
    let (h, m, s) = (rem / 3600, rem % 3600 / 60, rem % 60);
    // Days since 1970-01-01 to a civil date (Howard Hinnant's algorithm).
    let z = days as i64 + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!("{year:04}{month:02}{day:02}-{h:02}{m:02}{s:02}")
}
