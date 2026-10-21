//! Content transfer: brings this node's copy of a folder in line with the
//! merged view (see `view`).
//!
//! One task per folder, woken whenever the folder's doc changes and every
//! [`RETRY`] otherwise. Each pass merges every path and acts where this node
//! is behind:
//!
//! * a file this node is missing or holds an older version of is built
//!   piece by piece: from what an earlier try left, then from this node's
//!   own files (the old version at that path first, then any other file
//!   with the same pieces), and the rest downloaded over the peer wire
//!   from the version's swarm (`peer_wire::Client`). Every piece is checked
//!   against the piece hashes. Then the file is placed on disk;
//! * a file that was deleted is deleted here too;
//! * a delete and a new file with the same content, where this node has
//!   nothing at the new path, is a rename: the file is moved, not fetched;
//! * a conflict this node's version lost: its file is renamed to the
//!   conflict copy's name (see `view::conflict_name`) and the winner fetched.
//!   The copy is a new file, which reaches the other nodes as any other.
//!   Nodes holding the winner do nothing; nodes holding neither fetch it.
//!
//! The disk changes themselves are made by the folder's index task
//! ([`Apply`]), which owns this node's entries, so a change and the entry
//! describing it are written together and a local edit not indexed yet is
//! never overwritten.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use iroh::EndpointId;
use iroh_blobs::store::fs::FsStore;
use iroh_blobs::Hash;
use iroh_docs::protocol::Docs;
use iroh_docs::NamespaceId;
use tokio::sync::{mpsc, oneshot, Notify};

use crate::folder::Folders;
use crate::index::{self, safe_path, Apply, Entries, FileMeta};
use crate::member::Names;
use crate::peer_wire::Client;
use crate::pieces::{self, Source, Staged};
use crate::view::{conflict_name, merge, node_state, Candidate, NodeState, Outcome, Version};

/// How often a folder is checked when its doc hasn't changed, so a failed
/// fetch is retried.
pub const RETRY: Duration = Duration::from_secs(10);

/// How long a delete, or a fetch of content this node already holds at
/// another path, waits after this node first plans it. A rename arrives as a
/// delete and a new file, possibly in separate syncs and in either order;
/// waiting lets the other half arrive, so the file is moved rather than
/// deleted and copied.
pub const RENAME_GRACE: Duration = Duration::from_secs(2);

/// How long a staging file must have gone unwritten before it is removed
/// as stale. A pass can't tell which version a staging file is for (its
/// name is a hash), and right after a start, before the folder's doc has
/// synced, no fetch is planned yet: without this, a node would delete the
/// partial downloads it is about to resume.
pub const STALE_STAGING: Duration = Duration::from_secs(24 * 3600);

/// One folder's content task.
pub(crate) struct Content {
    pub name: String,
    pub folder: NamespaceId,
    pub me: EndpointId,
    pub docs: Docs,
    pub blobs: FsStore,
    /// Fetches pieces from other nodes.
    pub client: Client,
    /// The nodes that keep the folder, for each download's swarm.
    pub folders: Folders,
    /// The folder root, where fetched files are staged.
    pub root: PathBuf,
    /// This node's entries, to find local copies of content.
    pub entries: Entries,
    /// Notified whenever the folder's doc changes.
    pub changed: Arc<Notify>,
    /// To the folder's index task.
    pub apply: mpsc::Sender<Apply>,
    /// Members' names, for naming conflict copies.
    pub names: Names,
}

/// What a pass does for one path, or for two in a rename.
enum Action {
    /// Another node renamed `from` to `to`: move this node's file too.
    Move {
        from: String,
        from_mine: FileMeta,
        to: String,
        to_mine: Option<FileMeta>,
        meta: FileMeta,
        tombstone: Version,
    },
    Delete {
        path: String,
        mine: FileMeta,
        version: Version,
    },
    Fetch {
        path: String,
        mine: Option<FileMeta>,
        meta: FileMeta,
        peers: Vec<EndpointId>,
    },
    /// This node's version lost a conflict: keep it as `copy` and fetch the
    /// winner.
    Resolve {
        path: String,
        mine: FileMeta,
        copy: String,
        meta: FileMeta,
        peers: Vec<EndpointId>,
    },
}

/// What a folder's content task remembers between passes.
#[derive(Default)]
struct Memory {
    /// Unsafe paths and failures, logged once each so a retry every pass
    /// doesn't flood the log.
    refused: HashSet<String>,
    failed: HashSet<String>,
    /// When each action waiting for `RENAME_GRACE` was first planned.
    waiting: HashMap<String, Instant>,
    /// Fetches that failed, by path and version, and when to try again:
    /// not on every change to the folder, which would ask a node that sent
    /// bad pieces again and again.
    retry_at: HashMap<String, Instant>,
    /// The staging files this task has fetched into: one whose version is
    /// no longer wanted is removed at the next pass, not after
    /// [`STALE_STAGING`].
    staging: HashSet<PathBuf>,
}

impl Content {
    /// Runs until the index task stops taking changes.
    pub async fn run(self) {
        let mut memory = Memory::default();
        loop {
            let wait = match self.pass(&mut memory).await {
                Ok(Some(wait)) => wait,
                Ok(None) => return,
                Err(err) => {
                    tracing::warn!("folder {}: content: {err:#}", self.name);
                    RETRY
                }
            };
            tokio::select! {
                () = self.changed.notified() => {}
                () = tokio::time::sleep(wait) => {}
            }
        }
    }

    /// One pass over the folder. Returns how long to wait for the next one
    /// if nothing changes, or `None` once the index task is gone.
    async fn pass(&self, memory: &mut Memory) -> anyhow::Result<Option<Duration>> {
        let entries = index::read_all(&self.docs, &self.blobs, self.folder).await?;
        // Content this node holds, at some path.
        let held: HashSet<Hash> = entries
            .values()
            .filter_map(|per_node| per_node.get(&self.me)?.content)
            .collect();
        let now = Instant::now();
        let mut wait = RETRY;
        let mut waiting = HashMap::new();
        let mut ready = Vec::new();
        let actions = self.plan(entries, &mut memory.refused);
        self.remove_stale_staging(&actions, &mut memory.staging)
            .await;
        for action in actions {
            // Either half of a rename whose other half may not have arrived
            // yet waits RENAME_GRACE.
            let key = match &action {
                Action::Delete { path, .. } => Some(format!("delete {path}")),
                Action::Fetch { path, meta, .. }
                    if meta.content.is_some_and(|h| held.contains(&h)) =>
                {
                    Some(format!("fetch {path}"))
                }
                _ => None,
            };
            if let Some(key) = key {
                let first = memory.waiting.get(&key).copied().unwrap_or(now);
                waiting.insert(key, first);
                let age = now - first;
                if age < RENAME_GRACE {
                    wait = wait.min(RENAME_GRACE - age);
                    continue;
                }
            }
            ready.push(action);
        }
        memory.waiting = waiting;
        // Moves first, so a moved file is never deleted before it moves;
        // then deletes, which are quick and free space; then fetches.
        for action in ready {
            if !self.act(action, memory).await? {
                return Ok(None);
            }
        }
        Ok(Some(wait))
    }

    /// Removes the staging files at the folder root that none of `actions`
    /// will fetch into: at once for those this task fetched into (in
    /// `staging`), else once nothing has written to them for
    /// [`STALE_STAGING`]. They were left by a fetch of a version no longer
    /// wanted (a newer one came, or the file was deleted). One for a version
    /// still wanted stays, for the fetch to resume. Runs between passes'
    /// actions, so no fetch is writing.
    async fn remove_stale_staging(&self, actions: &[Action], staging: &mut HashSet<PathBuf>) {
        let wanted: HashSet<PathBuf> = actions
            .iter()
            .filter_map(|action| match action {
                Action::Fetch { path, meta, .. } | Action::Resolve { path, meta, .. } => {
                    let content = meta.content?;
                    Some(pieces::staging_path(&self.root, path, content))
                }
                _ => None,
            })
            .collect();
        let superseded: Vec<PathBuf> = staging.difference(&wanted).cloned().collect();
        staging.retain(|path| wanted.contains(path));
        let (root, swarms) = (self.root.clone(), self.client.swarms());
        let removed = tokio::task::spawn_blocking(move || {
            let mut removed = Vec::new();
            for path in superseded {
                // Gone already if the fetch completed and the file was placed.
                if std::fs::remove_file(&path).is_ok() {
                    removed.push(path);
                }
            }
            for entry in std::fs::read_dir(&root)?.flatten() {
                let name = entry.file_name();
                let staging = name.to_str().is_some_and(pieces::is_staging_name);
                let path = entry.path();
                let old = entry
                    .metadata()
                    .and_then(|m| m.modified())
                    .is_ok_and(|at| at.elapsed().is_ok_and(|age| age > STALE_STAGING));
                if staging
                    && old
                    && !wanted.contains(&path)
                    && !swarms.uses(&path)
                    && std::fs::remove_file(&path).is_ok()
                {
                    removed.push(path);
                }
            }
            std::io::Result::Ok(removed)
        })
        .await;
        match removed {
            Ok(Ok(removed)) => {
                for path in removed {
                    tracing::info!("folder {}: removed stale {}", self.name, path.display());
                }
            }
            Ok(Err(err)) => tracing::debug!("folder {}: listing staging files: {err}", self.name),
            Err(err) => tracing::debug!("folder {}: listing staging files: {err}", self.name),
        }
    }

    /// What this node must do to match the merged view of `entries`.
    fn plan(
        &self,
        entries: BTreeMap<String, BTreeMap<EndpointId, FileMeta>>,
        refused: &mut HashSet<String>,
    ) -> Vec<Action> {
        let (mut deletes, mut fetches, mut resolves) = (Vec::new(), Vec::new(), Vec::new());
        for (path, per_node) in entries {
            let metas: Vec<FileMeta> = per_node.values().cloned().collect();
            let Some(outcome) = merge(&metas) else {
                continue;
            };
            if !safe_path(&path) {
                if refused.insert(path.clone()) {
                    tracing::warn!("folder {}: refusing unsafe path {path:?}", self.name);
                }
                continue;
            }
            let mine = per_node.get(&self.me).cloned();
            match (&outcome, node_state(mine.as_ref(), &outcome)) {
                (Outcome::File(c), NodeState::Missing | NodeState::Outdated) => {
                    let (meta, peers) = self.take(mine.as_ref(), &metas, c);
                    fetches.push(Action::Fetch {
                        path,
                        mine,
                        meta,
                        peers,
                    });
                }
                (Outcome::Deleted, NodeState::WillDelete) => {
                    let Some(mine) = mine else { continue };
                    let mut version = Version::default();
                    for m in &metas {
                        version.merge(&m.version);
                    }
                    deletes.push(Action::Delete {
                        path,
                        mine,
                        version,
                    });
                }
                (Outcome::Conflict { winner, others }, _) => {
                    let held = mine.as_ref().and_then(|m| m.content);
                    if held == Some(winner.content) {
                        // The losers keep their versions as conflict copies,
                        // which arrive here as new files.
                        continue;
                    }
                    let (meta, peers) = self.take(mine.as_ref(), &metas, winner);
                    let lost = others.iter().find(|o| Some(o.content) == held);
                    match (mine, lost) {
                        (Some(mine), Some(lost)) => {
                            // Named after the losing version's newest copy, so
                            // every node holding it picks the same name.
                            let label = self.names.display(lost.node);
                            resolves.push(Action::Resolve {
                                copy: conflict_name(&path, lost, &label),
                                path,
                                mine,
                                meta,
                                peers,
                            });
                        }
                        (mine, _) => fetches.push(Action::Fetch {
                            path,
                            mine,
                            meta,
                            peers,
                        }),
                    }
                }
                _ => {}
            }
        }
        // A delete and a fetch of the same content, where this node has
        // nothing at the new path, is a rename: move the file instead.
        let mut moves = Vec::new();
        let mut kept = Vec::new();
        for fetch in fetches {
            let Action::Fetch {
                path: to,
                mine: to_mine,
                meta,
                peers,
            } = fetch
            else {
                unreachable!("only fetches");
            };
            let renamed = deletes.iter().position(
                |d| matches!(d, Action::Delete { mine, .. } if mine.content == meta.content),
            );
            match renamed {
                Some(i) if to_mine.as_ref().is_none_or(FileMeta::is_deleted) => {
                    let Action::Delete {
                        path: from,
                        mine: from_mine,
                        version: tombstone,
                    } = deletes.remove(i)
                    else {
                        unreachable!("only deletes");
                    };
                    moves.push(Action::Move {
                        from,
                        from_mine,
                        to,
                        to_mine,
                        meta,
                        tombstone,
                    });
                }
                _ => kept.push(Action::Fetch {
                    path: to,
                    mine: to_mine,
                    meta,
                    peers,
                }),
            }
        }
        moves
            .into_iter()
            .chain(deletes)
            .chain(kept)
            .chain(resolves)
            .collect()
    }

    /// The entry this node writes when it takes candidate `c` for a path:
    /// its version has seen this node's own entry and every entry holding
    /// `c`'s content, and its piece hashes are theirs (the content's). Also
    /// returns the other nodes holding that content.
    fn take(
        &self,
        mine: Option<&FileMeta>,
        metas: &[FileMeta],
        c: &Candidate,
    ) -> (FileMeta, Vec<EndpointId>) {
        let mut version = mine.map(|m| m.version.clone()).unwrap_or_default();
        let mut peers = Vec::new();
        let mut pieces = (0, Vec::new());
        for m in metas.iter().filter(|m| m.content == Some(c.content)) {
            version.merge(&m.version);
            pieces = (m.piece_length, m.pieces.clone());
            if m.node != self.me {
                peers.push(m.node);
            }
        }
        let meta = FileMeta {
            content: Some(c.content),
            size: c.size,
            mtime_ns: c.mtime_ns,
            node: self.me,
            version,
            piece_length: pieces.0,
            pieces: pieces.1,
        };
        (meta, peers)
    }

    /// Carries out one action. Returns `false` once the index task is gone.
    async fn act(&self, action: Action, memory: &mut Memory) -> anyhow::Result<bool> {
        let name = &self.name;
        let (done, result) = oneshot::channel();
        let (request, what) = match action {
            Action::Move {
                from,
                from_mine,
                to,
                to_mine,
                meta,
                tombstone,
            } => {
                let what = format!("moving {from} to {to}");
                let request = Apply::Move {
                    from,
                    from_expect: from_mine,
                    to,
                    to_expect: to_mine,
                    meta,
                    tombstone,
                    done,
                };
                (request, what)
            }
            Action::Delete {
                path,
                mine,
                version,
            } => {
                let what = format!("deleting {path}");
                let request = Apply::Delete {
                    path,
                    expect: mine,
                    version,
                    done,
                };
                (request, what)
            }
            Action::Fetch {
                path,
                mine,
                meta,
                peers,
            } => {
                let Some(temp) = self.fetch(&path, &meta, peers, memory).await? else {
                    return Ok(true);
                };
                let what = format!("writing {path}");
                let request = Apply::Place {
                    path,
                    expect: mine,
                    meta,
                    temp,
                    done,
                };
                (request, what)
            }
            Action::Resolve {
                path,
                mine,
                copy,
                meta,
                peers,
            } => {
                let Some(temp) = self.fetch(&path, &meta, peers, memory).await? else {
                    return Ok(true);
                };
                let what = format!(
                    "resolving the conflict on {path}: keeping this node's version as {copy}"
                );
                let request = Apply::Resolve {
                    path,
                    expect: mine,
                    copy,
                    meta,
                    temp,
                    done,
                };
                (request, what)
            }
        };
        let Some(applied) = self.send(request, result).await else {
            return Ok(false);
        };
        match applied {
            Ok(true) => tracing::info!("folder {name}: {what}"),
            Ok(false) => tracing::debug!("folder {name}: {what}: the path changed; skipped"),
            Err(err) => {
                if memory.failed.insert(what.clone()) {
                    tracing::warn!("folder {name}: {what}: {err:#}");
                } else {
                    tracing::debug!("folder {name}: {what}: {err:#}");
                }
            }
        }
        Ok(true)
    }

    /// Gets version `meta` of `path` into its staging file (see
    /// `pieces::staging_path`): the pieces this node already holds are
    /// copied from its own files, the rest fetched from `peers`. Returns
    /// the file once it is complete; the index task checks it before placing
    /// it.
    async fn fetch(
        &self,
        path: &str,
        meta: &FileMeta,
        peers: Vec<EndpointId>,
        memory: &mut Memory,
    ) -> anyhow::Result<Option<PathBuf>> {
        let content = meta.content.expect("fetching a file with content");
        let temp = pieces::staging_path(&self.root, path, content);
        let key = format!("fetching {path} {content}");
        let waiting = memory
            .retry_at
            .get(&key)
            .is_some_and(|at| Instant::now() < *at);
        if waiting {
            return Ok(None);
        }
        let sources = self.sources(path, meta);
        memory.staging.insert(temp.clone());
        let (staging, wanted, root) = (temp.clone(), meta.clone(), self.root.clone());
        let staged =
            tokio::task::spawn_blocking(move || Staged::open(&staging, &wanted, &root, &sources))
                .await?;
        let done = match staged {
            Err(err) => Err(anyhow::Error::from(err).context("staging")),
            Ok(staged) if staged.is_complete() => {
                tokio::task::spawn_blocking(move || staged.sync())
                    .await?
                    .map(|()| true)
                    .map_err(anyhow::Error::from)
            }
            Ok(_) if peers.is_empty() => {
                // Nobody to ask: try again later, quietly.
                memory.retry_at.insert(key, Instant::now() + RETRY);
                return Ok(None);
            }
            Ok(staged) => {
                let from: Vec<String> = peers.iter().map(|p| self.names.display(*p)).collect();
                let copied = match staged.copied {
                    0 => String::new(),
                    n => format!("; {} copied locally", crate::peers::bytes(n)),
                };
                tracing::info!(
                    "folder {}: fetching {path} ({}{copied}) from {}",
                    self.name,
                    crate::peers::bytes(staged.to_fetch),
                    from.join(", ")
                );
                self.fetch_from(path, meta, peers.clone(), staged).await
            }
        };
        let err = match done {
            Ok(true) => {
                memory.retry_at.remove(&key);
                return Ok(Some(temp));
            }
            Ok(false) => anyhow::anyhow!("no node could send all of it; trying again later"),
            Err(err) => err,
        };
        memory.retry_at.insert(key.clone(), Instant::now() + RETRY);
        if memory.failed.insert(key) {
            tracing::warn!("folder {}: fetching {path}: {err:#}", self.name);
        } else {
            tracing::debug!("folder {}: fetching {path}: {err:#}", self.name);
        }
        Ok(None)
    }

    /// Downloads what `staged` lacks over the peer wire, from the swarm of
    /// version `meta` of `path`: `peers` (the nodes whose entry has it) and
    /// every other node that keeps the folder, asked again during the
    /// download (see [`Client::download`]). The folder's doc is watched
    /// meanwhile: once another version of `path` wins (or it is deleted),
    /// the download stops and its staging file is removed.
    async fn fetch_from(
        &self,
        path: &str,
        meta: &FileMeta,
        peers: Vec<EndpointId>,
        staged: Staged,
    ) -> anyhow::Result<bool> {
        let content = meta.content.expect("fetching a file with content");
        // Whether the watch took a wake meant for `run`.
        let woken = AtomicBool::new(false);
        let superseded = async {
            loop {
                self.changed.notified().await;
                woken.store(true, Ordering::Relaxed);
                let per_node =
                    match index::read_path(&self.docs, &self.blobs, self.folder, path).await {
                        Ok(per_node) => per_node,
                        Err(err) => {
                            tracing::debug!("folder {}: reading {path}: {err:#}", self.name);
                            continue;
                        }
                    };
                let metas: Vec<FileMeta> = per_node.into_values().collect();
                let winner = match merge(&metas) {
                    Some(Outcome::File(winner) | Outcome::Conflict { winner, .. }) => {
                        Some(winner.content)
                    }
                    Some(Outcome::Deleted) | None => None,
                };
                if winner != Some(content) {
                    return;
                }
            }
        };
        let swarm = || {
            let mut nodes = self.folders.folder_peers(self.folder);
            nodes.extend(peers.iter().copied());
            nodes.sort();
            nodes.dedup();
            nodes
        };
        let download = self.client.download(self.folder, path, meta, staged, swarm);
        let fetched = tokio::select! {
            fetched = download => fetched,
            () = superseded => {
                let temp = pieces::staging_path(&self.root, path, content);
                tracing::info!(
                    "folder {}: {path}: a newer version came; stopped fetching this one",
                    self.name
                );
                if let Err(err) = tokio::fs::remove_file(&temp).await {
                    tracing::debug!("folder {}: removing {}: {err}", self.name, temp.display());
                }
                Ok(false)
            }
        };
        if woken.load(Ordering::Relaxed) {
            // Other changes may have come with it: let the next pass see them.
            self.changed.notify_one();
        }
        fetched
    }

    /// This node's files that pieces of version `meta` of `path` can be
    /// copied from: the old version at `path` first, then every other file
    /// sharing a piece with it.
    fn sources(&self, path: &str, meta: &FileMeta) -> Vec<Source> {
        let wanted: HashSet<Hash> = meta.pieces.iter().copied().collect();
        if wanted.is_empty() {
            return Vec::new();
        }
        self.entries.with(|entries| {
            let shares = |m: &FileMeta| m.pieces.iter().any(|h| wanted.contains(h));
            let old = entries.get_key_value(path).filter(|(_, m)| shares(m));
            let others = entries.iter().filter(|(p, m)| *p != path && shares(m));
            old.into_iter()
                .chain(others)
                .map(|(p, m)| (p.clone(), m.clone()))
                .collect()
        })
    }

    /// Sends `request` to the index task and waits for its answer. `None` if
    /// the index task is gone.
    async fn send(
        &self,
        request: Apply,
        result: oneshot::Receiver<anyhow::Result<bool>>,
    ) -> Option<anyhow::Result<bool>> {
        self.apply.send(request).await.ok()?;
        result.await.ok()
    }
}
