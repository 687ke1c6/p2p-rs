//! Content transfer: brings this node's copy of a folder in line with the
//! merged view (see `view`). On while `run --with-content` is given.
//!
//! One task per folder, woken whenever the folder's doc changes and every
//! [`RETRY`] otherwise. Each pass merges every path and acts where this node
//! is behind:
//!
//! * a file this node is missing or holds an older version of is fetched by
//!   hash from the nodes holding it (`iroh-blobs` verifies it while streaming
//!   and resumes a partial download from what the store already has), then
//!   placed on disk;
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
use std::sync::Arc;
use std::time::{Duration, Instant};

use iroh::EndpointId;
use iroh_blobs::api::downloader::Downloader;
use iroh_blobs::store::fs::FsStore;
use iroh_blobs::Hash;
use iroh_docs::protocol::Docs;
use iroh_docs::NamespaceId;
use tokio::sync::{mpsc, oneshot, Notify};

use crate::index::{self, safe_path, Apply, FileMeta};
use crate::member::Names;
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

/// One folder's content task.
pub(crate) struct Content {
    pub name: String,
    pub folder: NamespaceId,
    pub me: EndpointId,
    pub docs: Docs,
    pub blobs: FsStore,
    pub downloader: Downloader,
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
        providers: Vec<EndpointId>,
    },
    /// This node's version lost a conflict: keep it as `copy` and fetch the
    /// winner.
    Resolve {
        path: String,
        mine: FileMeta,
        copy: String,
        meta: FileMeta,
        providers: Vec<EndpointId>,
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
        for action in self.plan(entries, &mut memory.refused) {
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
            if !self.act(action, &mut memory.failed).await? {
                return Ok(None);
            }
        }
        Ok(Some(wait))
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
                    let (meta, providers) = self.take(mine.as_ref(), &metas, c);
                    fetches.push(Action::Fetch {
                        path,
                        mine,
                        meta,
                        providers,
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
                    let (meta, providers) = self.take(mine.as_ref(), &metas, winner);
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
                                providers,
                            });
                        }
                        (mine, _) => fetches.push(Action::Fetch {
                            path,
                            mine,
                            meta,
                            providers,
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
                providers,
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
                    providers,
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
    /// `c`'s content. Also returns the other nodes holding that content.
    fn take(
        &self,
        mine: Option<&FileMeta>,
        metas: &[FileMeta],
        c: &Candidate,
    ) -> (FileMeta, Vec<EndpointId>) {
        let mut version = mine.map(|m| m.version.clone()).unwrap_or_default();
        let mut providers = Vec::new();
        for m in metas.iter().filter(|m| m.content == Some(c.content)) {
            version.merge(&m.version);
            if m.node != self.me {
                providers.push(m.node);
            }
        }
        let meta = FileMeta {
            content: Some(c.content),
            size: c.size,
            mtime_ns: c.mtime_ns,
            node: self.me,
            version,
        };
        (meta, providers)
    }

    /// Carries out one action. Returns `false` once the index task is gone.
    async fn act(&self, action: Action, failed: &mut HashSet<String>) -> anyhow::Result<bool> {
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
                providers,
            } => {
                let hash = meta.content.expect("a fetch has content");
                if !self
                    .fetch(&path, hash, meta.size, providers, failed)
                    .await?
                {
                    return Ok(true);
                }
                let what = format!("writing {path}");
                let request = Apply::Place {
                    path,
                    expect: mine,
                    meta,
                    done,
                };
                (request, what)
            }
            Action::Resolve {
                path,
                mine,
                copy,
                meta,
                providers,
            } => {
                let hash = meta.content.expect("a winner has content");
                if !self
                    .fetch(&path, hash, meta.size, providers, failed)
                    .await?
                {
                    return Ok(true);
                }
                let what = format!(
                    "resolving the conflict on {path}: keeping this node's version as {copy}"
                );
                let request = Apply::Resolve {
                    path,
                    expect: mine,
                    copy,
                    meta,
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
                if failed.insert(what.clone()) {
                    tracing::warn!("folder {name}: {what}: {err:#}");
                } else {
                    tracing::debug!("folder {name}: {what}: {err:#}");
                }
            }
        }
        Ok(true)
    }

    /// Makes sure blob `hash` is complete in the store, downloading it from
    /// `providers` if not. Returns whether it is.
    async fn fetch(
        &self,
        path: &str,
        hash: Hash,
        size: u64,
        providers: Vec<EndpointId>,
        failed: &mut HashSet<String>,
    ) -> anyhow::Result<bool> {
        if self.blobs.has(hash).await? {
            return Ok(true);
        }
        if providers.is_empty() {
            return Ok(false);
        }
        let from: Vec<String> = providers
            .iter()
            .map(|p| p.fmt_short().to_string())
            .collect();
        tracing::info!(
            "folder {}: fetching {path} ({}) from {}",
            self.name,
            crate::peers::bytes(size),
            from.join(", ")
        );
        let result = self.downloader.download(hash, providers).await;
        if let Err(err) = &result {
            if failed.insert(format!("fetching {path} {hash}")) {
                tracing::warn!("folder {}: fetching {path}: {err:#}", self.name);
            } else {
                tracing::debug!("folder {}: fetching {path}: {err:#}", self.name);
            }
        }
        Ok(self.blobs.has(hash).await?)
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
