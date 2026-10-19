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
//! * a file that was deleted is deleted here too.
//!
//! The disk changes themselves are made by the folder's index task
//! ([`Apply`]), which owns this node's entries, so a change and the entry
//! describing it are written together and a local edit not indexed yet is
//! never overwritten. Conflicts are left alone until step 7.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use iroh::EndpointId;
use iroh_blobs::api::downloader::Downloader;
use iroh_blobs::store::fs::FsStore;
use iroh_blobs::Hash;
use iroh_docs::protocol::Docs;
use iroh_docs::NamespaceId;
use tokio::sync::{mpsc, oneshot, Notify};

use crate::index::{self, safe_path, Apply, FileMeta};
use crate::view::{merge, node_state, NodeState, Outcome, Version};

/// How often a folder is checked when its doc hasn't changed, so a failed
/// fetch is retried.
pub const RETRY: Duration = Duration::from_secs(10);

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
}

/// What a pass does for one path.
enum Action {
    Fetch {
        path: String,
        mine: Option<FileMeta>,
        meta: FileMeta,
        providers: Vec<EndpointId>,
    },
    Delete {
        path: String,
        mine: FileMeta,
        version: Version,
    },
}

impl Content {
    /// Runs until the index task stops taking changes.
    pub async fn run(self) {
        // Logged once each, so a retry every pass doesn't flood the log.
        let mut refused = HashSet::new();
        let mut failed = HashSet::new();
        loop {
            match self.pass(&mut refused, &mut failed).await {
                Ok(true) => {}
                Ok(false) => return,
                Err(err) => tracing::warn!("folder {}: content: {err:#}", self.name),
            }
            tokio::select! {
                () = self.changed.notified() => {}
                () = tokio::time::sleep(RETRY) => {}
            }
        }
    }

    /// One pass over the folder. Returns `false` once the index task is gone.
    async fn pass(
        &self,
        refused: &mut HashSet<String>,
        failed: &mut HashSet<(String, Hash)>,
    ) -> anyhow::Result<bool> {
        let entries = index::read_all(&self.docs, &self.blobs, self.folder).await?;
        let mut actions = Vec::new();
        for (path, per_node) in entries {
            let metas: Vec<FileMeta> = per_node.values().cloned().collect();
            let Some(outcome) = merge(&metas) else {
                continue;
            };
            let mine = per_node.get(&self.me).cloned();
            let action = match (&outcome, node_state(mine.as_ref(), &outcome)) {
                (Outcome::File(c), NodeState::Missing | NodeState::Outdated) => {
                    // This node's version has seen its own entry and every
                    // copy of the content it takes.
                    let mut version = mine.as_ref().map(|m| m.version.clone()).unwrap_or_default();
                    for m in metas.iter().filter(|m| m.content == Some(c.content)) {
                        version.merge(&m.version);
                    }
                    let providers = metas
                        .iter()
                        .filter(|m| m.content == Some(c.content) && m.node != self.me)
                        .map(|m| m.node)
                        .collect();
                    let meta = FileMeta {
                        content: Some(c.content),
                        size: c.size,
                        mtime_ns: c.mtime_ns,
                        node: self.me,
                        version,
                    };
                    Action::Fetch {
                        path: path.clone(),
                        mine,
                        meta,
                        providers,
                    }
                }
                (Outcome::Deleted, NodeState::WillDelete) => {
                    let mut version = Version::default();
                    for m in &metas {
                        version.merge(&m.version);
                    }
                    let Some(mine) = mine else { continue };
                    Action::Delete {
                        path: path.clone(),
                        mine,
                        version,
                    }
                }
                _ => continue,
            };
            if !safe_path(&path) {
                if refused.insert(path.clone()) {
                    tracing::warn!("folder {}: refusing unsafe path {path:?}", self.name);
                }
                continue;
            }
            actions.push(action);
        }
        // Deletes first: they are quick, and free space for the fetches.
        actions.sort_by_key(|a| matches!(a, Action::Fetch { .. }));
        for action in actions {
            match action {
                Action::Delete {
                    path,
                    mine,
                    version,
                } => {
                    let (done, result) = oneshot::channel();
                    let request = Apply::Delete {
                        path: path.clone(),
                        expect: mine,
                        version,
                        done,
                    };
                    let Some(applied) = self.send(request, result).await else {
                        return Ok(false);
                    };
                    match applied {
                        Ok(true) => tracing::info!("folder {}: deleted {path}", self.name),
                        Ok(false) => {
                            tracing::debug!("folder {}: {path} changed; not deleting", self.name)
                        }
                        Err(err) => {
                            tracing::warn!("folder {}: deleting {path}: {err:#}", self.name)
                        }
                    }
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
                        continue;
                    }
                    let (done, result) = oneshot::channel();
                    let request = Apply::Place {
                        path: path.clone(),
                        expect: mine,
                        meta,
                        done,
                    };
                    let Some(applied) = self.send(request, result).await else {
                        return Ok(false);
                    };
                    match applied {
                        Ok(true) => tracing::info!("folder {}: wrote {path}", self.name),
                        Ok(false) => {
                            tracing::debug!("folder {}: {path} changed; not writing", self.name)
                        }
                        Err(err) => {
                            if failed.insert((path.clone(), hash)) {
                                tracing::warn!("folder {}: writing {path}: {err:#}", self.name);
                            }
                        }
                    }
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
        failed: &mut HashSet<(String, Hash)>,
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
            if failed.insert((path.to_string(), hash)) {
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
