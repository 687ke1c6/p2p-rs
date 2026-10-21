//! Synced folders: each has its own doc holding every node's index of it
//! (see `index`), listed in the membership doc as `folders/<doc id>` → its
//! name and the doc's secret, which lets any member write to it. Each node
//! keeping the folder lists itself as `folder-nodes/<doc id>/<node id>`, and
//! syncs the folder doc with the others listed. Where a folder lives on this
//! node is local: the `folders` file in the config directory.
//!
//! A folder is known by its name: adding one the group already has joins it.
//! Folders of one name created while their nodes were apart settle on the one
//! listed first (the oldest entry, then the smaller doc ID); a node keeping
//! another switches to it and indexes its files into it afresh.
//!
//! `folder add <name> <path>` can't create the doc itself, since the running
//! node holds the docs store. Like `member add`, it queues the request as a
//! file `folder-adds/<name>` holding the path, and the monitor applies it.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::ffi::OsString;
use std::fs::{self, DirBuilder};
use std::io;
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::os::unix::fs::DirBuilderExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{bail, Context};
use iroh::{Endpoint, EndpointAddr, EndpointId};
use iroh_blobs::api::downloader::Downloader;
use iroh_blobs::store::fs::FsStore;
use iroh_docs::engine::LiveEvent;
use iroh_docs::protocol::Docs;
use iroh_docs::store::Query;
use iroh_docs::{Capability, ContentStatus, NamespaceId, NamespaceSecret};
use n0_future::task::AbortOnDropHandle;
use n0_future::StreamExt;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, Notify};
use tokio::task::{JoinHandle, JoinSet};

use crate::content::Content;
use crate::index::{self, Apply, Entries, FileMeta, FolderIndex, IndexStats};
use crate::member::{check_name, NameError, Names};
use crate::peer_wire::{Client, Refusal};
use crate::record::{unix_now, GroupId};
use crate::view::FolderView;

/// File listing this node's folders inside the config directory.
pub const FOLDERS_FILE: &str = "folders";

/// Directory of queued folder adds inside the config directory.
pub const FOLDER_ADDS_DIR: &str = "folder-adds";

/// Prefix of every folder's key in the membership doc: `folders/<doc id>`.
pub const FOLDERS_PREFIX: &str = "folders/";

/// Prefix of the keys listing a folder's nodes in the membership doc:
/// `folder-nodes/<doc id>/<node id>`.
pub const FOLDER_NODES_PREFIX: &str = "folder-nodes/";

/// How long fetching one missing index metadata blob may take.
const META_FETCH_TIMEOUT: Duration = Duration::from_secs(5);

/// How many times, a second apart, an entry's metadata blob that didn't
/// arrive with it is fetched from the node that sent the entry, before
/// [`Folders::fetch_missing`] (each presence interval) is left to it.
const META_FETCH_TRIES: u32 = 10;

/// Requests waiting for a folder's index task.
const APPLY_QUEUE: usize = 16;

/// Directory of rendered folder views inside the config directory, one file
/// per folder name, for `folder show`.
pub const VIEWS_DIR: &str = "views";

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum FolderNameError {
    #[error(transparent)]
    Name(#[from] NameError),
    #[error("folder name contains '/'")]
    Slash,
}

/// Checks a folder name and returns it trimmed. The rules are a member
/// name's, and no `/`.
pub fn check_folder_name(name: &str) -> Result<String, FolderNameError> {
    let name = check_name(name)?;
    if name.contains('/') {
        return Err(FolderNameError::Slash);
    }
    Ok(name)
}

/// A folder this node syncs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FolderConfig {
    /// The folder's doc.
    pub id: NamespaceId,
    pub name: String,
    pub path: PathBuf,
}

/// This node's folders.
pub fn load_local(config_dir: &Path) -> anyhow::Result<Vec<FolderConfig>> {
    let path = config_dir.join(FOLDERS_FILE);
    match fs::read(&path) {
        Ok(bytes) => {
            postcard::from_bytes(&bytes).with_context(|| format!("{} is corrupt", path.display()))
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(err) => Err(err).with_context(|| format!("reading {}", path.display())),
    }
}

fn save_local(config_dir: &Path, folders: &[FolderConfig]) -> anyhow::Result<()> {
    let path = config_dir.join(FOLDERS_FILE);
    let tmp = config_dir.join(format!("{FOLDERS_FILE}.tmp"));
    fs::write(&tmp, postcard::to_stdvec(folders)?)?;
    fs::rename(&tmp, &path).with_context(|| format!("writing {}", path.display()))
}

/// Queues adding a folder named `name` at `path` (absolute, already checked)
/// for the running node to apply.
pub fn queue_add(config_dir: &Path, name: &str, path: &Path) -> io::Result<()> {
    let dir = config_dir.join(FOLDER_ADDS_DIR);
    DirBuilder::new().recursive(true).mode(0o700).create(&dir)?;
    let tmp = dir.join(format!("{name}.tmp"));
    fs::write(&tmp, path.as_os_str().as_bytes())?;
    fs::rename(&tmp, dir.join(name))
}

/// What a `folders/<doc id>` entry holds.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct FolderRecord {
    name: String,
    /// The folder doc's secret: write access to it.
    secret: [u8; 32],
}

/// A folder listed in the membership doc.
#[derive(Debug, Clone)]
pub(crate) struct GroupFolder {
    pub id: NamespaceId,
    pub name: String,
    secret: NamespaceSecret,
    /// When it was first listed, by the doc's clock (microseconds).
    created: u64,
}

/// The group's folders, from the membership doc.
pub(crate) async fn group_folders(
    docs: &Docs,
    blobs: &FsStore,
    group: GroupId,
) -> anyhow::Result<Vec<GroupFolder>> {
    let doc = docs.open(group).await?.context("membership doc missing")?;
    let entries = doc
        .get_many(Query::key_prefix(FOLDERS_PREFIX))
        .await?
        .collect::<Vec<_>>()
        .await;
    let mut out: HashMap<NamespaceId, GroupFolder> = HashMap::new();
    for entry in entries {
        let entry = entry?;
        let Some(id) = std::str::from_utf8(entry.key())
            .ok()
            .and_then(|key| key.strip_prefix(FOLDERS_PREFIX))
            .and_then(|id| id.parse::<NamespaceId>().ok())
        else {
            continue;
        };
        let Ok(bytes) = blobs.get_bytes(entry.content_hash()).await else {
            continue;
        };
        let Ok(record) = postcard::from_bytes::<FolderRecord>(&bytes) else {
            continue;
        };
        let secret = NamespaceSecret::from_bytes(&record.secret);
        let Ok(name) = check_folder_name(&record.name) else {
            continue;
        };
        if secret.id() != id {
            continue;
        }
        let created = entry.timestamp();
        out.entry(id)
            .and_modify(|f| f.created = f.created.min(created))
            .or_insert(GroupFolder {
                id,
                name,
                secret,
                created,
            });
    }
    Ok(out.into_values().collect())
}

/// The group's folder called `name`: of several, the one listed first.
fn canonical<'a>(folders: &'a [GroupFolder], name: &str) -> Option<&'a GroupFolder> {
    folders
        .iter()
        .filter(|f| f.name == name)
        .min_by_key(|f| (f.created, f.id))
}

/// The nodes keeping folder `id`, from the membership doc.
pub(crate) async fn folder_nodes(
    docs: &Docs,
    group: GroupId,
    id: NamespaceId,
) -> anyhow::Result<BTreeSet<EndpointId>> {
    let doc = docs.open(group).await?.context("membership doc missing")?;
    let prefix = format!("{FOLDER_NODES_PREFIX}{id}/");
    let entries = doc
        .get_many(Query::key_prefix(&prefix))
        .await?
        .collect::<Vec<_>>()
        .await;
    let mut out = BTreeSet::new();
    for entry in entries {
        let entry = entry?;
        if let Some(node) = std::str::from_utf8(entry.key())
            .ok()
            .and_then(|key| key.strip_prefix(&prefix))
            .and_then(|node| node.parse::<EndpointId>().ok())
        {
            out.insert(node);
        }
    }
    Ok(out)
}

/// A folder the peer-wire server serves from.
#[derive(Debug, Clone)]
pub(crate) struct Serving {
    pub root: PathBuf,
    pub entries: Entries,
    /// To the folder's index task, to ask it to re-read a file.
    pub apply: Option<mpsc::Sender<Apply>>,
}

/// A folder this node keeps, as `status` shows it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FolderState {
    pub config: FolderConfig,
    pub stats: IndexStats,
    /// Not being indexed: the folder's root or its marker is missing.
    pub paused: bool,
    /// The other nodes this node syncs the folder's doc with.
    pub peers: usize,
}

/// What `folder show` reads: a folder's merged view and the names to show
/// its nodes by.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ViewFile {
    pub view: FolderView,
    pub names: BTreeMap<EndpointId, String>,
    /// The entries the view was built from: path → node → entry.
    pub entries: BTreeMap<String, BTreeMap<EndpointId, FileMeta>>,
    /// Unix seconds.
    pub written_at: u64,
}

impl ViewFile {
    pub fn render(&self, all: bool) -> String {
        self.view.render(&|id| self.label(id), all)
    }

    /// Every node's raw entries, as `folder show --entries` prints them.
    pub fn render_entries(&self) -> String {
        let nodes: Vec<_> = self.view.nodes.iter().map(|id| self.label(*id)).collect();
        format!(
            "folder {}  ({} nodes: {})\n{}",
            self.view.name,
            nodes.len(),
            nodes.join(", "),
            crate::view::render_entries(&self.entries, &|id| self.label(id))
        )
    }

    fn label(&self, id: EndpointId) -> String {
        self.names
            .get(&id)
            .cloned()
            .unwrap_or_else(|| id.fmt_short().to_string())
    }
}

/// Reads the view `folder show` prints for folder `name`.
pub fn read_view(config_dir: &Path, name: &str) -> anyhow::Result<Option<ViewFile>> {
    let path = config_dir.join(VIEWS_DIR).join(name);
    match fs::read(&path) {
        Ok(bytes) => {
            Ok(Some(postcard::from_bytes(&bytes).with_context(|| {
                format!("{} is corrupt", path.display())
            })?))
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err).with_context(|| format!("reading {}", path.display())),
    }
}

/// The folders this node keeps: for each, a task keeping its index (see
/// `index::watch`), syncing of its doc with the folder's other nodes, and a
/// flag set whenever the doc changes, so its view is only rebuilt then.
#[derive(Debug, Clone)]
pub struct Folders(Arc<Inner>);

#[derive(Debug)]
struct Inner {
    config_dir: PathBuf,
    docs: Docs,
    blobs: FsStore,
    me: EndpointId,
    /// Fetches index metadata docs failed to fetch.
    downloader: Downloader,
    /// Fetches file contents over the peer wire.
    client: Client,
    debounce: Duration,
    /// Members' names, for naming conflict copies.
    names: Names,
    running: Mutex<HashMap<NamespaceId, Running>>,
    /// Held by `add` and `check`, so they never start the same folder twice.
    starting: tokio::sync::Mutex<()>,
}

#[derive(Debug)]
struct Running {
    config: FolderConfig,
    stats: Arc<Mutex<IndexStats>>,
    /// The index task. `None`, or finished, while the folder is paused.
    task: Option<JoinHandle<()>>,
    /// Sets `dirty` and notifies `changed` on every change to the doc.
    events: JoinHandle<()>,
    dirty: Arc<AtomicBool>,
    /// Wakes the content task.
    changed: Arc<Notify>,
    /// The nodes the doc is synced with.
    peers: BTreeSet<EndpointId>,
    /// The peers whose last doc sync with this node succeeded, either way
    /// round. Docs doesn't try a failed sync again (see
    /// [`Folders::retry_sync`]).
    synced: Arc<Mutex<HashSet<EndpointId>>>,
    /// This node's entries, kept by the index task.
    entries: Entries,
    /// To the index task, while it runs.
    apply: Option<mpsc::Sender<Apply>>,
}

impl Running {
    fn paused(&self) -> bool {
        self.task.as_ref().is_none_or(|task| task.is_finished())
    }

    fn stop(self) {
        if let Some(task) = self.task {
            task.abort();
        }
        self.events.abort();
    }
}

impl Folders {
    /// Bytes of file data fetched from each peer (see
    /// [`Client::fetched`]).
    pub(crate) fn fetched(&self) -> HashMap<EndpointId, u64> {
        self.0.client.fetched()
    }

    pub(crate) fn new(
        config_dir: &Path,
        docs: Docs,
        blobs: FsStore,
        endpoint: &Endpoint,
        debounce: Duration,
        direct_only: bool,
        names: Names,
    ) -> Self {
        Self(Arc::new(Inner {
            config_dir: config_dir.to_owned(),
            docs,
            downloader: blobs.downloader(endpoint),
            client: Client::new(endpoint.clone(), direct_only),
            blobs,
            me: endpoint.id(),
            debounce,
            names,
            running: Mutex::new(HashMap::new()),
            starting: tokio::sync::Mutex::new(()),
        }))
    }

    /// Brings every folder in the `folders` file up to date: moves it to the
    /// group's folder of its name if that is another doc, starts its doc's
    /// tasks, syncs with nodes that have joined it, and starts or resumes its
    /// index. Runs at startup and every presence interval; logs a folder
    /// pausing or resuming once.
    pub(crate) async fn check(&self, group: GroupId) -> anyhow::Result<()> {
        let _starting = self.0.starting.lock().await;
        let mut local = load_local(&self.0.config_dir)?;
        let listed = group_folders(&self.0.docs, &self.0.blobs, group).await?;
        let mut moved = false;
        for config in local.iter_mut() {
            let Some(canon) = canonical(&listed, &config.name) else {
                continue;
            };
            if canon.id == config.id {
                continue;
            }
            tracing::info!(
                "folder {}: another node created it first; switching to its doc {}",
                config.name,
                canon.id.fmt_short()
            );
            if let Some(running) = self.0.running.lock().unwrap().remove(&config.id) {
                running.stop();
            }
            if let Ok(Some(old)) = self.0.docs.open(config.id).await {
                let _ = old.leave().await;
            }
            self.join_doc(group, canon).await?;
            config.id = canon.id;
            moved = true;
        }
        if moved {
            save_local(&self.0.config_dir, &local)?;
        }
        for config in local {
            self.keep_running(group, config).await;
        }
        Ok(())
    }

    /// Opens a listed folder's doc and lists this node as keeping it.
    async fn join_doc(&self, group: GroupId, folder: &GroupFolder) -> anyhow::Result<()> {
        self.0
            .docs
            .import_namespace(Capability::Write(folder.secret.clone()))
            .await
            .context("opening folder doc")?;
        self.list_node(group, folder.id).await
    }

    async fn list_node(&self, group: GroupId, id: NamespaceId) -> anyhow::Result<()> {
        let membership = self
            .0
            .docs
            .open(group)
            .await?
            .context("membership doc missing")?;
        let author = self.0.docs.author_default().await?;
        membership
            .set_bytes(
                author,
                format!("{FOLDER_NODES_PREFIX}{id}/{}", self.0.me),
                b"keeps".to_vec(),
            )
            .await?;
        Ok(())
    }

    /// Starts what isn't running for `config`, and syncs with new nodes.
    async fn keep_running(&self, group: GroupId, config: FolderConfig) {
        let id = config.id;
        let name = config.name.clone();
        let known = self.0.running.lock().unwrap().contains_key(&id);
        if !known {
            match self.start_doc(config.clone()).await {
                Ok(running) => {
                    self.0.running.lock().unwrap().insert(id, running);
                }
                Err(err) => {
                    tracing::warn!("folder {name}: {err:#}");
                    return;
                }
            }
        }
        if let Err(err) = self.sync_peers(group, id).await {
            tracing::warn!("folder {name}: syncing: {err:#}");
        }
        if let Err(err) = self.fetch_missing_meta(id).await {
            tracing::debug!("folder {name}: fetching index metadata: {err:#}");
        }
        let was_paused = match self.0.running.lock().unwrap().get(&id) {
            Some(running) if !running.paused() => return,
            Some(_) => known,
            None => return,
        };
        match self.start_index(&config).await {
            Ok(task) => {
                if was_paused {
                    tracing::info!("folder {name} resumed");
                }
                if let Some(running) = self.0.running.lock().unwrap().get_mut(&id) {
                    running.task = Some(task);
                }
            }
            Err(err) => {
                let mut running = self.0.running.lock().unwrap();
                let Some(running) = running.get_mut(&id) else {
                    return;
                };
                // A task that just stopped, or a first start: say why once.
                if running.task.take().is_some() || !was_paused {
                    tracing::warn!("folder {name} paused: {err:#}");
                }
            }
        }
    }

    /// Opens the folder's doc and starts following its changes.
    async fn start_doc(&self, config: FolderConfig) -> anyhow::Result<Running> {
        let doc = self
            .0
            .docs
            .open(config.id)
            .await?
            .context("folder doc missing")?;
        let mut events = doc.subscribe().await?;
        let dirty = Arc::new(AtomicBool::new(true));
        let changed = Arc::new(Notify::new());
        let (flag, notify) = (dirty.clone(), changed.clone());
        let synced = Arc::new(Mutex::new(HashSet::new()));
        let results = synced.clone();
        let (blobs, downloader) = (self.0.blobs.clone(), self.0.downloader.clone());
        let events = tokio::spawn(async move {
            // Metadata fetches, stopped with this task.
            let mut fetches = JoinSet::new();
            while let Some(event) = events.next().await {
                // Docs fetches an entry's metadata once, when the entry
                // arrives, and not again if that fails (as it does on a busy
                // machine): fetch it here until it is there.
                if let Ok(LiveEvent::InsertRemote {
                    from,
                    entry,
                    content_status: ContentStatus::Missing | ContentStatus::Incomplete,
                }) = &event
                {
                    let (hash, from) = (entry.content_hash(), *from);
                    let (blobs, downloader) = (blobs.clone(), downloader.clone());
                    let (flag, notify) = (flag.clone(), notify.clone());
                    fetches.spawn(async move {
                        for _ in 0..META_FETCH_TRIES {
                            // Docs' own fetch first: usually it succeeds.
                            tokio::time::sleep(Duration::from_secs(1)).await;
                            if blobs.has(hash).await.unwrap_or(false) {
                                break;
                            }
                            let download = downloader.download(hash, vec![from]);
                            if let Ok(Ok(())) =
                                tokio::time::timeout(META_FETCH_TIMEOUT, download).await
                            {
                                flag.store(true, Ordering::Relaxed);
                                notify.notify_one();
                                break;
                            }
                        }
                    });
                }
                while fetches.try_join_next().is_some() {}
                if let Ok(LiveEvent::SyncFinished(sync)) = &event {
                    let mut results = results.lock().unwrap();
                    match sync.result {
                        Ok(_) => results.insert(sync.peer),
                        Err(_) => results.remove(&sync.peer),
                    };
                }
                if matches!(
                    event,
                    Ok(LiveEvent::InsertLocal { .. }
                        | LiveEvent::InsertRemote { .. }
                        | LiveEvent::ContentReady { .. })
                ) {
                    flag.store(true, Ordering::Relaxed);
                    notify.notify_one();
                }
            }
        });
        Ok(Running {
            config,
            stats: Arc::default(),
            task: None,
            events,
            dirty,
            changed,
            peers: BTreeSet::new(),
            synced,
            entries: Entries::default(),
            apply: None,
        })
    }

    /// Starts syncing the folder's doc with nodes listed as keeping it that
    /// it isn't synced with yet.
    async fn sync_peers(&self, group: GroupId, id: NamespaceId) -> anyhow::Result<()> {
        let nodes = folder_nodes(&self.0.docs, group, id).await?;
        let new: Vec<EndpointId> = {
            let running = self.0.running.lock().unwrap();
            let Some(running) = running.get(&id) else {
                return Ok(());
            };
            nodes
                .iter()
                .filter(|n| **n != self.0.me && !running.peers.contains(*n))
                .copied()
                .collect()
        };
        if new.is_empty() {
            return Ok(());
        }
        let doc = self.0.docs.open(id).await?.context("folder doc missing")?;
        doc.start_sync(new.iter().copied().map(EndpointAddr::new).collect())
            .await?;
        if let Some(running) = self.0.running.lock().unwrap().get_mut(&id) {
            running.peers.extend(new);
            // The view lists every node keeping the folder.
            running.dirty.store(true, Ordering::Relaxed);
        }
        Ok(())
    }

    /// Starts syncing each folder's doc again with every peer `online` says
    /// is online whose last sync failed or hasn't finished. Docs syncs only
    /// when asked, on a new gossip neighbour, or on a neighbour's report, and
    /// never tries a failed sync again: entries written before a node joined
    /// would never reach it. Docs runs one sync per peer at a time, so a
    /// sync still running isn't doubled. Every presence interval.
    pub(crate) async fn retry_sync(&self, online: impl Fn(EndpointId) -> bool) {
        let due: Vec<(NamespaceId, Vec<EndpointId>)> = {
            let running = self.0.running.lock().unwrap();
            running
                .iter()
                .map(|(id, r)| {
                    let synced = r.synced.lock().unwrap();
                    let due = r.peers.iter().copied();
                    (
                        *id,
                        due.filter(|p| !synced.contains(p) && online(*p)).collect(),
                    )
                })
                .filter(|(_, due): &(_, Vec<_>)| !due.is_empty())
                .collect()
        };
        for (id, peers) in due {
            let Ok(Some(doc)) = self.0.docs.open(id).await else {
                continue;
            };
            tracing::debug!(
                "doc {}: syncing again with {} peers",
                id.fmt_short(),
                peers.len()
            );
            if let Err(err) = doc
                .start_sync(peers.into_iter().map(EndpointAddr::new).collect())
                .await
            {
                tracing::debug!("doc {}: syncing again: {err:#}", id.fmt_short());
            }
        }
    }

    /// Fetches the metadata blobs of entries docs synced but couldn't fetch
    /// the content of, from the nodes the folder is synced with (see
    /// [`fetch_missing`](Self::fetch_missing)).
    async fn fetch_missing_meta(&self, id: NamespaceId) -> anyhow::Result<()> {
        let peers: Vec<EndpointId> = match self.0.running.lock().unwrap().get(&id) {
            Some(running) if !running.peers.is_empty() => running.peers.iter().copied().collect(),
            _ => return Ok(()),
        };
        if self.fetch_missing(id, peers).await? {
            if let Some(running) = self.0.running.lock().unwrap().get(&id) {
                running.dirty.store(true, Ordering::Relaxed);
            }
        }
        Ok(())
    }

    /// Fetches the content of every entry in doc `id` this node has the
    /// entry but not the content of, from `peers`. Returns whether any
    /// arrived. Docs fetches an entry's content when the entry arrives, and
    /// tries again only when a neighbour announces it over gossip, which may
    /// never reach this node: a fetch that failed (a timeout on a busy
    /// machine) would leave the entry unreadable for good.
    pub(crate) async fn fetch_missing(
        &self,
        id: NamespaceId,
        peers: Vec<EndpointId>,
    ) -> anyhow::Result<bool> {
        if peers.is_empty() {
            return Ok(false);
        }
        let doc = self.0.docs.open(id).await?.context("doc missing")?;
        let entries = doc.get_many(Query::all()).await?.collect::<Vec<_>>().await;
        let mut missing = Vec::new();
        for entry in entries {
            let hash = entry?.content_hash();
            if !self.0.blobs.has(hash).await? {
                missing.push(hash);
            }
        }
        if missing.is_empty() {
            return Ok(false);
        }
        tracing::debug!(
            "doc {}: fetching {} missing entries",
            id.fmt_short(),
            missing.len()
        );
        let mut fetched = false;
        for hash in missing {
            let download = self.0.downloader.download(hash, peers.clone());
            match tokio::time::timeout(META_FETCH_TIMEOUT, download).await {
                Ok(Ok(())) => fetched = true,
                Ok(Err(err)) => tracing::debug!("fetching {}: {err:#}", hash.fmt_short()),
                Err(_) => tracing::debug!("fetching {}: timed out", hash.fmt_short()),
            }
        }
        Ok(fetched)
    }

    /// Starts the index task, if the folder's root is there, and with
    /// content transfer on, the content task, which stops with it.
    async fn start_index(&self, config: &FolderConfig) -> anyhow::Result<JoinHandle<()>> {
        index::check_root(&config.path)?;
        let (stats, changed, entries) = self
            .0
            .running
            .lock()
            .unwrap()
            .get(&config.id)
            .map(|r| (r.stats.clone(), r.changed.clone(), r.entries.clone()))
            .unwrap_or_default();
        let index = FolderIndex::open(
            &self.0.docs,
            &self.0.blobs,
            config.id,
            config.path.clone(),
            self.0.me,
            entries.clone(),
        )
        .await?;
        *stats.lock().unwrap() = index.stats();
        let (tx, rx) = mpsc::channel(APPLY_QUEUE);
        if let Some(running) = self.0.running.lock().unwrap().get_mut(&config.id) {
            running.apply = Some(tx.clone());
        }
        let content = Content {
            name: config.name.clone(),
            folder: config.id,
            me: self.0.me,
            docs: self.0.docs.clone(),
            blobs: self.0.blobs.clone(),
            client: self.0.client.clone(),
            folders: self.clone(),
            root: config.path.clone(),
            entries,
            changed,
            apply: tx,
            names: self.0.names.clone(),
        };
        let watch = index::watch(index, config.name.clone(), self.0.debounce, stats, rx);
        Ok(tokio::spawn(async move {
            // Aborted when the index task ends or is aborted.
            let _content = AbortOnDropHandle::new(tokio::spawn(content.run()));
            watch.await;
        }))
    }

    /// Keeps a folder named `name` at `path` on this node: joins the
    /// group's folder of that name, or creates it if there is none, and
    /// starts indexing and syncing it.
    pub(crate) async fn add(
        &self,
        group: GroupId,
        name: &str,
        path: &Path,
    ) -> anyhow::Result<NamespaceId> {
        let name = check_folder_name(name)?;
        let _starting = self.0.starting.lock().await;
        if !path.is_dir() {
            bail!("{} is not a directory", path.display());
        }
        let path = path
            .canonicalize()
            .with_context(|| format!("resolving {}", path.display()))?;
        let mut local = load_local(&self.0.config_dir)?;
        if let Some(other) = local.iter().find(|f| f.name == name) {
            bail!(
                "this node already keeps folder {name} at {}",
                other.path.display()
            );
        }
        if let Some(other) = local.iter().find(|f| overlaps(&f.path, &path)) {
            bail!(
                "{} overlaps folder {} at {}",
                path.display(),
                other.name,
                other.path.display()
            );
        }
        let marker = path.join(index::MARKER);
        if !marker.exists() {
            fs::write(&marker, b"").with_context(|| format!("creating {}", marker.display()))?;
        }
        let listed = group_folders(&self.0.docs, &self.0.blobs, group).await?;
        let (id, joined) = match canonical(&listed, &name) {
            Some(folder) => {
                self.join_doc(group, folder).await?;
                (folder.id, true)
            }
            None => {
                let secret = NamespaceSecret::from_bytes(&rand::random());
                let id = secret.id();
                self.0
                    .docs
                    .import_namespace(Capability::Write(secret.clone()))
                    .await
                    .context("creating folder doc")?;
                let record = FolderRecord {
                    name: name.clone(),
                    secret: secret.to_bytes(),
                };
                let membership = self
                    .0
                    .docs
                    .open(group)
                    .await?
                    .context("membership doc missing")?;
                let author = self.0.docs.author_default().await?;
                membership
                    .set_bytes(
                        author,
                        format!("{FOLDERS_PREFIX}{id}"),
                        postcard::to_stdvec(&record)?,
                    )
                    .await?;
                self.list_node(group, id).await?;
                (id, false)
            }
        };
        let config = FolderConfig { id, name, path };
        local.push(config.clone());
        save_local(&self.0.config_dir, &local)?;
        tracing::info!(
            "{} folder {} at {}",
            if joined { "joined" } else { "created" },
            config.name,
            config.path.display()
        );
        self.keep_running(group, config).await;
        Ok(id)
    }

    /// Applies the folder adds `folder add` queued. A request that fails is
    /// logged and dropped: the reasons (not a directory, already kept) don't
    /// go away by retrying.
    pub(crate) async fn apply_queued(&self, group: GroupId) {
        let dir = self.0.config_dir.join(FOLDER_ADDS_DIR);
        let entries = match fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(err) if err.kind() == io::ErrorKind::NotFound => return,
            Err(err) => {
                tracing::warn!("reading {}: {err}", dir.display());
                return;
            }
        };
        for entry in entries.flatten() {
            let file = entry.path();
            let Some(name) = file.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            if name.ends_with(".tmp") {
                continue;
            }
            let result = match fs::read(&file) {
                Ok(bytes) => {
                    let path = PathBuf::from(OsString::from_vec(bytes));
                    self.add(group, name, &path).await.map(|_| ())
                }
                Err(err) => Err(err.into()),
            };
            if let Err(err) = result {
                tracing::warn!("can't add folder {name}: {err:#}");
            }
            if let Err(err) = fs::remove_file(&file) {
                tracing::warn!("removing {}: {err}", file.display());
            }
        }
    }

    /// The merged view of folder `id`.
    pub(crate) async fn view(&self, group: GroupId, id: NamespaceId) -> anyhow::Result<FolderView> {
        Ok(self.view_and_entries(group, id).await?.0)
    }

    /// The merged view of folder `id` and the entries it was built from.
    async fn view_and_entries(
        &self,
        group: GroupId,
        id: NamespaceId,
    ) -> anyhow::Result<(FolderView, BTreeMap<String, BTreeMap<EndpointId, FileMeta>>)> {
        let name = self
            .0
            .running
            .lock()
            .unwrap()
            .get(&id)
            .map(|r| r.config.name.clone())
            .context("not a folder this node keeps")?;
        let nodes = folder_nodes(&self.0.docs, group, id).await?;
        let entries = index::read_all(&self.0.docs, &self.0.blobs, id).await?;
        let view = FolderView::build(id, name, nodes, entries.clone());
        Ok((view, entries))
    }

    /// Rewrites the view file of every folder whose doc changed since its
    /// last one.
    pub(crate) async fn write_views(&self, group: GroupId, names: &Names) {
        let changed: Vec<(NamespaceId, String)> = self
            .0
            .running
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, r)| r.dirty.swap(false, Ordering::Relaxed))
            .map(|(id, r)| (*id, r.config.name.clone()))
            .collect();
        for (id, name) in changed {
            let result = async {
                let (view, entries) = self.view_and_entries(group, id).await?;
                // Version vectors may name nodes that no longer keep the
                // folder, so label those too.
                let names = view
                    .nodes
                    .iter()
                    .chain(
                        entries
                            .values()
                            .flat_map(BTreeMap::values)
                            .flat_map(|meta| meta.version.0.keys()),
                    )
                    .map(|id| (*id, names.display(*id)))
                    .collect();
                let file = ViewFile {
                    view,
                    names,
                    entries,
                    written_at: unix_now(),
                };
                let dir = self.0.config_dir.join(VIEWS_DIR);
                fs::create_dir_all(&dir)?;
                let tmp = dir.join(format!("{name}.tmp"));
                fs::write(&tmp, postcard::to_stdvec(&file)?)?;
                fs::rename(&tmp, dir.join(&name))?;
                anyhow::Ok(())
            }
            .await;
            if let Err(err) = result {
                tracing::warn!("folder {name}: writing its view: {err:#}");
                if let Some(running) = self.0.running.lock().unwrap().get(&id) {
                    running.dirty.store(true, Ordering::Relaxed);
                }
            }
        }
    }

    /// Syncs every folder doc shared with `id` with it right away.
    pub(crate) async fn resync_with(&self, id: EndpointId) {
        let shared: Vec<NamespaceId> = self
            .0
            .running
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, r)| r.peers.contains(&id))
            .map(|(folder, r)| {
                // Until this sync succeeds, `retry_sync` tries again.
                r.synced.lock().unwrap().remove(&id);
                *folder
            })
            .collect();
        for folder in shared {
            if let Ok(Some(doc)) = self.0.docs.open(folder).await {
                if let Err(err) = doc.start_sync(vec![EndpointAddr::new(id)]).await {
                    tracing::debug!("syncing folder doc with {}: {err:#}", id.fmt_short());
                }
            }
        }
    }

    /// What the peer-wire server needs to serve folder `id` to `remote`: refuses
    /// a remote that doesn't keep the folder, or a folder this node doesn't
    /// keep (the same answer, so as not to say which folders exist), and a
    /// paused folder.
    pub(crate) fn serving(&self, id: NamespaceId, remote: EndpointId) -> Result<Serving, Refusal> {
        let running = self.0.running.lock().unwrap();
        let Some(running) = running.get(&id).filter(|r| r.peers.contains(&remote)) else {
            return Err(Refusal::NotKept);
        };
        if running.paused() {
            return Err(Refusal::NoSuchFile);
        }
        Ok(Serving {
            root: running.config.path.clone(),
            entries: running.entries.clone(),
            apply: running.apply.clone(),
        })
    }

    /// The other nodes that keep folder `id`, as far as this node knows.
    pub(crate) fn folder_peers(&self, id: NamespaceId) -> Vec<EndpointId> {
        self.0
            .running
            .lock()
            .unwrap()
            .get(&id)
            .map(|r| r.peers.iter().copied().collect())
            .unwrap_or_default()
    }

    /// The folders this node keeps, by name.
    pub fn list(&self) -> Vec<FolderState> {
        let mut out: Vec<_> = self
            .0
            .running
            .lock()
            .unwrap()
            .values()
            .map(|r| FolderState {
                config: r.config.clone(),
                stats: *r.stats.lock().unwrap(),
                paused: r.paused(),
                peers: r.peers.len(),
            })
            .collect();
        out.sort_by(|a, b| a.config.name.cmp(&b.config.name));
        out
    }

    /// Stops every folder's tasks.
    pub(crate) fn stop_all(&self) {
        for (_, running) in self.0.running.lock().unwrap().drain() {
            running.stop();
        }
    }
}

/// Whether one path is the other or inside it.
fn overlaps(a: &Path, b: &Path) -> bool {
    a.starts_with(b) || b.starts_with(a)
}
