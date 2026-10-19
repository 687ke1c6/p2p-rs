//! A folder's index: one entry per file in the folder's doc, keyed by its
//! path relative to the folder root and pointing at a small [`FileMeta`] blob.
//!
//! [`FolderIndex::reconcile`] brings the entries under a path in line with
//! what is on disk. A file whose size and modification time match its entry
//! isn't read again; any other file is hashed with BLAKE3, the same hash
//! `iroh-blobs` uses, and its entry rewritten. An entry whose file is gone
//! becomes a tombstone. [`watch`] runs a full reconcile at start and then
//! reconciles the paths the file watcher reports, once they have been quiet
//! for the debounce interval, so a burst of writes yields one entry per file.
//!
//! Ignored paths (`.syncignore`, gitignore syntax, plus our own temporary
//! files) are neither indexed nor tombstoned: ignoring a file must not delete
//! it on other nodes. Nor is anything under a directory that can't be read.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{self, File, Metadata};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, UNIX_EPOCH};

use anyhow::Context;
use ignore::gitignore::{Gitignore, GitignoreBuilder};
use iroh::EndpointId;
use iroh_blobs::store::fs::FsStore;
use iroh_blobs::Hash;
use iroh_docs::api::Doc;
use iroh_docs::protocol::Docs;
use iroh_docs::store::Query;
use iroh_docs::{AuthorId, NamespaceId};
use n0_future::StreamExt;
use notify::event::{AccessKind, AccessMode};
use notify::{EventKind, RecursiveMode, Watcher};
use serde::{Deserialize, Serialize};

use crate::view::Version;

/// The ignore file at a folder's root, in `.gitignore` syntax.
pub const SYNCIGNORE: &str = ".syncignore";

/// Files starting with this are the node's own temporary files, always
/// ignored.
pub const TEMP_PREFIX: &str = ".sync-node-tmp-";

/// Marks a folder's root. A folder whose marker is missing is paused rather
/// than scanned: its directory has moved, been deleted or isn't mounted, and
/// scanning it would tombstone every file. Always ignored, so never synced.
pub const MARKER: &str = ".syncfolder";

/// Checks that `root` is a folder root: a directory holding the marker.
pub fn check_root(root: &Path) -> anyhow::Result<()> {
    if !root.join(MARKER).is_file() {
        anyhow::bail!(
            "{} has no {MARKER} marker: moved, deleted or not mounted?",
            root.display()
        );
    }
    Ok(())
}

/// How long a changed path must be quiet before it is indexed, by default.
pub const DEBOUNCE: Duration = Duration::from_secs(1);

/// A path that keeps changing is indexed anyway after this many debounce
/// intervals.
const MAX_DELAY_INTERVALS: u32 = 10;

/// What a folder entry points at.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileMeta {
    /// BLAKE3 hash of the contents, as `iroh-blobs` computes it. `None` for a
    /// tombstone.
    pub content: Option<Hash>,
    pub size: u64,
    /// Modification time, unix nanoseconds. For a tombstone, when the
    /// delete was noticed.
    pub mtime_ns: u64,
    /// The node that wrote this entry, describing its own disk.
    pub node: EndpointId,
    /// Which edits this version has seen (see `view`).
    pub version: Version,
}

impl FileMeta {
    pub fn is_deleted(&self) -> bool {
        self.content.is_none()
    }

    fn to_bytes(&self) -> Vec<u8> {
        postcard::to_stdvec(self).expect("meta serializes")
    }
}

/// Live files and their total size.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct IndexStats {
    pub files: usize,
    pub bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Stat {
    size: u64,
    mtime_ns: u64,
}

impl Stat {
    fn of(meta: &Metadata) -> Self {
        let mtime_ns = meta
            .modified()
            .ok()
            .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        Self {
            size: meta.len(),
            mtime_ns,
        }
    }
}

/// What a walk found under a path.
#[derive(Debug, Default)]
struct Walk {
    files: Vec<(String, Stat)>,
    /// Directories that couldn't be read: their entries are left alone.
    unreadable: Vec<String>,
}

pub struct FolderIndex {
    root: PathBuf,
    doc: Doc,
    blobs: FsStore,
    author: AuthorId,
    node: EndpointId,
    entries: HashMap<String, FileMeta>,
    ignore: Gitignore,
}

impl FolderIndex {
    /// Opens `node`'s index of the folder at `root`, kept in doc `id`.
    pub async fn open(
        docs: &Docs,
        blobs: &FsStore,
        id: NamespaceId,
        root: PathBuf,
        node: EndpointId,
    ) -> anyhow::Result<Self> {
        let doc = docs.open(id).await?.context("folder doc missing")?;
        let author = docs.author_default().await?;
        let entries = read_entries(&doc, blobs, author).await?;
        let ignore = load_ignore(&root);
        Ok(Self {
            root,
            doc,
            blobs: blobs.clone(),
            author,
            node,
            entries,
            ignore,
        })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// This node's entries, tombstones included.
    pub fn entries(&self) -> &HashMap<String, FileMeta> {
        &self.entries
    }

    pub fn stats(&self) -> IndexStats {
        let live = self.entries.values().filter(|m| !m.is_deleted());
        IndexStats {
            files: live.clone().count(),
            bytes: live.map(|m| m.size).sum(),
        }
    }

    /// Re-reads `.syncignore`.
    pub fn reload_ignore(&mut self) {
        self.ignore = load_ignore(&self.root);
    }

    fn is_ignored(&self, rel: &str, is_dir: bool) -> bool {
        is_ignored(&self.ignore, rel, is_dir)
    }

    /// Brings the entries at and under `rel` (`""` for the whole folder) in
    /// line with the disk. Returns how many entries it wrote.
    pub async fn reconcile(&mut self, rel: &str) -> anyhow::Result<usize> {
        check_root(&self.root)?;
        let (root, start, ignore) = (self.root.clone(), rel.to_string(), self.ignore.clone());
        let walk = tokio::task::spawn_blocking(move || walk(&root, &start, &ignore)).await??;
        let mut written = 0;
        let mut seen = HashSet::new();
        for (path, stat) in walk.files {
            seen.insert(path.clone());
            if self.index_file(&path, stat).await? {
                written += 1;
            }
        }
        let gone: Vec<String> = self
            .entries
            .iter()
            .filter(|(path, meta)| {
                !meta.is_deleted()
                    && is_under(path, rel)
                    && !seen.contains(*path)
                    && !walk.unreadable.iter().any(|dir| is_under(path, dir))
                    && !self.is_ignored(path, false)
            })
            .map(|(path, _)| path.clone())
            .collect();
        if !gone.is_empty() {
            // The root may have gone during the walk.
            check_root(&self.root)?;
        }
        for path in gone {
            self.tombstone(&path).await?;
            written += 1;
        }
        Ok(written)
    }

    /// Indexes one file found at `stat`. Returns whether it wrote an entry.
    async fn index_file(&mut self, path: &str, stat: Stat) -> anyhow::Result<bool> {
        let old = self.entries.get(path).cloned();
        if let Some(old) = &old {
            if !old.is_deleted() && old.size == stat.size && old.mtime_ns == stat.mtime_ns {
                return Ok(false);
            }
        }
        let abs = self.root.join(path);
        let Some((hash, stat)) = tokio::task::spawn_blocking(move || hash_file(&abs)).await??
        else {
            // Gone or changed while hashing: its next event brings it back.
            return Ok(false);
        };
        let version = match &old {
            // Only the modification time changed: the same version.
            Some(old) if old.content == Some(hash) => old.version.clone(),
            _ => self.next_version(path, old.as_ref()).await?,
        };
        let meta = FileMeta {
            content: Some(hash),
            size: stat.size,
            mtime_ns: stat.mtime_ns,
            node: self.node,
            version,
        };
        self.write(path, meta).await?;
        Ok(true)
    }

    async fn tombstone(&mut self, path: &str) -> anyhow::Result<()> {
        let old = self.entries.get(path).cloned();
        let version = self.next_version(path, old.as_ref()).await?;
        let meta = FileMeta {
            content: None,
            size: 0,
            mtime_ns: unix_now_ns(),
            node: self.node,
            version,
        };
        self.write(path, meta).await
    }

    /// The version of a change to `path` replacing this node's `old` entry:
    /// `old`'s version, merged with every other node's entry for the path
    /// with the same content (the same file, wherever it came from), plus
    /// one edit by this node.
    async fn next_version(&self, path: &str, old: Option<&FileMeta>) -> anyhow::Result<Version> {
        let Some(old) = old else {
            return Ok(Version::default().bumped(self.node));
        };
        let mut version = old.version.clone();
        let others = self
            .doc
            .get_many(Query::key_exact(path))
            .await?
            .collect::<Vec<_>>()
            .await;
        for entry in others {
            let entry = entry?;
            if entry.author() == self.author {
                continue;
            }
            let Ok(bytes) = self.blobs.get_bytes(entry.content_hash()).await else {
                continue;
            };
            if let Ok(meta) = postcard::from_bytes::<FileMeta>(&bytes) {
                if meta.content == old.content {
                    version.merge(&meta.version);
                }
            }
        }
        Ok(version.bumped(self.node))
    }

    async fn write(&mut self, path: &str, meta: FileMeta) -> anyhow::Result<()> {
        self.doc
            .set_bytes(self.author, path.as_bytes().to_vec(), meta.to_bytes())
            .await?;
        tracing::debug!("indexed {path}: {meta:?}");
        self.entries.insert(path.to_string(), meta);
        Ok(())
    }
}

/// The entries `author` wrote to `doc`.
async fn read_entries(
    doc: &Doc,
    blobs: &FsStore,
    author: AuthorId,
) -> anyhow::Result<HashMap<String, FileMeta>> {
    let entries = doc
        .get_many(Query::author(author))
        .await?
        .collect::<Vec<_>>()
        .await;
    let mut out = HashMap::new();
    for entry in entries {
        let entry = entry?;
        let Ok(path) = std::str::from_utf8(entry.key()) else {
            continue;
        };
        let Ok(bytes) = blobs.get_bytes(entry.content_hash()).await else {
            continue;
        };
        if let Ok(meta) = postcard::from_bytes::<FileMeta>(&bytes) {
            out.insert(path.to_string(), meta);
        }
    }
    Ok(out)
}

/// This node's entries in folder doc `id`, as stored.
pub async fn read_index(
    docs: &Docs,
    blobs: &FsStore,
    id: NamespaceId,
) -> anyhow::Result<HashMap<String, FileMeta>> {
    let doc = docs.open(id).await?.context("folder doc missing")?;
    let author = docs.author_default().await?;
    read_entries(&doc, blobs, author).await
}

/// Every node's entries in folder doc `id`: path → node → entry. Entries
/// whose metadata hasn't arrived yet are left out.
pub async fn read_all(
    docs: &Docs,
    blobs: &FsStore,
    id: NamespaceId,
) -> anyhow::Result<BTreeMap<String, BTreeMap<EndpointId, FileMeta>>> {
    let doc = docs.open(id).await?.context("folder doc missing")?;
    let entries = doc.get_many(Query::all()).await?.collect::<Vec<_>>().await;
    let mut out: BTreeMap<String, BTreeMap<EndpointId, FileMeta>> = BTreeMap::new();
    for entry in entries {
        let entry = entry?;
        let Ok(path) = std::str::from_utf8(entry.key()) else {
            continue;
        };
        let Ok(bytes) = blobs.get_bytes(entry.content_hash()).await else {
            continue;
        };
        if let Ok(meta) = postcard::from_bytes::<FileMeta>(&bytes) {
            out.entry(path.to_string())
                .or_default()
                .insert(meta.node, meta);
        }
    }
    Ok(out)
}

fn load_ignore(root: &Path) -> Gitignore {
    let mut builder = GitignoreBuilder::new(root);
    let file = root.join(SYNCIGNORE);
    if file.is_file() {
        if let Some(err) = builder.add(&file) {
            tracing::warn!("{}: {err}", file.display());
        }
    }
    builder.build().unwrap_or_else(|err| {
        tracing::warn!("{}: {err}; ignoring nothing", file.display());
        Gitignore::empty()
    })
}

fn is_ignored(ignore: &Gitignore, rel: &str, is_dir: bool) -> bool {
    let name = rel.rsplit('/').next().unwrap_or(rel);
    name.starts_with(TEMP_PREFIX)
        || rel == MARKER
        || ignore
            .matched_path_or_any_parents(Path::new(rel), is_dir)
            .is_ignore()
}

/// Whether `path` is `dir` or inside it. Everything is under `""`.
fn is_under(path: &str, dir: &str) -> bool {
    dir.is_empty()
        || path == dir
        || (path.starts_with(dir) && path.as_bytes().get(dir.len()) == Some(&b'/'))
}

/// Finds the regular files at and under `rel`, skipping ignored paths.
fn walk(root: &Path, rel: &str, ignore: &Gitignore) -> io::Result<Walk> {
    let mut out = Walk::default();
    let meta = match fs::symlink_metadata(root.join(rel)) {
        Ok(meta) => meta,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(out),
        Err(err) => return Err(err),
    };
    let mut stack = vec![(rel.to_string(), meta)];
    while let Some((rel, meta)) = stack.pop() {
        if !rel.is_empty() && is_ignored(ignore, &rel, meta.is_dir()) {
            continue;
        }
        if meta.is_file() {
            out.files.push((rel, Stat::of(&meta)));
        } else if meta.is_dir() {
            let entries = match fs::read_dir(root.join(&rel)) {
                Ok(entries) => entries,
                Err(err) if err.kind() == io::ErrorKind::NotFound => continue,
                Err(err) => {
                    tracing::warn!("can't read {}: {err}", root.join(&rel).display());
                    out.unreadable.push(rel);
                    continue;
                }
            };
            for entry in entries {
                let entry = entry?;
                let Some(name) = entry.file_name().to_str().map(str::to_string) else {
                    tracing::warn!("skipping {}: not UTF-8", entry.path().display());
                    continue;
                };
                let child = if rel.is_empty() {
                    name
                } else {
                    format!("{rel}/{name}")
                };
                // Doesn't follow symlinks.
                match entry.metadata() {
                    Ok(meta) => stack.push((child, meta)),
                    Err(err) if err.kind() == io::ErrorKind::NotFound => {}
                    Err(err) => {
                        tracing::warn!("can't stat {}: {err}", entry.path().display());
                        out.unreadable.push(child);
                    }
                }
            }
        } else {
            tracing::debug!("skipping {rel}: not a regular file");
        }
    }
    Ok(out)
}

/// Hashes the file at `path`, or `None` if it isn't a regular file any more
/// or changed while being read.
fn hash_file(path: &Path) -> io::Result<Option<(Hash, Stat)>> {
    let not_found = |err: io::Error| {
        if err.kind() == io::ErrorKind::NotFound {
            Ok(None)
        } else {
            Err(err)
        }
    };
    let before = match fs::symlink_metadata(path) {
        Ok(meta) if meta.is_file() => meta,
        Ok(_) => return Ok(None),
        Err(err) => return not_found(err),
    };
    let file = match File::open(path) {
        Ok(file) => file,
        Err(err) => return not_found(err),
    };
    let mut hasher = blake3::Hasher::new();
    hasher.update_reader(file)?;
    let after = match fs::symlink_metadata(path) {
        Ok(meta) => meta,
        Err(err) => return not_found(err),
    };
    let stat = Stat::of(&after);
    if Stat::of(&before) != stat {
        return Ok(None);
    }
    Ok(Some((hasher.finalize().into(), stat)))
}

/// Keeps `index` in line with its folder until the task is dropped: a full
/// reconcile now, then the paths the watcher reports, each once it has been
/// quiet for `debounce`. `stats` is updated after every pass.
pub(crate) async fn watch(
    mut index: FolderIndex,
    name: String,
    debounce: Duration,
    stats: Arc<Mutex<IndexStats>>,
) {
    let root = index.root().to_path_buf();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let watcher = notify::recommended_watcher(move |event| {
        let _ = tx.send(event);
    });
    let mut watcher = match watcher {
        Ok(watcher) => watcher,
        Err(err) => {
            tracing::warn!("folder {name}: can't watch {}: {err}", root.display());
            return;
        }
    };
    // Watch before the first scan, so nothing changed during it is missed.
    if let Err(err) = watcher.watch(&root, RecursiveMode::Recursive) {
        tracing::warn!("folder {name}: can't watch {}: {err}", root.display());
        return;
    }
    let pass = |index: &mut FolderIndex, written: anyhow::Result<usize>, what: &str| {
        match written {
            Ok(0) => tracing::debug!("folder {name}: {what}: no changes"),
            Ok(n) => tracing::info!("folder {name}: {what}: {n} entries updated"),
            Err(err) => tracing::warn!("folder {name}: {what}: {err:#}"),
        }
        *stats.lock().unwrap() = index.stats();
    };
    let written = index.reconcile("").await;
    let failed = written.is_err();
    pass(&mut index, written, "scan");
    if failed && check_root(&root).is_err() {
        // `Folders::check` resumes it once the marker is back.
        return;
    }
    let s = index.stats();
    tracing::info!(
        "folder {name}: watching {}, {} files, {}",
        root.display(),
        s.files,
        crate::peers::bytes(s.bytes)
    );

    let mut pending: HashSet<String> = HashSet::new();
    let mut full = false;
    let mut first: Option<Instant> = None;
    loop {
        let event = match first {
            None => match rx.recv().await {
                Some(event) => Some(event),
                None => return,
            },
            Some(first) => {
                let max_wait = (first + debounce * MAX_DELAY_INTERVALS)
                    .saturating_duration_since(Instant::now());
                match tokio::time::timeout(debounce.min(max_wait), rx.recv()).await {
                    Ok(Some(event)) => Some(event),
                    Ok(None) => return,
                    Err(_) => None,
                }
            }
        };
        match event {
            Some(Ok(event)) => {
                let wanted = match event.kind {
                    EventKind::Access(AccessKind::Close(AccessMode::Write)) => true,
                    EventKind::Access(_) => false,
                    _ => true,
                };
                if event.need_rescan() {
                    full = true;
                } else if wanted {
                    for path in &event.paths {
                        match path.strip_prefix(&root).ok().and_then(Path::to_str) {
                            Some("") | None => full = true,
                            Some(SYNCIGNORE) => {
                                index.reload_ignore();
                                full = true;
                            }
                            Some(rel) => {
                                pending.insert(rel.to_string());
                            }
                        }
                    }
                }
                if (full || !pending.is_empty()) && first.is_none() {
                    first = Some(Instant::now());
                }
            }
            Some(Err(err)) => {
                tracing::warn!("folder {name}: watcher: {err}");
                full = true;
                first.get_or_insert_with(Instant::now);
            }
            None => {
                // Quiet for `debounce`, or waited long enough: index.
                let written = if full {
                    index.reconcile("").await
                } else {
                    let mut total = 0;
                    let mut result = Ok(0);
                    for rel in &pending {
                        match index.reconcile(rel).await {
                            Ok(n) => total += n,
                            Err(err) => {
                                result = Err(err);
                                break;
                            }
                        }
                    }
                    result.map(|_: usize| total)
                };
                let what = if full { "rescan" } else { "changes" };
                let failed = written.is_err();
                pass(&mut index, written, what);
                if failed && check_root(&root).is_err() {
                    return;
                }
                pending.clear();
                full = false;
                first = None;
            }
        }
    }
}

fn unix_now_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}
