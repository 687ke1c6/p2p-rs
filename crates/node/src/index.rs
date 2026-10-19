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
//!
//! With content transfer on, every indexed file is also added to the blob
//! store by reference (not copied), so other nodes can fetch it, and the
//! index task makes the changes the content task asks for ([`Apply`]):
//! the disk and this node's entries only ever change in this task.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{self, File, Metadata};
use std::io::{self, Write as _};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, UNIX_EPOCH};

use anyhow::Context;
use ignore::gitignore::{Gitignore, GitignoreBuilder};
use iroh::EndpointId;
use iroh_blobs::api::blobs::{AddPathOptions, ImportMode};
use iroh_blobs::store::fs::FsStore;
use iroh_blobs::{BlobFormat, Hash};
use iroh_docs::api::Doc;
use iroh_docs::protocol::Docs;
use iroh_docs::store::Query;
use iroh_docs::{AuthorId, NamespaceId};
use n0_future::StreamExt;
use notify::event::{AccessKind, AccessMode};
use notify::{EventKind, RecursiveMode, Watcher};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};

use crate::view::Version;

/// The ignore file at a folder's root, in `.gitignore` syntax.
pub const SYNCIGNORE: &str = ".syncignore";

/// Files starting with this are the node's own temporary files, always
/// ignored.
pub const TEMP_PREFIX: &str = ".sync-node-tmp-";

/// The file placement in progress, at the folder root: a placement writes
/// it before moving the file into place and removes it once the entry is
/// written. If the node stops in between, opening the index finishes the
/// placement; otherwise the next scan would index the file as a new local
/// file, whose version would then beat a concurrent delete or rename made
/// elsewhere. A temporary file, so never indexed.
const JOURNAL: &str = ".sync-node-tmp-placing";

/// What [`JOURNAL`] holds: the entry to write once the file is in place.
#[derive(Debug, Serialize, Deserialize)]
struct Placing {
    path: String,
    meta: FileMeta,
}

/// Marks a folder's root. A folder whose marker is missing is paused rather
/// than scanned: its directory has moved, been deleted or isn't mounted, and
/// scanning it would tombstone every file. Always ignored, so never synced.
pub const MARKER: &str = ".syncfolder";

/// Whether `rel` is safe to write under a folder root: relative,
/// `/`-separated, with no empty, `.` or `..` components and no NUL. Paths come
/// from other nodes' entries, so this is checked before anything is written.
pub fn safe_path(rel: &str) -> bool {
    !rel.is_empty()
        && !rel.contains('\0')
        && rel
            .split('/')
            .all(|part| !part.is_empty() && part != "." && part != "..")
}

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

    fn stat(&self) -> Stat {
        Stat {
            size: self.size,
            mtime_ns: self.mtime_ns,
        }
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

/// A change the content task asks the index task to make. `done` gets
/// whether it was made: it isn't if this node's entry is no longer `expect`,
/// or the disk no longer matches it (a local change not indexed yet).
#[derive(Debug)]
pub(crate) enum Apply {
    /// Put `meta`'s content at `path` and make `meta` this node's entry.
    Place {
        path: String,
        expect: Option<FileMeta>,
        meta: FileMeta,
        done: oneshot::Sender<anyhow::Result<bool>>,
    },
    /// Delete the file at `path` and tombstone it with `version`.
    Delete {
        path: String,
        expect: FileMeta,
        version: Version,
        done: oneshot::Sender<anyhow::Result<bool>>,
    },
    /// A conflict this node's version of `path` lost: rename it to `copy`,
    /// then put `meta`'s content (the winner) at `path`.
    Resolve {
        path: String,
        expect: FileMeta,
        copy: String,
        meta: FileMeta,
        done: oneshot::Sender<anyhow::Result<bool>>,
    },
    /// A rename elsewhere: move this node's file at `from` to `to`, rather
    /// than fetch `to` and delete `from`. `meta` becomes the entry at `to`,
    /// and `from` is tombstoned with `tombstone`.
    Move {
        from: String,
        from_expect: FileMeta,
        to: String,
        to_expect: Option<FileMeta>,
        meta: FileMeta,
        tombstone: Version,
        done: oneshot::Sender<anyhow::Result<bool>>,
    },
}

pub struct FolderIndex {
    root: PathBuf,
    doc: Doc,
    blobs: FsStore,
    author: AuthorId,
    node: EndpointId,
    entries: HashMap<String, FileMeta>,
    ignore: Gitignore,
    /// Content transfer is on: files are added to the blob store.
    content: bool,
}

impl FolderIndex {
    /// Opens `node`'s index of the folder at `root`, kept in doc `id`. With
    /// `content`, indexed files are also added to the blob store.
    pub async fn open(
        docs: &Docs,
        blobs: &FsStore,
        id: NamespaceId,
        root: PathBuf,
        node: EndpointId,
        content: bool,
    ) -> anyhow::Result<Self> {
        let doc = docs.open(id).await?.context("folder doc missing")?;
        let author = docs.author_default().await?;
        let entries = read_entries(&doc, blobs, author).await?;
        let ignore = load_ignore(&root);
        let mut index = Self {
            root,
            doc,
            blobs: blobs.clone(),
            author,
            node,
            entries,
            ignore,
            content,
        };
        index.finish_journal().await?;
        Ok(index)
    }

    /// Finishes a placement the node stopped in the middle of (see
    /// [`JOURNAL`]): writes its entry if the file is in place as planned.
    async fn finish_journal(&mut self) -> anyhow::Result<()> {
        let journal = self.root.join(JOURNAL);
        let bytes = match fs::read(&journal) {
            Ok(bytes) => bytes,
            Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(err) => return Err(err).context("reading the placement journal"),
        };
        if let Ok(Placing { path, meta }) = postcard::from_bytes(&bytes) {
            let placed = safe_path(&path)
                && fs::symlink_metadata(self.root.join(&path))
                    .is_ok_and(|m| m.is_file() && Stat::of(&m) == meta.stat());
            if placed && self.entries.get(&path) != Some(&meta) {
                tracing::debug!("finishing the placement of {path}");
                self.write(&path, meta).await?;
            }
        }
        let _ = fs::remove_file(&journal);
        Ok(())
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
        let unchanged = old.as_ref().and_then(|old| {
            let same = old.size == stat.size && old.mtime_ns == stat.mtime_ns;
            old.content.filter(|_| same)
        });
        if let Some(hash) = unchanged {
            // Indexed before content transfer was on, or the store lost it.
            if !self.content || self.blobs.has(hash).await? {
                return Ok(false);
            }
        }
        let abs = self.root.join(path);
        let hashed = if self.content {
            self.import(abs).await?
        } else {
            tokio::task::spawn_blocking(move || hash_file(&abs)).await??
        };
        let Some((hash, stat)) = hashed else {
            // Gone or changed while hashing: its next event brings it back.
            return Ok(false);
        };
        if unchanged == Some(hash) && Some(stat) == old.as_ref().map(FileMeta::stat) {
            // Only added to the store.
            return Ok(false);
        }
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

    /// Adds the file at `abs` to the blob store by reference, returning its
    /// hash, or `None` if it isn't a regular file any more or changed while
    /// being read.
    async fn import(&self, abs: PathBuf) -> anyhow::Result<Option<(Hash, Stat)>> {
        let stat = |path: PathBuf| async move {
            match tokio::fs::symlink_metadata(&path).await {
                Ok(meta) if meta.is_file() => Ok(Some(Stat::of(&meta))),
                Ok(_) => Ok(None),
                Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
                Err(err) => Err(err),
            }
        };
        let Some(before) = stat(abs.clone()).await? else {
            return Ok(None);
        };
        let tag = self
            .blobs
            .add_path_with_opts(AddPathOptions {
                path: abs.clone(),
                format: BlobFormat::Raw,
                mode: ImportMode::TryReference,
            })
            .temp_tag()
            .await;
        let tag = match tag {
            Ok(tag) => tag,
            // Gone while being read.
            Err(_) if stat(abs.clone()).await?.is_none() => return Ok(None),
            Err(err) => return Err(err).context("adding to the blob store"),
        };
        if stat(abs).await? != Some(before) {
            return Ok(None);
        }
        Ok(Some((tag.hash(), before)))
    }

    /// Puts blob `meta.content` at `path` and makes `meta` (with the size
    /// and modification time the file ends up with) this node's entry, if
    /// the entry is still `expect` and the disk matches it. The blob is
    /// written to a temporary file next to the target, checked against its
    /// hash, given the entry's modification time, and only then moved into
    /// place. Returns whether it placed the file.
    pub async fn place(
        &mut self,
        path: &str,
        expect: Option<&FileMeta>,
        meta: FileMeta,
    ) -> anyhow::Result<bool> {
        check_root(&self.root)?;
        self.check_writable(path)?;
        if self.entries.get(path) != expect {
            return Ok(false);
        }
        let expect = expect.filter(|e| !e.is_deleted()).map(FileMeta::stat);
        self.place_at(path, expect, meta).await
    }

    /// Refuses a path from another node that isn't safe to write here.
    fn check_writable(&self, path: &str) -> anyhow::Result<()> {
        if !safe_path(path) || self.is_ignored(path, false) {
            anyhow::bail!("refusing to write {path:?}: unsafe or ignored path");
        }
        Ok(())
    }

    /// Writes `meta`'s content at `path`, if the disk there matches
    /// `expect`, and makes `meta` the entry. The content comes from the blob
    /// store, or failing that from another of this node's files with the
    /// same content: a file added by reference can't be read from the store
    /// once it has moved.
    async fn place_at(
        &mut self,
        path: &str,
        expect: Option<Stat>,
        mut meta: FileMeta,
    ) -> anyhow::Result<bool> {
        let hash = meta.content.context("placing a tombstone")?;
        let abs = self.root.join(path);
        let (root, rel, target) = (self.root.clone(), path.to_string(), abs.clone());
        let ready = tokio::task::spawn_blocking(move || -> anyhow::Result<bool> {
            make_parents(&root, &rel)?;
            Ok(disk_matches(&target, expect)?)
        })
        .await??;
        if !ready {
            return Ok(false);
        }
        let temp = abs.with_file_name(format!("{TEMP_PREFIX}{:016x}", rand::random::<u64>()));
        let result = async {
            let exported = self.blobs.export(hash, &temp).finish().await;
            if let Err(err) = exported {
                let Some(source) = self.local_copy(hash, path) else {
                    return Err(err).context("exporting from the blob store");
                };
                let temp = temp.clone();
                tokio::task::spawn_blocking(move || fs::copy(source, temp))
                    .await?
                    .context("copying a local file with the same content")?;
            }
            meta.node = self.node;
            let placing = Placing {
                path: path.to_string(),
                meta: meta.clone(),
            };
            let (temp, journal) = (temp.clone(), self.root.join(JOURNAL));
            tokio::task::spawn_blocking(move || {
                finish_place(&temp, &abs, hash, expect, &journal, placing)
            })
            .await?
        }
        .await;
        let stat = match result {
            Ok(Some(stat)) => stat,
            other => {
                let _ = fs::remove_file(&temp);
                return other.map(|_| false);
            }
        };
        meta.size = stat.size;
        meta.mtime_ns = stat.mtime_ns;
        self.write(path, meta).await?;
        let _ = fs::remove_file(self.root.join(JOURNAL));
        Ok(true)
    }

    /// Another of this node's files holding `hash`, as its entry says,
    /// other than `except`.
    fn local_copy(&self, hash: Hash, except: &str) -> Option<PathBuf> {
        self.entries
            .iter()
            .find(|(path, meta)| *path != except && meta.content == Some(hash))
            .map(|(path, _)| self.root.join(path))
    }

    /// Resolves a conflict this node's version lost: renames its file at
    /// `path` (still as `expect` says) to `copy`, indexes the copy as a new
    /// file, and puts the winner, `meta`, at `path`. Returns whether it did.
    pub async fn resolve(
        &mut self,
        path: &str,
        expect: &FileMeta,
        copy: &str,
        meta: FileMeta,
    ) -> anyhow::Result<bool> {
        check_root(&self.root)?;
        self.check_writable(path)?;
        self.check_writable(copy)?;
        if expect.is_deleted()
            || self.entries.get(path) != Some(expect)
            || self.entries.get(copy).is_some_and(|e| !e.is_deleted())
        {
            return Ok(false);
        }
        let (from, to, expect) = (self.root.join(path), self.root.join(copy), expect.stat());
        let moved = tokio::task::spawn_blocking(move || -> io::Result<Option<Stat>> {
            if !disk_matches(&from, Some(expect))? || !disk_matches(&to, None)? {
                return Ok(None);
            }
            fs::rename(&from, &to)?;
            Ok(Some(Stat::of(&fs::symlink_metadata(&to)?)))
        })
        .await??;
        let Some(stat) = moved else {
            return Ok(false);
        };
        self.index_file(copy, stat).await?;
        // The name is free now. If this fails, the next reconcile tombstones
        // `path`, the winner's edit beats that, and a later pass fetches it.
        self.place_at(path, None, meta).await
    }

    /// Moves this node's file at `from` (still as `from_expect` says) to
    /// `to`, where this node has nothing (`to_expect` is missing or a
    /// tombstone), gives it `meta`'s modification time, and records `meta`
    /// at `to` and a tombstone with version `tombstone` at `from`. Returns
    /// whether it did.
    pub async fn move_file(
        &mut self,
        from: &str,
        from_expect: &FileMeta,
        to: &str,
        to_expect: Option<&FileMeta>,
        mut meta: FileMeta,
        tombstone: Version,
    ) -> anyhow::Result<bool> {
        check_root(&self.root)?;
        self.check_writable(to)?;
        if !safe_path(from)
            || from_expect.is_deleted()
            || from_expect.content != meta.content
            || to_expect.is_some_and(|e| !e.is_deleted())
            || self.entries.get(from) != Some(from_expect)
            || self.entries.get(to) != to_expect
        {
            return Ok(false);
        }
        let (root, src, dst) = (self.root.clone(), from.to_string(), to.to_string());
        let (expect, mtime_ns) = (from_expect.stat(), meta.mtime_ns);
        let moved = tokio::task::spawn_blocking(move || -> anyhow::Result<Option<Stat>> {
            let (from, to) = (root.join(&src), root.join(&dst));
            make_parents(&root, &dst)?;
            if !disk_matches(&from, Some(expect))? || !disk_matches(&to, None)? {
                return Ok(None);
            }
            File::options()
                .write(true)
                .open(&from)?
                .set_modified(UNIX_EPOCH + Duration::from_nanos(mtime_ns))?;
            fs::rename(&from, &to)?;
            remove_empty_parents(&root, &src);
            Ok(Some(Stat::of(&fs::symlink_metadata(&to)?)))
        })
        .await??;
        let Some(stat) = moved else {
            return Ok(false);
        };
        meta.size = stat.size;
        meta.mtime_ns = stat.mtime_ns;
        meta.node = self.node;
        self.write(to, meta).await?;
        let gone = FileMeta {
            content: None,
            size: 0,
            mtime_ns: unix_now_ns(),
            node: self.node,
            version: tombstone,
        };
        self.write(from, gone).await?;
        if self.content {
            // Records the new path in the blob store, so the file can still be
            // sent from here.
            if let Err(err) = self.import(self.root.join(to)).await {
                tracing::debug!("adding moved {to} to the blob store: {err:#}");
            }
        }
        Ok(true)
    }

    /// Deletes the file at `path` and tombstones it with `version`, if this
    /// node's entry is still `expect` and the file matches it (or is already
    /// gone). Directories left empty are removed too. Returns whether it did.
    pub async fn remove(
        &mut self,
        path: &str,
        expect: &FileMeta,
        version: Version,
    ) -> anyhow::Result<bool> {
        check_root(&self.root)?;
        if !safe_path(path) || expect.is_deleted() || self.entries.get(path) != Some(expect) {
            return Ok(false);
        }
        let (root, rel, expect) = (self.root.clone(), path.to_string(), expect.stat());
        let removed = tokio::task::spawn_blocking(move || -> io::Result<bool> {
            let abs = root.join(&rel);
            match fs::symlink_metadata(&abs) {
                Ok(meta) if meta.is_file() && Stat::of(&meta) == expect => fs::remove_file(&abs)?,
                Ok(_) => return Ok(false),
                Err(err) if err.kind() == io::ErrorKind::NotFound => {}
                Err(err) => return Err(err),
            }
            remove_empty_parents(&root, &rel);
            Ok(true)
        })
        .await??;
        if !removed {
            return Ok(false);
        }
        let meta = FileMeta {
            content: None,
            size: 0,
            mtime_ns: unix_now_ns(),
            node: self.node,
            version,
        };
        self.write(path, meta).await?;
        Ok(true)
    }

    /// Makes a change the content task asked for and reports back.
    async fn apply(&mut self, apply: Apply) {
        match apply {
            Apply::Place {
                path,
                expect,
                meta,
                done,
            } => {
                let _ = done.send(self.place(&path, expect.as_ref(), meta).await);
            }
            Apply::Delete {
                path,
                expect,
                version,
                done,
            } => {
                let _ = done.send(self.remove(&path, &expect, version).await);
            }
            Apply::Resolve {
                path,
                expect,
                copy,
                meta,
                done,
            } => {
                let _ = done.send(self.resolve(&path, &expect, &copy, meta).await);
            }
            Apply::Move {
                from,
                from_expect,
                to,
                to_expect,
                meta,
                tombstone,
                done,
            } => {
                let result = self
                    .move_file(
                        &from,
                        &from_expect,
                        &to,
                        to_expect.as_ref(),
                        meta,
                        tombstone,
                    )
                    .await;
                let _ = done.send(result);
            }
        }
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

/// Whether the disk at `abs` is as this node's entry says: a regular file
/// with `expect`'s size and modification time, or nothing if `expect` is
/// `None`.
fn disk_matches(abs: &Path, expect: Option<Stat>) -> io::Result<bool> {
    match fs::symlink_metadata(abs) {
        Ok(meta) => Ok(meta.is_file() && Some(Stat::of(&meta)) == expect),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(expect.is_none()),
        Err(err) => Err(err),
    }
}

/// Creates the directories above `rel` under `root`, one at a time, refusing
/// to go through anything that isn't a real directory, such as a symlink.
fn make_parents(root: &Path, rel: &str) -> anyhow::Result<()> {
    let mut dir = root.to_path_buf();
    let parts: Vec<&str> = rel.split('/').collect();
    for part in &parts[..parts.len() - 1] {
        dir.push(part);
        match fs::symlink_metadata(&dir) {
            Ok(meta) if meta.is_dir() => {}
            Ok(_) => anyhow::bail!("{} is not a directory", dir.display()),
            Err(err) if err.kind() == io::ErrorKind::NotFound => match fs::create_dir(&dir) {
                Ok(()) => {}
                Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {}
                Err(err) => return Err(err).context(format!("creating {}", dir.display())),
            },
            Err(err) => return Err(err).context(format!("reading {}", dir.display())),
        }
    }
    Ok(())
}

/// Removes the directories above `rel` under `root` that are now empty,
/// deepest first.
fn remove_empty_parents(root: &Path, rel: &str) {
    let mut parent = rel;
    while let Some((dir, _)) = parent.rsplit_once('/') {
        if fs::remove_dir(root.join(dir)).is_err() {
            return;
        }
        parent = dir;
    }
}

/// Checks the exported file at `temp` against `hash` and the entry's size,
/// gives it the entry's modification time and moves it to `abs`, if the disk
/// there still matches `expect`. The entry, with the file's final size and
/// modification time, goes to `journal` first. Returns that size and
/// modification time, or `None` if the disk changed.
fn finish_place(
    temp: &Path,
    abs: &Path,
    hash: Hash,
    expect: Option<Stat>,
    journal: &Path,
    mut placing: Placing,
) -> anyhow::Result<Option<Stat>> {
    let (size, mtime_ns) = (placing.meta.size, placing.meta.mtime_ns);
    let file = File::options().read(true).write(true).open(temp)?;
    let mut hasher = blake3::Hasher::new();
    hasher.update_reader(&file)?;
    let (got, len) = (Hash::from(hasher.finalize()), file.metadata()?.len());
    if got != hash || len != size {
        anyhow::bail!(
            "exported {} doesn't match: got {} ({len} bytes)",
            hash.fmt_short(),
            got.fmt_short()
        );
    }
    file.set_modified(UNIX_EPOCH + Duration::from_nanos(mtime_ns))?;
    file.sync_all()?;
    // The file system may store a coarser modification time.
    let stat = Stat::of(&file.metadata()?);
    drop(file);
    if !disk_matches(abs, expect)? {
        return Ok(None);
    }
    placing.meta.size = stat.size;
    placing.meta.mtime_ns = stat.mtime_ns;
    let mut record = File::create(journal)?;
    record.write_all(&postcard::to_stdvec(&placing)?)?;
    record.sync_all()?;
    fs::rename(temp, abs)?;
    Ok(Some(stat))
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
/// quiet for `debounce`. In between, makes the changes the content task
/// sends on `apply`. `stats` is updated after every pass and change.
pub(crate) async fn watch(
    mut index: FolderIndex,
    name: String,
    debounce: Duration,
    stats: Arc<Mutex<IndexStats>>,
    mut apply: Option<mpsc::Receiver<Apply>>,
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
        let quiet = async {
            match first {
                None => std::future::pending().await,
                Some(first) => {
                    let max_wait = (first + debounce * MAX_DELAY_INTERVALS)
                        .saturating_duration_since(Instant::now());
                    tokio::time::sleep(debounce.min(max_wait)).await
                }
            }
        };
        let requested = async {
            match apply.as_mut() {
                Some(apply) => apply.recv().await,
                None => std::future::pending().await,
            }
        };
        let event = tokio::select! {
            event = rx.recv() => match event {
                Some(event) => Some(event),
                None => return,
            },
            () = quiet => None,
            Some(request) = requested => {
                index.apply(request).await;
                *stats.lock().unwrap() = index.stats();
                continue;
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
