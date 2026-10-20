//! The block protocol (see `plans/blocks/spec.md`): file contents split into
//! blocks, each identified by its BLAKE3 hash and listed in the file's index
//! entry, and moved between nodes one block per request.
//!
//! A request names a folder, a path, the content (version) of the file and a
//! block number, on its own bidirectional QUIC stream; the answer is a
//! header and the block's bytes, or a refusal. Messages are a little-endian
//! `u32` length and a postcard body of at most [`MAX_HEADER`] bytes.
//!
//! A node serves only blocks of files its own index lists, to members that
//! keep the folder, and only after reading them from disk and checking them
//! against its entry: never a file being edited, never a path outside the
//! folder.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fs::{self, File};
use std::io::{self, Read};
use std::os::unix::fs::{FileExt, MetadataExt};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::Context;
use iroh::endpoint::{Connection, RecvStream, SendStream, VarInt};
use iroh::protocol::{AcceptError, ProtocolHandler};
use iroh::{Endpoint, EndpointId};
use iroh_blobs::Hash;
use iroh_docs::NamespaceId;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::sync::{watch, Semaphore};
use tokio::task::{AbortHandle, JoinSet};

use crate::folder::Folders;
use crate::index::{safe_path, Apply, FileMeta, Stat, TEMP_PREFIX};

/// The block protocol's ALPN. Gated: members only.
pub const ALPN: &[u8] = b"/sync-node/blocks/0";

/// Largest message (request or response header) accepted.
pub const MAX_HEADER: u32 = 8 << 10;

/// How long a server waits for a request on a new stream.
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// How long a client waits for a block.
pub const BLOCK_TIMEOUT: Duration = Duration::from_secs(30);

/// Requests a node serves at once; more are refused as `Busy`.
pub const MAX_SERVING: usize = 64;

/// Stream error code for a request the server couldn't read.
pub const BAD_REQUEST: u32 = 400;

/// Requests a client keeps open to one provider.
pub const WINDOW: usize = 8;

/// How long a client waits to connect to a provider.
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// How long a client waits before asking a busy provider again, and how many
/// times it does before moving on.
pub const BUSY_WAIT: Duration = Duration::from_millis(200);
pub const BUSY_TRIES: u32 = 25;

/// One block of one version of one file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockRequest {
    pub folder: NamespaceId,
    /// Relative to the folder root, `/`-separated.
    pub path: String,
    /// The version of the file, by its content hash.
    pub content: Hash,
    /// The block number.
    pub index: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BlockResponse {
    /// Followed by exactly `len` bytes.
    Data {
        len: u32,
    },
    Refused(Refusal),
}

/// Why a server didn't send a block.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
pub enum Refusal {
    #[error("doesn't keep this folder")]
    NotKept,
    #[error("no such file or version")]
    NoSuchFile,
    #[error("the file changed")]
    Changed,
    #[error("busy")]
    Busy,
    #[error("refused by the relay policy")]
    Policy,
}

/// The smallest block size.
pub const MIN_BLOCK_SIZE: u32 = 128 << 10;

/// The largest block size.
pub const MAX_BLOCK_SIZE: u32 = 16 << 20;

/// Files are split into at most this many blocks, up to the largest block
/// size.
pub const MAX_BLOCKS: u64 = 2048;

/// The block size for a file of `size` bytes: the smallest power of two from
/// [`MIN_BLOCK_SIZE`] to [`MAX_BLOCK_SIZE`] that splits it into at most
/// [`MAX_BLOCKS`] blocks. 0 for an empty file, which has no blocks. Depends
/// on the size alone, so the same content always has the same block list.
pub fn block_size(size: u64) -> u32 {
    if size == 0 {
        return 0;
    }
    let mut block = MIN_BLOCK_SIZE;
    while block < MAX_BLOCK_SIZE && size.div_ceil(u64::from(block)) > MAX_BLOCKS {
        block *= 2;
    }
    block
}

/// A file's content hash and block list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hashed {
    /// BLAKE3 of the whole content, as `b3sum` prints it.
    pub content: Hash,
    pub block_size: u32,
    /// BLAKE3 of each block, in order. The last block may be short.
    pub blocks: Vec<Hash>,
}

/// Hashes content fed to it in order into its whole hash and block list, in
/// one pass.
#[derive(Debug)]
pub struct Hasher {
    whole: blake3::Hasher,
    block: blake3::Hasher,
    block_size: u32,
    /// Bytes in the current block so far.
    filled: u64,
    blocks: Vec<Hash>,
}

impl Hasher {
    /// A hasher for content of `size` bytes, which sets the block size.
    pub fn new(size: u64) -> Self {
        Self {
            whole: blake3::Hasher::new(),
            block: blake3::Hasher::new(),
            block_size: block_size(size),
            filled: 0,
            blocks: Vec::new(),
        }
    }

    pub fn update(&mut self, mut data: &[u8]) {
        self.whole.update(data);
        while !data.is_empty() {
            let room = u64::from(self.block_size) - self.filled;
            let take = data.len().min(usize::try_from(room).unwrap_or(usize::MAX));
            self.block.update(&data[..take]);
            self.filled += take as u64;
            if self.filled == u64::from(self.block_size) {
                self.blocks.push(self.block.finalize().into());
                self.block.reset();
                self.filled = 0;
            }
            data = &data[take..];
        }
    }

    pub fn finish(mut self) -> Hashed {
        if self.filled > 0 {
            self.blocks.push(self.block.finalize().into());
        }
        Hashed {
            content: self.whole.finalize().into(),
            block_size: self.block_size,
            blocks: self.blocks,
        }
    }
}

/// Hashes everything `reader` yields, which the caller expects to be `size`
/// bytes (it sets the block size). Returns the hash and the number of bytes
/// read, which differs from `size` if the content changed meanwhile.
pub fn hash_reader(size: u64, mut reader: impl Read) -> io::Result<(Hashed, u64)> {
    let mut hasher = Hasher::new(size);
    let mut buf = vec![0; 64 << 10];
    let mut read = 0;
    loop {
        match reader.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => {
                hasher.update(&buf[..n]);
                read += n as u64;
            }
            Err(err) if err.kind() == io::ErrorKind::Interrupted => {}
            Err(err) => return Err(err),
        }
    }
    Ok((hasher.finish(), read))
}

/// Writes one framed message.
pub async fn write_message<T: Serialize>(send: &mut SendStream, msg: &T) -> anyhow::Result<()> {
    let body = postcard::to_stdvec(msg)?;
    let len = u32::try_from(body.len())
        .ok()
        .filter(|len| *len <= MAX_HEADER)
        .context("message too large")?;
    send.write_all(&len.to_le_bytes()).await?;
    send.write_all(&body).await?;
    Ok(())
}

/// Reads one framed message, refusing one over [`MAX_HEADER`].
pub async fn read_message<T: DeserializeOwned>(recv: &mut RecvStream) -> anyhow::Result<T> {
    let mut len = [0; 4];
    recv.read_exact(&mut len).await?;
    let len = u32::from_le_bytes(len);
    anyhow::ensure!(len <= MAX_HEADER, "message of {len} bytes is too large");
    let mut body = vec![0; len as usize];
    recv.read_exact(&mut body).await?;
    Ok(postcard::from_bytes(&body)?)
}

/// Asks the node at the other end of `conn` for one block. The bytes aren't
/// checked here: the caller compares them with its block list.
pub async fn request_block(
    conn: &Connection,
    request: &BlockRequest,
) -> anyhow::Result<Result<Vec<u8>, Refusal>> {
    let exchange = async {
        let (mut send, mut recv) = conn.open_bi().await?;
        write_message(&mut send, request).await?;
        send.finish()?;
        match read_message::<BlockResponse>(&mut recv).await? {
            BlockResponse::Data { len } => {
                anyhow::ensure!(len <= MAX_BLOCK_SIZE, "block of {len} bytes is too large");
                let mut block = vec![0; len as usize];
                recv.read_exact(&mut block).await?;
                Ok(Ok(block))
            }
            BlockResponse::Refused(refusal) => Ok(Err(refusal)),
        }
    };
    tokio::time::timeout(BLOCK_TIMEOUT, exchange)
        .await
        .context("timed out waiting for a block")?
}

/// The serving side of the block protocol.
#[derive(Debug, Clone)]
pub struct BlocksProtocol {
    folders: Folders,
    serving: Arc<Semaphore>,
}

impl BlocksProtocol {
    pub(crate) fn new(folders: Folders) -> Self {
        Self {
            folders,
            serving: Arc::new(Semaphore::new(MAX_SERVING)),
        }
    }

    /// Answers one request on its own stream.
    async fn answer(self, remote: EndpointId, mut send: SendStream, mut recv: RecvStream) {
        let request =
            tokio::time::timeout(REQUEST_TIMEOUT, read_message::<BlockRequest>(&mut recv));
        let request = match request.await {
            Ok(Ok(request)) => request,
            failed => {
                let why = match failed {
                    Ok(Err(err)) => format!("{err:#}"),
                    _ => "timed out".to_string(),
                };
                tracing::debug!("{}: bad block request: {why}", remote.fmt_short());
                let _ = recv.stop(VarInt::from_u32(BAD_REQUEST));
                let _ = send.reset(VarInt::from_u32(BAD_REQUEST));
                return;
            }
        };
        let sent = async {
            match self.serve(remote, &request).await {
                Ok(block) => {
                    let len = block.len() as u32;
                    write_message(&mut send, &BlockResponse::Data { len }).await?;
                    send.write_all(&block).await?;
                }
                Err(refusal) => {
                    tracing::debug!(
                        "{}: refused block {} of {}: {refusal}",
                        remote.fmt_short(),
                        request.index,
                        request.path
                    );
                    write_message(&mut send, &BlockResponse::Refused(refusal)).await?;
                }
            }
            send.finish()?;
            anyhow::Ok(())
        };
        if let Err(err) = sent.await {
            tracing::debug!("{}: sending a block: {err:#}", remote.fmt_short());
        }
    }

    /// The block `request` asks for, read from disk and checked against this
    /// node's entry, or why not.
    async fn serve(&self, remote: EndpointId, request: &BlockRequest) -> Result<Vec<u8>, Refusal> {
        let folder = self.folders.serving(request.folder, remote)?;
        if !safe_path(&request.path) {
            return Err(Refusal::NoSuchFile);
        }
        let entry = folder
            .entries
            .get(&request.path)
            .filter(|e| e.content == Some(request.content))
            .ok_or(Refusal::NoSuchFile)?;
        if request.index as usize >= entry.blocks.len() {
            return Err(Refusal::NoSuchFile);
        }
        let _permit = self
            .serving
            .clone()
            .try_acquire_owned()
            .map_err(|_| Refusal::Busy)?;
        let (root, path, index) = (folder.root.clone(), request.path.clone(), request.index);
        let read =
            tokio::task::spawn_blocking(move || read_block(&root, &path, &entry, index)).await;
        match read {
            Ok(Ok(Some(block))) => Ok(block),
            other => {
                if let Ok(Err(err)) = &other {
                    tracing::debug!("reading block {index} of {}: {err}", request.path);
                }
                // The file no longer matches the entry: have the index look
                // at it, in case the watcher hasn't (or can't: same size and
                // time). Dropped if the index task is busy; it's a nudge.
                if let Some(apply) = &folder.apply {
                    let _ = apply.try_send(Apply::Rehash {
                        path: request.path.clone(),
                    });
                }
                Err(Refusal::Changed)
            }
        }
    }
}

impl ProtocolHandler for BlocksProtocol {
    async fn accept(&self, conn: Connection) -> Result<(), AcceptError> {
        let remote = conn.remote_id();
        while let Ok((send, recv)) = conn.accept_bi().await {
            tokio::spawn(self.clone().answer(remote, send, recv));
        }
        Ok(())
    }
}

/// Opens the file at `rel` under `root` if it is a regular file reached
/// through real directories only (no symlink on the way), and the same file
/// after opening as before (nothing swapped in). `None` if not.
fn open_regular(root: &Path, rel: &str) -> io::Result<Option<(File, fs::Metadata)>> {
    let parts: Vec<&str> = rel.split('/').collect();
    let mut dir = PathBuf::from(root);
    for part in &parts[..parts.len() - 1] {
        dir.push(part);
        if !fs::symlink_metadata(&dir)?.is_dir() {
            return Ok(None);
        }
    }
    let abs = root.join(rel);
    let before = fs::symlink_metadata(&abs)?;
    if !before.is_file() {
        return Ok(None);
    }
    let file = File::open(&abs)?;
    let meta = file.metadata()?;
    if (meta.dev(), meta.ino()) != (before.dev(), before.ino()) {
        return Ok(None);
    }
    Ok(Some((file, meta)))
}

/// Where block `index` of the file `meta` describes starts, and its length.
fn span(meta: &FileMeta, index: u32) -> (u64, usize) {
    let offset = u64::from(index) * u64::from(meta.block_size);
    let len = u64::from(meta.block_size).min(meta.size.saturating_sub(offset));
    (offset, len as usize)
}

/// Reads block `index` of the file at `rel` under `root`, if the file is
/// still as `entry` describes it (see [`open_regular`]): with the entry's
/// size and modification time, and a block that hashes to the entry's.
/// `None` if not.
fn read_block(root: &Path, rel: &str, entry: &FileMeta, index: u32) -> io::Result<Option<Vec<u8>>> {
    let Some((file, meta)) = open_regular(root, rel)? else {
        return Ok(None);
    };
    if Stat::of(&meta) != entry.stat() {
        return Ok(None);
    }
    let (offset, len) = span(entry, index);
    let mut block = vec![0; len];
    file.read_exact_at(&mut block, offset)?;
    if Hash::new(&block) != entry.blocks[index as usize] {
        return Ok(None);
    }
    Ok(Some(block))
}

/// The temporary file version `content` of `path` is fetched into: at the
/// folder root (so nothing is written below it before the index task has
/// checked the way), named after the path and version, so a fetch that
/// stopped finds it again.
pub fn staging_path(root: &Path, path: &str, content: Hash) -> PathBuf {
    let mut hasher = blake3::Hasher::new();
    hasher.update(path.as_bytes());
    hasher.update(&[0]);
    hasher.update(content.as_bytes());
    root.join(format!(
        "{TEMP_PREFIX}{}",
        &hasher.finalize().to_hex()[..16]
    ))
}

/// One of this node's files that blocks can be copied from: its path under
/// the folder root and its entry.
pub type Source = (String, Arc<FileMeta>);

/// A staging file being filled (see [`staging_path`]), and the blocks it
/// still lacks.
#[derive(Debug)]
pub struct Staged {
    file: Arc<File>,
    missing: VecDeque<u32>,
    /// Bytes copied from this node's own files.
    pub copied: u64,
    /// Bytes still to fetch.
    pub to_fetch: u64,
}

impl Staged {
    /// Opens the staging file `temp` for version `meta` (or creates it at
    /// full length), keeps the blocks already there that match the block
    /// list, and copies missing blocks from this node's files under `root`,
    /// trying `sources` in order. Every copied block is checked against the
    /// block list before it is written. Blocking.
    pub fn open(temp: &Path, meta: &FileMeta, root: &Path, sources: &[Source]) -> io::Result<Self> {
        let (file, have) = open_staged(temp, meta)?;
        let mut missing: VecDeque<u32> = (0..meta.blocks.len() as u32)
            .filter(|i| !have[*i as usize])
            .collect();
        let copied = copy_local(&file, meta, root, sources, &mut missing)?;
        let to_fetch = missing.iter().map(|&i| span(meta, i).1 as u64).sum();
        Ok(Self {
            file: Arc::new(file),
            missing,
            copied,
            to_fetch,
        })
    }

    /// Whether every block is there.
    pub fn is_complete(&self) -> bool {
        self.missing.is_empty()
    }

    /// Syncs the staging file to disk. Blocking.
    pub fn sync(&self) -> io::Result<()> {
        self.file.sync_all()
    }
}

/// Whether `name` (a file name at a folder root) is a staging file's (see
/// [`staging_path`]), not some other temporary file of this node's.
pub fn is_staging_name(name: &str) -> bool {
    name.strip_prefix(TEMP_PREFIX)
        .is_some_and(|rest| rest.len() == 16 && rest.bytes().all(|b| b.is_ascii_hexdigit()))
}

/// The fetching side of the block protocol: one connection per provider,
/// kept while it stays open.
#[derive(Debug, Clone)]
pub struct Client {
    endpoint: Endpoint,
    conns: Arc<Mutex<HashMap<EndpointId, Connection>>>,
    /// Bytes of verified blocks written, per provider.
    fetched: Arc<Mutex<HashMap<EndpointId, u64>>>,
}

/// What became of one block request.
enum Got {
    /// The block, checked against the block list.
    Block(Vec<u8>),
    Refused(Refusal),
    /// The bytes don't hash to the block list's: the provider is wrong.
    Mismatch,
    /// The request failed: connection, stream or timeout.
    Failed(anyhow::Error),
}

impl Client {
    pub fn new(endpoint: Endpoint) -> Self {
        Self {
            endpoint,
            conns: Arc::default(),
            fetched: Arc::default(),
        }
    }

    /// Bytes of file data fetched from each provider since this node
    /// started: verified blocks written, each counted once, for the
    /// provider that sent it first.
    pub fn fetched(&self) -> HashMap<EndpointId, u64> {
        self.fetched.lock().unwrap().clone()
    }

    /// A connection to `id`, reusing an open one.
    async fn connect(&self, id: EndpointId) -> anyhow::Result<Connection> {
        let open = self.conns.lock().unwrap().get(&id).cloned();
        if let Some(conn) = open.filter(|c| c.close_reason().is_none()) {
            return Ok(conn);
        }
        let conn = tokio::time::timeout(CONNECT_TIMEOUT, self.endpoint.connect(id, ALPN))
            .await
            .context("timed out connecting")??;
        self.conns.lock().unwrap().insert(id, conn.clone());
        Ok(conn)
    }

    /// Fetches the blocks `staged` lacks for version `meta` of `path` in
    /// `folder` from all the providers `offered` at once, as [`Schedule`]
    /// hands them out. The offer may grow during the fetch (a node finished
    /// its own copy): new providers join it. A provider that can't be reached, refuses, fails or
    /// sends a block that doesn't match is dropped for this file, and its
    /// blocks go to the others. Every block is checked against the block list before it is
    /// written. Returns whether the file is complete (and synced to disk);
    /// if not, the staging file keeps what arrived, for a later try.
    pub async fn fetch_file(
        &self,
        folder: NamespaceId,
        path: &str,
        meta: &FileMeta,
        mut offered: watch::Receiver<Vec<EndpointId>>,
        mut staged: Staged,
    ) -> anyhow::Result<bool> {
        let content = meta.content.context("fetching a tombstone")?;
        let request = BlockRequest {
            folder,
            path: path.to_string(),
            content,
            index: 0,
        };
        let mut tasks = JoinSet::new();
        let mut schedule = Schedule::new(staged.missing.drain(..), 0);
        let mut providers = Vec::new();
        let ids = offered.borrow_and_update().clone();
        self.join(&ids, &mut providers, &mut schedule, &mut tasks);
        // Until the offer's sender is gone.
        let mut watching = true;
        // Open requests' tasks, by provider and block number.
        let mut requests: HashMap<(usize, u32), AbortHandle> = HashMap::new();
        // Whether an `Event::Wake` is pending.
        let mut waking = false;
        while !schedule.is_complete() {
            for (i, p) in providers.iter().enumerate() {
                let Some(conn) = p.conn.as_ref().filter(|_| p.ready()) else {
                    continue;
                };
                while let Some(index) = schedule.next(i, Instant::now()) {
                    let conn = conn.clone();
                    let request = BlockRequest {
                        index,
                        ..request.clone()
                    };
                    let expected = meta.blocks[index as usize];
                    let task = tasks.spawn(async move {
                        Event::Block(i, index, fetch_block(&conn, &request, expected).await)
                    });
                    requests.insert((i, index), task);
                }
            }
            // Wake when a request becomes overdue, for the endgame.
            if !waking {
                if let Some(at) = schedule.next_overdue(Instant::now()) {
                    waking = true;
                    tasks.spawn(async move {
                        tokio::time::sleep_until(at.into()).await;
                        Event::Wake
                    });
                }
            }
            let joined = tokio::select! {
                joined = tasks.join_next() => joined,
                changed = offered.changed(), if watching => {
                    match changed {
                        Ok(()) => {
                            let ids = offered.borrow_and_update().clone();
                            self.join(&ids, &mut providers, &mut schedule, &mut tasks);
                        }
                        Err(_) => watching = false,
                    }
                    continue;
                }
            };
            let Some(joined) = joined else {
                // Nothing open, nobody left to ask.
                break;
            };
            let event = match joined {
                Ok(event) => event,
                // A request that was stopped.
                Err(err) if err.is_cancelled() => continue,
                Err(err) => return Err(err.into()),
            };
            let (i, index, got) = match event {
                Event::Connected(i, Ok(conn)) => {
                    providers[i].conn = Some(conn);
                    continue;
                }
                Event::Connected(i, Err(err)) => {
                    let p = &mut providers[i];
                    tracing::debug!("{}: connecting: {err:#}", p.id.fmt_short());
                    p.dropped = true;
                    continue;
                }
                Event::Resume(i) => {
                    providers[i].paused = false;
                    continue;
                }
                Event::Wake => {
                    waking = false;
                    continue;
                }
                Event::Block(i, index, got) => (i, index, got),
            };
            requests.remove(&(i, index));
            if !schedule.is_asked(i, index) {
                // Stopped, but it finished first: another provider sent the
                // block, or this one was dropped.
                continue;
            }
            let p = &mut providers[i];
            let why = match got {
                Got::Block(block) => {
                    // Written here, not in the request's task, so a stopped
                    // request never writes.
                    let (file, offset) = (staged.file.clone(), span(meta, index).0);
                    let len = block.len() as u64;
                    tokio::task::spawn_blocking(move || file.write_all_at(&block, offset))
                        .await?
                        .context("writing the temporary file")?;
                    *self.fetched.lock().unwrap().entry(p.id).or_default() += len;
                    for other in schedule.delivered(i, index, Instant::now()) {
                        if let Some(task) = requests.remove(&(other, index)) {
                            task.abort();
                        }
                    }
                    continue;
                }
                Got::Refused(Refusal::Busy) if p.busy < BUSY_TRIES => {
                    p.busy += 1;
                    schedule.returned(i, index);
                    if !p.paused {
                        p.paused = true;
                        tasks.spawn(async move {
                            tokio::time::sleep(BUSY_WAIT).await;
                            Event::Resume(i)
                        });
                    }
                    continue;
                }
                Got::Refused(refusal) => anyhow::anyhow!("refused block {index}: {refusal}"),
                Got::Mismatch => anyhow::anyhow!("block {index} doesn't match the block list"),
                Got::Failed(err) => err.context(format!("block {index}")),
            };
            tracing::debug!("{}: fetching {path}: {why:#}", p.id.fmt_short());
            p.dropped = true;
            if p.conn.as_ref().is_some_and(|c| c.close_reason().is_some()) {
                self.conns.lock().unwrap().remove(&p.id);
            }
            schedule.returned(i, index);
            for index in schedule.drop_provider(i) {
                if let Some(task) = requests.remove(&(i, index)) {
                    task.abort();
                }
            }
        }
        tasks.abort_all();
        if !schedule.is_complete() {
            return Ok(false);
        }
        tokio::task::spawn_blocking(move || staged.sync()).await??;
        Ok(true)
    }

    /// Adds the providers among `ids` not in `providers` yet, and starts
    /// connecting to them.
    fn join(
        &self,
        ids: &[EndpointId],
        providers: &mut Vec<Provider>,
        schedule: &mut Schedule,
        tasks: &mut JoinSet<Event>,
    ) {
        for &id in ids {
            if providers.iter().any(|p| p.id == id) {
                continue;
            }
            if !providers.is_empty() {
                tracing::debug!("{}: joins the fetch", id.fmt_short());
            }
            let (i, client) = (schedule.add_provider(), self.clone());
            tasks.spawn(async move { Event::Connected(i, client.connect(id).await) });
            providers.push(Provider::new(id));
        }
    }
}

/// Which provider is asked for which block during a fetch. Every provider
/// draws from one queue of missing blocks, up to [`WINDOW`] requests open to
/// each: a faster provider finishes its blocks sooner and so is given more.
///
/// Once the queue is empty (the endgame), a provider with room is also asked
/// for a block whose request to another provider is overdue: open more than
/// [`OVERDUE`] times as long as requests in this fetch typically take. The
/// first to send it wins, so a slow or stalled provider can't hold up the
/// end, while providers of about the same speed aren't asked twice for the
/// same bytes.
///
/// Bookkeeping only: [`Client::fetch_file`] makes the requests, and passes
/// the time in. Providers are numbered from 0.
#[derive(Debug)]
pub struct Schedule {
    missing: VecDeque<u32>,
    /// Open requests: block number → the providers asked for it (one, or
    /// two in the endgame), and when.
    asked: BTreeMap<u32, Vec<(usize, Instant)>>,
    /// Open requests per provider.
    open: Vec<usize>,
    /// How long a delivered request typically took (a moving average), once
    /// one has been.
    typical: Option<Duration>,
}

/// How many times the typical request time a request must be open before
/// its block is asked of a second provider.
pub const OVERDUE: u32 = 2;

impl Schedule {
    pub fn new(missing: impl IntoIterator<Item = u32>, providers: usize) -> Self {
        Self {
            missing: missing.into_iter().collect(),
            asked: BTreeMap::new(),
            open: vec![0; providers],
            typical: None,
        }
    }

    /// Adds a provider, numbered next.
    pub fn add_provider(&mut self) -> usize {
        self.open.push(0);
        self.open.len() - 1
    }

    /// Whether every block has been delivered.
    pub fn is_complete(&self) -> bool {
        self.missing.is_empty() && self.asked.is_empty()
    }

    /// The next block to ask provider `p` for at `now`, if it has room: the
    /// first missing block or, in the endgame, the first block whose only
    /// request, to another provider, is overdue.
    pub fn next(&mut self, p: usize, now: Instant) -> Option<u32> {
        if self.open[p] >= WINDOW {
            return None;
        }
        let index = match self.missing.pop_front() {
            Some(index) => index,
            None => {
                let overdue = self.overdue_after()?;
                *self
                    .asked
                    .iter()
                    .find(|(_, by)| by.len() == 1 && by[0].0 != p && now - by[0].1 > overdue)?
                    .0
            }
        };
        self.asked.entry(index).or_default().push((p, now));
        self.open[p] += 1;
        Some(index)
    }

    /// When, after `now`, the next request becomes overdue, if one will: the
    /// time to look again for endgame requests.
    pub fn next_overdue(&self, now: Instant) -> Option<Instant> {
        let overdue = self.overdue_after()?;
        if !self.missing.is_empty() {
            return None;
        }
        self.asked
            .values()
            .filter(|by| by.len() == 1)
            .map(|by| by[0].1 + overdue)
            .filter(|at| *at > now)
            .min()
    }

    fn overdue_after(&self) -> Option<Duration> {
        self.typical.map(|t| t * OVERDUE)
    }

    /// Whether provider `p` is asked for block `index`: not delivered by
    /// another, nor given back.
    pub fn is_asked(&self, p: usize, index: u32) -> bool {
        self.asked
            .get(&index)
            .is_some_and(|by| by.iter().any(|(q, _)| *q == p))
    }

    /// Provider `p` delivered block `index` at `now`. Returns the other
    /// providers asked for it, whose requests can stop.
    pub fn delivered(&mut self, p: usize, index: u32, now: Instant) -> Vec<usize> {
        let by = self.asked.remove(&index).unwrap_or_default();
        let mut others = Vec::new();
        for (q, at) in by {
            self.open[q] -= 1;
            if q == p {
                let took = now - at;
                self.typical = Some(match self.typical {
                    Some(typical) => (typical * 7 + took) / 8,
                    None => took,
                });
            } else {
                others.push(q);
            }
        }
        others
    }

    /// Provider `p` didn't deliver block `index`: it goes back to the front
    /// of the queue, unless another provider is still asked for it.
    pub fn returned(&mut self, p: usize, index: u32) {
        let Some(by) = self.asked.get_mut(&index) else {
            return;
        };
        let Some(at) = by.iter().position(|(q, _)| *q == p) else {
            return;
        };
        by.remove(at);
        self.open[p] -= 1;
        if by.is_empty() {
            self.asked.remove(&index);
            self.missing.push_front(index);
        }
    }

    /// Gives back every block provider `p` is asked for (see
    /// [`returned`](Self::returned)), and returns them: their requests can
    /// stop.
    pub fn drop_provider(&mut self, p: usize) -> Vec<u32> {
        let blocks: Vec<u32> = self
            .asked
            .iter()
            .filter(|(_, by)| by.iter().any(|(q, _)| *q == p))
            .map(|(&index, _)| index)
            .collect();
        // Last first: each goes to the front, so the queue stays in order.
        for &index in blocks.iter().rev() {
            self.returned(p, index);
        }
        blocks
    }
}

/// One provider during a fetch.
struct Provider {
    id: EndpointId,
    /// Once connected.
    conn: Option<Connection>,
    /// `Busy` answers so far.
    busy: u32,
    /// Waiting after a `Busy` answer.
    paused: bool,
    /// Unreachable, or refused, failed or sent a wrong block: not asked
    /// again for this file.
    dropped: bool,
}

impl Provider {
    fn new(id: EndpointId) -> Self {
        Self {
            id,
            conn: None,
            busy: 0,
            paused: false,
            dropped: false,
        }
    }

    /// Whether it can be given more blocks.
    fn ready(&self) -> bool {
        !self.paused && !self.dropped
    }
}

/// Something that happened during a fetch, for the provider at an index.
enum Event {
    Connected(usize, anyhow::Result<Connection>),
    /// Block number, and what became of the request.
    Block(usize, u32, Got),
    /// A pause after `Busy` is over.
    Resume(usize),
    /// A request may be overdue: time to look for endgame requests.
    Wake,
}

/// Requests one block and checks it against `expected`.
async fn fetch_block(conn: &Connection, request: &BlockRequest, expected: Hash) -> Got {
    let block = match request_block(conn, request).await {
        Ok(Ok(block)) => block,
        Ok(Err(refusal)) => return Got::Refused(refusal),
        Err(err) => return Got::Failed(err),
    };
    let checked = tokio::task::spawn_blocking(move || {
        if Hash::new(&block) == expected {
            Got::Block(block)
        } else {
            Got::Mismatch
        }
    });
    checked
        .await
        .unwrap_or_else(|err| Got::Failed(anyhow::Error::from(err)))
}

/// Copies the `missing` blocks of `meta` that `sources` (files under `root`)
/// hold into `file`, taking them out of `missing`; a block that doesn't
/// hash to the block list is left missing. Returns the bytes copied.
fn copy_local(
    file: &File,
    meta: &FileMeta,
    root: &Path,
    sources: &[Source],
    missing: &mut VecDeque<u32>,
) -> io::Result<u64> {
    if missing.is_empty() || sources.is_empty() {
        return Ok(0);
    }
    // For each wanted hash, the first source holding it and where.
    let mut found: HashMap<Hash, Option<(usize, u32)>> = missing
        .iter()
        .map(|&i| (meta.blocks[i as usize], None))
        .collect();
    for (s, (_, source)) in sources.iter().enumerate() {
        for (i, hash) in source.blocks.iter().enumerate() {
            if let Some(slot @ None) = found.get_mut(hash) {
                *slot = Some((s, i as u32));
            }
        }
    }
    let mut opened: HashMap<usize, Option<File>> = HashMap::new();
    let mut block = Vec::new();
    let mut copied = 0;
    let mut left = VecDeque::new();
    for index in missing.drain(..) {
        let expected = meta.blocks[index as usize];
        let Some((s, at)) = found[&expected] else {
            left.push_back(index);
            continue;
        };
        let (rel, source) = &sources[s];
        let src = opened
            .entry(s)
            .or_insert_with(|| open_regular(root, rel).ok().flatten().map(|(file, _)| file));
        let (to, len) = span(meta, index);
        block.resize(len, 0);
        let read = src
            .as_ref()
            .is_some_and(|src| src.read_exact_at(&mut block, span(source, at).0).is_ok());
        if !read || Hash::new(&block) != expected {
            left.push_back(index);
            continue;
        }
        file.write_all_at(&block, to)?;
        copied += len as u64;
    }
    *missing = left;
    Ok(copied)
}

/// Opens (or creates) the temporary file for `meta`'s content at its full
/// length, and finds which of its blocks already match the block list.
fn open_staged(temp: &Path, meta: &FileMeta) -> io::Result<(File, Vec<bool>)> {
    let mut have = vec![false; meta.blocks.len()];
    match fs::symlink_metadata(temp) {
        Ok(found) if !found.is_file() => {
            return Err(io::Error::other(format!(
                "{} is not a regular file",
                temp.display()
            )))
        }
        _ => {}
    }
    let file = File::options()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(temp)?;
    if file.metadata()?.len() == meta.size {
        let mut block = vec![0; meta.block_size as usize];
        for (i, expected) in meta.blocks.iter().enumerate() {
            let (offset, len) = span(meta, i as u32);
            file.read_exact_at(&mut block[..len], offset)?;
            have[i] = Hash::new(&block[..len]) == *expected;
        }
    } else {
        file.set_len(0)?;
        file.set_len(meta.size)?;
    }
    Ok((file, have))
}
