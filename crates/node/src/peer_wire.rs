//! The peer wire (BitTorrent's peer wire protocol, see
//! `plans/bittorrent/plan.md`): one bidirectional QUIC stream per swarm on a
//! [`ALPN`] connection. The node that wants pieces opens it with a
//! [`Handshake`]; then [`Message`]s both ways: bitfields and `Have`,
//! interest and choking, requests for blocks of pieces, answered with
//! `Piece` or `Reject`. Messages are a little-endian `u32` length and a
//! postcard body of at most [`MAX_HEADER`] bytes; a `Piece` is followed by
//! its bytes.
//!
//! A node serves only pieces of files its own index lists, to members that
//! keep the folder, and only after reading them from disk and checking them
//! against its entry: never a file being edited, never a path outside the
//! folder.
//!
//! The old protocol, one piece per request on its own stream
//! ([`PIECE_REQUEST_ALPN`], [`Client`], [`Schedule`]), still fetches until
//! BitTorrent plan step 4 replaces it.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::os::unix::fs::FileExt;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::Context;
use iroh::endpoint::{Connection, ReadExactError, RecvStream, SendStream, VarInt};
use iroh::protocol::{AcceptError, ProtocolHandler};
use iroh::{Endpoint, EndpointId};
use iroh_blobs::Hash;
use iroh_docs::NamespaceId;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::sync::{watch, Semaphore};
use tokio::task::{AbortHandle, JoinSet};

use crate::folder::{Folders, Serving};
use crate::index::{safe_path, Apply, FileMeta};
use crate::pieces::{read_piece, span, Staged, MAX_PIECE_LENGTH};

/// The old one-piece-per-request protocol's ALPN, still used by
/// [`Client::fetch_file`] (removed in BitTorrent plan step 4). Gated:
/// members only.
pub const PIECE_REQUEST_ALPN: &[u8] = b"/sync-node/blocks/0";

/// Largest message (request or response header) accepted.
pub const MAX_HEADER: u32 = 8 << 10;

/// How long a server waits for a request on a new stream.
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// How long a client waits for a piece.
pub const PIECE_TIMEOUT: Duration = Duration::from_secs(30);

/// Requests a node serves at once; more are refused as `Busy`.
pub const MAX_SERVING: usize = 64;

/// Stream error code for a request the server couldn't read.
pub const BAD_REQUEST: u32 = 400;

/// Requests a client keeps open to one peer.
pub const WINDOW: usize = 8;

/// How long a client waits to connect to a peer.
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// How long a client waits before asking a busy peer again, and how many
/// times it does before moving on.
pub const BUSY_WAIT: Duration = Duration::from_millis(200);
pub const BUSY_TRIES: u32 = 25;

/// One piece of one version of one file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PieceRequest {
    pub folder: NamespaceId,
    /// Relative to the folder root, `/`-separated.
    pub path: String,
    /// The version of the file, by its content hash.
    pub content: Hash,
    /// The piece number.
    pub index: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PieceResponse {
    /// Followed by exactly `len` bytes.
    Data {
        len: u32,
    },
    Refused(Refusal),
}

/// Why a server didn't send a piece.
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

/// Asks the node at the other end of `conn` for one piece. The bytes aren't
/// checked here: the caller compares them with its piece hashes.
pub async fn request_piece(
    conn: &Connection,
    request: &PieceRequest,
) -> anyhow::Result<Result<Vec<u8>, Refusal>> {
    let exchange = async {
        let (mut send, mut recv) = conn.open_bi().await?;
        write_message(&mut send, request).await?;
        send.finish()?;
        match read_message::<PieceResponse>(&mut recv).await? {
            PieceResponse::Data { len } => {
                anyhow::ensure!(len <= MAX_PIECE_LENGTH, "piece of {len} bytes is too large");
                let mut piece = vec![0; len as usize];
                recv.read_exact(&mut piece).await?;
                Ok(Ok(piece))
            }
            PieceResponse::Refused(refusal) => Ok(Err(refusal)),
        }
    };
    tokio::time::timeout(PIECE_TIMEOUT, exchange)
        .await
        .context("timed out waiting for a piece")?
}

/// The serving side of the old one-piece-per-request protocol
/// ([`PIECE_REQUEST_ALPN`]).
#[derive(Debug, Clone)]
pub struct PieceRequestProtocol {
    folders: Folders,
    serving: Arc<Semaphore>,
}

impl PieceRequestProtocol {
    pub(crate) fn new(folders: Folders) -> Self {
        Self {
            folders,
            serving: Arc::new(Semaphore::new(MAX_SERVING)),
        }
    }

    /// Answers one request on its own stream.
    async fn answer(self, remote: EndpointId, mut send: SendStream, mut recv: RecvStream) {
        let request =
            tokio::time::timeout(REQUEST_TIMEOUT, read_message::<PieceRequest>(&mut recv));
        let request = match request.await {
            Ok(Ok(request)) => request,
            failed => {
                let why = match failed {
                    Ok(Err(err)) => format!("{err:#}"),
                    _ => "timed out".to_string(),
                };
                tracing::debug!("{}: bad piece request: {why}", remote.fmt_short());
                let _ = recv.stop(VarInt::from_u32(BAD_REQUEST));
                let _ = send.reset(VarInt::from_u32(BAD_REQUEST));
                return;
            }
        };
        let sent = async {
            match self.serve(remote, &request).await {
                Ok(piece) => {
                    let len = piece.len() as u32;
                    write_message(&mut send, &PieceResponse::Data { len }).await?;
                    send.write_all(&piece).await?;
                }
                Err(refusal) => {
                    tracing::debug!(
                        "{}: refused piece {} of {}: {refusal}",
                        remote.fmt_short(),
                        request.index,
                        request.path
                    );
                    write_message(&mut send, &PieceResponse::Refused(refusal)).await?;
                }
            }
            send.finish()?;
            anyhow::Ok(())
        };
        if let Err(err) = sent.await {
            tracing::debug!("{}: sending a piece: {err:#}", remote.fmt_short());
        }
    }

    /// The piece `request` asks for, read from disk and checked against this
    /// node's entry, or why not.
    async fn serve(&self, remote: EndpointId, request: &PieceRequest) -> Result<Vec<u8>, Refusal> {
        let folder = self.folders.serving(request.folder, remote)?;
        if !safe_path(&request.path) {
            return Err(Refusal::NoSuchFile);
        }
        let entry = folder
            .entries
            .get(&request.path)
            .filter(|e| e.content == Some(request.content))
            .ok_or(Refusal::NoSuchFile)?;
        if request.index as usize >= entry.pieces.len() {
            return Err(Refusal::NoSuchFile);
        }
        let _permit = self
            .serving
            .clone()
            .try_acquire_owned()
            .map_err(|_| Refusal::Busy)?;
        let (root, path, index) = (folder.root.clone(), request.path.clone(), request.index);
        let read =
            tokio::task::spawn_blocking(move || read_piece(&root, &path, &entry, index)).await;
        match read {
            Ok(Ok(Some(piece))) => Ok(piece),
            other => {
                if let Ok(Err(err)) = &other {
                    tracing::debug!("reading piece {index} of {}: {err}", request.path);
                }
                rehash(&folder, &request.path);
                Err(Refusal::Changed)
            }
        }
    }
}

impl ProtocolHandler for PieceRequestProtocol {
    async fn accept(&self, conn: Connection) -> Result<(), AcceptError> {
        let remote = conn.remote_id();
        while let Ok((send, recv)) = conn.accept_bi().await {
            tokio::spawn(self.clone().answer(remote, send, recv));
        }
        Ok(())
    }
}

/// The peer wire's ALPN. Gated: members only.
pub const ALPN: &[u8] = b"/sync-node/peer-wire/0";

/// The block length a leecher asks for: 16 KiB, as BitTorrent. The last
/// block of a piece may be shorter.
pub const BLOCK_LENGTH: u32 = 16 << 10;

/// The longest block a node serves; a longer request is rejected.
pub const MAX_BLOCK_LENGTH: u32 = 128 << 10;

/// The first message on a swarm's stream, from the node that opened it: the
/// swarm is version `content` of `path` in `folder` (BitTorrent's info
/// hash).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Handshake {
    pub folder: NamespaceId,
    /// Relative to the folder root, `/`-separated.
    pub path: String,
    pub content: Hash,
}

/// A block: `length` bytes from `begin` in piece `piece`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Block {
    pub piece: u32,
    pub begin: u32,
    pub length: u32,
}

/// A peer-wire message. After the [`Handshake`], the other end answers
/// with `Refused` (and closes the stream) or with its bitfield (`HaveAll`,
/// `HaveNone` or `Bitfield`); the opener sends its bitfield too. Anything
/// after that, both ways.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Message {
    /// The answer to a handshake for a swarm the other end won't join.
    Refused(Refusal),
    /// Which pieces the sender has: one bit per piece, the first piece in
    /// the high bit of the first byte.
    Bitfield(Vec<u8>),
    /// The sender has every piece: a seeder (BEP 6).
    HaveAll,
    HaveNone,
    /// The sender has verified this piece.
    Have(u32),
    Interested,
    NotInterested,
    /// The sender won't serve requests until `Unchoke`.
    Choke,
    Unchoke,
    Request(Block),
    /// Followed by exactly the block's `length` bytes.
    Piece(Block),
    /// A request the sender no longer wants; it is still answered.
    Cancel(Block),
    /// A request the sender won't serve (BEP 6): every request gets a
    /// `Piece` or a `Reject`.
    Reject(Block, Why),
}

/// Why a request was rejected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
pub enum Why {
    #[error("choked")]
    Choked,
    /// The file no longer matches the swarm's version.
    #[error("the file changed")]
    Changed,
    #[error("the piece isn't there")]
    NotHave,
    /// Past the end of the piece, empty, or longer than
    /// [`MAX_BLOCK_LENGTH`].
    #[error("not a block of this file")]
    Invalid,
    #[error("refused by the relay policy")]
    Policy,
}

/// One swarm's stream, at either end: framed messages (see
/// [`write_message`]), each `Piece` followed by its bytes.
#[derive(Debug)]
pub struct Wire {
    send: SendStream,
    recv: RecvStream,
}

impl Wire {
    /// Opens a stream on `conn` for the swarm `handshake` names, sends the
    /// handshake and returns the other end's answer: `Refused`, or its
    /// bitfield.
    pub async fn open(conn: &Connection, handshake: &Handshake) -> anyhow::Result<(Self, Message)> {
        let (mut send, recv) = conn.open_bi().await?;
        write_message(&mut send, handshake).await?;
        let mut wire = Self { send, recv };
        let answer = tokio::time::timeout(REQUEST_TIMEOUT, wire.recv())
            .await
            .context("timed out waiting for the handshake's answer")??
            .context("closed before answering the handshake")?;
        Ok((wire, answer.0))
    }

    pub async fn send(&mut self, msg: &Message) -> anyhow::Result<()> {
        write_message(&mut self.send, msg).await
    }

    /// Sends a `Piece` message for `block` and its bytes, `data`.
    pub async fn send_piece(&mut self, block: Block, data: &[u8]) -> anyhow::Result<()> {
        debug_assert_eq!(data.len(), block.length as usize);
        write_message(&mut self.send, &Message::Piece(block)).await?;
        self.send.write_all(data).await?;
        Ok(())
    }

    /// The next message, with a `Piece`'s bytes (empty for any other), or
    /// `None` if the other end finished the stream.
    pub async fn recv(&mut self) -> anyhow::Result<Option<(Message, Vec<u8>)>> {
        let mut len = [0; 4];
        match self.recv.read_exact(&mut len).await {
            Ok(()) => {}
            Err(ReadExactError::FinishedEarly(0)) => return Ok(None),
            Err(err) => return Err(err.into()),
        }
        let len = u32::from_le_bytes(len);
        anyhow::ensure!(len <= MAX_HEADER, "message of {len} bytes is too large");
        let mut body = vec![0; len as usize];
        self.recv.read_exact(&mut body).await?;
        let msg: Message = postcard::from_bytes(&body)?;
        let mut data = Vec::new();
        if let Message::Piece(block) = &msg {
            anyhow::ensure!(
                block.length <= MAX_BLOCK_LENGTH,
                "block of {} bytes is too large",
                block.length
            );
            data.resize(block.length as usize, 0);
            self.recv.read_exact(&mut data).await?;
        }
        Ok(Some((msg, data)))
    }

    /// Finishes this end of the stream.
    pub fn finish(&mut self) -> anyhow::Result<()> {
        Ok(self.send.finish()?)
    }

    /// Closes the stream both ways with `BAD_REQUEST`, for a message that
    /// breaks the protocol.
    fn reset(&mut self) {
        let _ = self.recv.stop(VarInt::from_u32(BAD_REQUEST));
        let _ = self.send.reset(VarInt::from_u32(BAD_REQUEST));
    }
}

/// The serving side of the peer wire: one task per swarm stream.
///
/// A seeder (this node's entry has the version) answers `HaveAll`, unchokes
/// a peer once it is interested (no choker yet: BitTorrent plan step 6), and
/// answers every request in order, with `Piece` or `Reject`. A piece is
/// read from disk and checked against its hash before its first block is
/// sent, and kept for the blocks after it.
#[derive(Debug, Clone)]
pub struct PeerWireProtocol {
    folders: Folders,
}

/// The last piece verified on one stream, for its next blocks.
struct Verified {
    piece: u32,
    data: Vec<u8>,
}

impl PeerWireProtocol {
    pub(crate) fn new(folders: Folders) -> Self {
        Self { folders }
    }

    /// Runs one swarm stream until the other end finishes it.
    async fn swarm(self, remote: EndpointId, send: SendStream, recv: RecvStream) {
        let mut wire = Wire { send, recv };
        let handshake =
            tokio::time::timeout(REQUEST_TIMEOUT, read_message::<Handshake>(&mut wire.recv));
        let handshake = match handshake.await {
            Ok(Ok(handshake)) => handshake,
            failed => {
                let why = match failed {
                    Ok(Err(err)) => format!("{err:#}"),
                    _ => "timed out".to_string(),
                };
                tracing::debug!("{}: bad handshake: {why}", remote.fmt_short());
                wire.reset();
                return;
            }
        };
        if let Err(err) = self.serve(remote, &handshake, &mut wire).await {
            tracing::debug!(
                "{}: swarm for {}: {err:#}",
                remote.fmt_short(),
                handshake.path
            );
            wire.reset();
        }
    }

    async fn serve(
        &self,
        remote: EndpointId,
        handshake: &Handshake,
        wire: &mut Wire,
    ) -> anyhow::Result<()> {
        let folder = match self.join(remote, handshake) {
            Ok(folder) => folder,
            Err(refusal) => {
                wire.send(&Message::Refused(refusal)).await?;
                wire.finish()?;
                return Ok(());
            }
        };
        wire.send(&Message::HaveAll).await?;
        let mut choked = true;
        let mut verified: Option<Verified> = None;
        while let Some((msg, _)) = wire.recv().await? {
            match msg {
                Message::Interested if choked => {
                    choked = false;
                    wire.send(&Message::Unchoke).await?;
                }
                Message::Request(block) => {
                    let answer = self
                        .block(&folder, handshake, block, choked, &mut verified)
                        .await;
                    match answer {
                        Ok(data) => wire.send_piece(block, data).await?,
                        Err(why) => {
                            tracing::debug!(
                                "{}: rejected {block:?} of {}: {why}",
                                remote.fmt_short(),
                                handshake.path
                            );
                            wire.send(&Message::Reject(block, why)).await?;
                        }
                    }
                }
                // A seeder downloads nothing: the peer's pieces and choking
                // don't matter. Requests are answered in order, so a cancel
                // always comes too late.
                Message::Interested
                | Message::NotInterested
                | Message::Bitfield(_)
                | Message::HaveAll
                | Message::HaveNone
                | Message::Have(_)
                | Message::Choke
                | Message::Unchoke
                | Message::Cancel(_)
                | Message::Reject(..) => {}
                Message::Refused(_) | Message::Piece(_) => {
                    anyhow::bail!("unexpected {msg:?}")
                }
            }
        }
        wire.finish()?;
        Ok(())
    }

    /// The folder to serve the swarm `handshake` names from, if this node
    /// seeds it to `remote`.
    fn join(&self, remote: EndpointId, handshake: &Handshake) -> Result<Serving, Refusal> {
        let folder = self.folders.serving(handshake.folder, remote)?;
        if !safe_path(&handshake.path) {
            return Err(Refusal::NoSuchFile);
        }
        folder
            .entries
            .get(&handshake.path)
            .filter(|e| e.content == Some(handshake.content))
            .ok_or(Refusal::NoSuchFile)?;
        Ok(folder)
    }

    /// The bytes of `block`, from the verified piece if it is that one's,
    /// else read from disk and checked; or why not.
    async fn block<'a>(
        &self,
        folder: &Serving,
        handshake: &Handshake,
        block: Block,
        choked: bool,
        verified: &'a mut Option<Verified>,
    ) -> Result<&'a [u8], Why> {
        // The entry now: the file may have changed since the handshake.
        let entry = folder
            .entries
            .get(&handshake.path)
            .filter(|e| e.content == Some(handshake.content))
            .ok_or(Why::Changed)?;
        if block.piece as usize >= entry.pieces.len() {
            return Err(Why::Invalid);
        }
        let end = u64::from(block.begin) + u64::from(block.length);
        if block.length == 0
            || block.length > MAX_BLOCK_LENGTH
            || end > span(&entry, block.piece).1 as u64
        {
            return Err(Why::Invalid);
        }
        if choked {
            return Err(Why::Choked);
        }
        if verified.as_ref().is_none_or(|v| v.piece != block.piece) {
            *verified = None;
            let (root, path, piece) = (folder.root.clone(), handshake.path.clone(), block.piece);
            let read =
                tokio::task::spawn_blocking(move || read_piece(&root, &path, &entry, piece)).await;
            match read {
                Ok(Ok(Some(data))) => *verified = Some(Verified { piece, data }),
                other => {
                    if let Ok(Err(err)) = &other {
                        tracing::debug!("reading piece {piece} of {}: {err}", handshake.path);
                    }
                    rehash(folder, &handshake.path);
                    return Err(Why::Changed);
                }
            }
        }
        let data = &verified.as_ref().expect("verified above").data;
        Ok(&data[block.begin as usize..end as usize])
    }
}

impl ProtocolHandler for PeerWireProtocol {
    async fn accept(&self, conn: Connection) -> Result<(), AcceptError> {
        let remote = conn.remote_id();
        while let Ok((send, recv)) = conn.accept_bi().await {
            tokio::spawn(self.clone().swarm(remote, send, recv));
        }
        Ok(())
    }
}

/// The file at `path` no longer matches its entry: has the index look at
/// it, in case the watcher hasn't (or can't: same size and time). Dropped if
/// the index task is busy; it's a nudge.
fn rehash(folder: &Serving, path: &str) {
    if let Some(apply) = &folder.apply {
        let _ = apply.try_send(Apply::Rehash {
            path: path.to_string(),
        });
    }
}

/// The fetching side of the peer wire: one connection per peer,
/// kept while it stays open.
#[derive(Debug, Clone)]
pub struct Client {
    endpoint: Endpoint,
    conns: Arc<Mutex<HashMap<EndpointId, Connection>>>,
    /// Bytes of verified pieces written, per peer.
    fetched: Arc<Mutex<HashMap<EndpointId, u64>>>,
}

/// What became of one piece request.
enum Got {
    /// The piece, checked against the piece hashes.
    Piece(Vec<u8>),
    Refused(Refusal),
    /// The bytes don't match the piece's hash: the peer is wrong.
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

    /// Bytes of file data fetched from each peer since this node
    /// started: verified pieces written, each counted once, for the
    /// peer that sent it first.
    pub fn fetched(&self) -> HashMap<EndpointId, u64> {
        self.fetched.lock().unwrap().clone()
    }

    /// A connection to `id`, reusing an open one.
    async fn connect(&self, id: EndpointId) -> anyhow::Result<Connection> {
        let open = self.conns.lock().unwrap().get(&id).cloned();
        if let Some(conn) = open.filter(|c| c.close_reason().is_none()) {
            return Ok(conn);
        }
        let conn = tokio::time::timeout(
            CONNECT_TIMEOUT,
            self.endpoint.connect(id, PIECE_REQUEST_ALPN),
        )
        .await
        .context("timed out connecting")??;
        self.conns.lock().unwrap().insert(id, conn.clone());
        Ok(conn)
    }

    /// Fetches the pieces `staged` lacks for version `meta` of `path` in
    /// `folder` from all the peers `offered` at once, as [`Schedule`]
    /// hands them out. The offer may grow during the fetch (a node finished
    /// its own copy): new peers join it. A peer that can't be reached, refuses, fails or
    /// sends a piece that doesn't match is dropped for this file, and its
    /// pieces go to the others. Every piece is checked against the piece hashes before it is
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
        let request = PieceRequest {
            folder,
            path: path.to_string(),
            content,
            index: 0,
        };
        let mut tasks = JoinSet::new();
        let mut schedule = Schedule::new(staged.missing.drain(..), 0);
        let mut peers = Vec::new();
        let ids = offered.borrow_and_update().clone();
        self.join(&ids, &mut peers, &mut schedule, &mut tasks);
        // Until the offer's sender is gone.
        let mut watching = true;
        // Open requests' tasks, by peer and piece number.
        let mut requests: HashMap<(usize, u32), AbortHandle> = HashMap::new();
        // Whether an `Event::Wake` is pending.
        let mut waking = false;
        while !schedule.is_complete() {
            for (i, p) in peers.iter().enumerate() {
                let Some(conn) = p.conn.as_ref().filter(|_| p.ready()) else {
                    continue;
                };
                while let Some(index) = schedule.next(i, Instant::now()) {
                    let conn = conn.clone();
                    let request = PieceRequest {
                        index,
                        ..request.clone()
                    };
                    let expected = meta.pieces[index as usize];
                    let task = tasks.spawn(async move {
                        Event::Piece(i, index, fetch_piece(&conn, &request, expected).await)
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
                            self.join(&ids, &mut peers, &mut schedule, &mut tasks);
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
                    peers[i].conn = Some(conn);
                    continue;
                }
                Event::Connected(i, Err(err)) => {
                    let p = &mut peers[i];
                    tracing::debug!("{}: connecting: {err:#}", p.id.fmt_short());
                    p.dropped = true;
                    continue;
                }
                Event::Resume(i) => {
                    peers[i].paused = false;
                    continue;
                }
                Event::Wake => {
                    waking = false;
                    continue;
                }
                Event::Piece(i, index, got) => (i, index, got),
            };
            requests.remove(&(i, index));
            if !schedule.is_asked(i, index) {
                // Stopped, but it finished first: another peer sent the
                // piece, or this one was dropped.
                continue;
            }
            let p = &mut peers[i];
            let why = match got {
                Got::Piece(piece) => {
                    // Written here, not in the request's task, so a stopped
                    // request never writes.
                    let (file, offset) = (staged.file.clone(), span(meta, index).0);
                    let len = piece.len() as u64;
                    tokio::task::spawn_blocking(move || file.write_all_at(&piece, offset))
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
                Got::Refused(refusal) => anyhow::anyhow!("refused piece {index}: {refusal}"),
                Got::Mismatch => anyhow::anyhow!("piece {index} doesn't match its hash"),
                Got::Failed(err) => err.context(format!("piece {index}")),
            };
            tracing::debug!("{}: fetching {path}: {why:#}", p.id.fmt_short());
            p.dropped = true;
            if p.conn.as_ref().is_some_and(|c| c.close_reason().is_some()) {
                self.conns.lock().unwrap().remove(&p.id);
            }
            schedule.returned(i, index);
            for index in schedule.drop_peer(i) {
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

    /// Adds the peers among `ids` not in `peers` yet, and starts
    /// connecting to them.
    fn join(
        &self,
        ids: &[EndpointId],
        peers: &mut Vec<Peer>,
        schedule: &mut Schedule,
        tasks: &mut JoinSet<Event>,
    ) {
        for &id in ids {
            if peers.iter().any(|p| p.id == id) {
                continue;
            }
            if !peers.is_empty() {
                tracing::debug!("{}: joins the fetch", id.fmt_short());
            }
            let (i, client) = (schedule.add_peer(), self.clone());
            tasks.spawn(async move { Event::Connected(i, client.connect(id).await) });
            peers.push(Peer::new(id));
        }
    }
}

/// Which peer is asked for which piece during a fetch. Every peer
/// draws from one queue of missing pieces, up to [`WINDOW`] requests open to
/// each: a faster peer finishes its pieces sooner and so is given more.
///
/// Once the queue is empty (the endgame), a peer with room is also asked
/// for a piece whose request to another peer is overdue: open more than
/// [`OVERDUE`] times as long as requests in this fetch typically take. The
/// first to send it wins, so a slow or stalled peer can't hold up the
/// end, while peers of about the same speed aren't asked twice for the
/// same bytes.
///
/// Bookkeeping only: [`Client::fetch_file`] makes the requests, and passes
/// the time in. Peers are numbered from 0.
#[derive(Debug)]
pub struct Schedule {
    missing: VecDeque<u32>,
    /// Open requests: piece number → the peers asked for it (one, or
    /// two in the endgame), and when.
    asked: BTreeMap<u32, Vec<(usize, Instant)>>,
    /// Open requests per peer.
    open: Vec<usize>,
    /// How long a delivered request typically took (a moving average), once
    /// one has been.
    typical: Option<Duration>,
}

/// How many times the typical request time a request must be open before
/// its piece is asked of a second peer.
pub const OVERDUE: u32 = 2;

impl Schedule {
    pub fn new(missing: impl IntoIterator<Item = u32>, peers: usize) -> Self {
        Self {
            missing: missing.into_iter().collect(),
            asked: BTreeMap::new(),
            open: vec![0; peers],
            typical: None,
        }
    }

    /// Adds a peer, numbered next.
    pub fn add_peer(&mut self) -> usize {
        self.open.push(0);
        self.open.len() - 1
    }

    /// Whether every piece has been delivered.
    pub fn is_complete(&self) -> bool {
        self.missing.is_empty() && self.asked.is_empty()
    }

    /// The next piece to ask peer `p` for at `now`, if it has room: the
    /// first missing piece or, in the endgame, the first piece whose only
    /// request, to another peer, is overdue.
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

    /// Whether peer `p` is asked for piece `index`: not delivered by
    /// another, nor given back.
    pub fn is_asked(&self, p: usize, index: u32) -> bool {
        self.asked
            .get(&index)
            .is_some_and(|by| by.iter().any(|(q, _)| *q == p))
    }

    /// Peer `p` delivered piece `index` at `now`. Returns the other
    /// peers asked for it, whose requests can stop.
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

    /// Peer `p` didn't deliver piece `index`: it goes back to the front
    /// of the queue, unless another peer is still asked for it.
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

    /// Gives back every piece peer `p` is asked for (see
    /// [`returned`](Self::returned)), and returns them: their requests can
    /// stop.
    pub fn drop_peer(&mut self, p: usize) -> Vec<u32> {
        let pieces: Vec<u32> = self
            .asked
            .iter()
            .filter(|(_, by)| by.iter().any(|(q, _)| *q == p))
            .map(|(&index, _)| index)
            .collect();
        // Last first: each goes to the front, so the queue stays in order.
        for &index in pieces.iter().rev() {
            self.returned(p, index);
        }
        pieces
    }
}

/// One peer during a fetch.
struct Peer {
    id: EndpointId,
    /// Once connected.
    conn: Option<Connection>,
    /// `Busy` answers so far.
    busy: u32,
    /// Waiting after a `Busy` answer.
    paused: bool,
    /// Unreachable, or refused, failed or sent a wrong piece: not asked
    /// again for this file.
    dropped: bool,
}

impl Peer {
    fn new(id: EndpointId) -> Self {
        Self {
            id,
            conn: None,
            busy: 0,
            paused: false,
            dropped: false,
        }
    }

    /// Whether it can be given more pieces.
    fn ready(&self) -> bool {
        !self.paused && !self.dropped
    }
}

/// Something that happened during a fetch, for the peer at an index.
enum Event {
    Connected(usize, anyhow::Result<Connection>),
    /// Piece number, and what became of the request.
    Piece(usize, u32, Got),
    /// A pause after `Busy` is over.
    Resume(usize),
    /// A request may be overdue: time to look for endgame requests.
    Wake,
}

/// Requests one piece and checks it against `expected`.
async fn fetch_piece(conn: &Connection, request: &PieceRequest, expected: Hash) -> Got {
    let piece = match request_piece(conn, request).await {
        Ok(Ok(piece)) => piece,
        Ok(Err(refusal)) => return Got::Refused(refusal),
        Err(err) => return Got::Failed(err),
    };
    let checked = tokio::task::spawn_blocking(move || {
        if Hash::new(&piece) == expected {
            Got::Piece(piece)
        } else {
            Got::Mismatch
        }
    });
    checked
        .await
        .unwrap_or_else(|err| Got::Failed(anyhow::Error::from(err)))
}
