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

use std::collections::HashMap;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::Context;
use iroh::endpoint::{
    Connection, ReadExactError, RecvStream, SendStream, VarInt, WeakConnectionHandle,
};
use iroh::protocol::{AcceptError, ProtocolHandler};
use iroh::{Endpoint, EndpointId};
use iroh_blobs::Hash;
use iroh_docs::NamespaceId;
use n0_future::task::AbortOnDropHandle;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;

use crate::choker::{queue_length, Choker, Rate, ROTATE_EVERY, ROUND, SNUB_AFTER};
use crate::folder::{Folders, Serving};
use crate::index::{safe_path, Apply, FileMeta};
use crate::picker::{Complete, Holds, Picker};
use crate::pieces::{read_piece, read_staged, span, Staged};

/// Largest message (any but a `Piece`'s bytes) accepted.
pub const MAX_HEADER: u32 = 8 << 10;

/// How long a node waits for the handshake on a new stream, and for its
/// answer.
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// Stream error code for a message that breaks the protocol.
pub const BAD_REQUEST: u32 = 400;

/// How long a node waits to connect to a peer.
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Why a node won't join a swarm.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
pub enum Refusal {
    #[error("doesn't keep this folder")]
    NotKept,
    #[error("no such file or version")]
    NoSuchFile,
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
    send: WireSend,
    recv: WireRecv,
}

/// The sending half of a [`Wire`].
#[derive(Debug)]
pub struct WireSend(SendStream);

/// The receiving half of a [`Wire`].
#[derive(Debug)]
pub struct WireRecv(RecvStream);

impl Wire {
    /// Opens a stream on `conn` for the swarm `handshake` names, sends the
    /// handshake and returns the other end's answer: `Refused`, or its
    /// bitfield.
    pub async fn open(conn: &Connection, handshake: &Handshake) -> anyhow::Result<(Self, Message)> {
        let (mut send, recv) = conn.open_bi().await?;
        write_message(&mut send, handshake).await?;
        let mut wire = Self {
            send: WireSend(send),
            recv: WireRecv(recv),
        };
        let answer = tokio::time::timeout(REQUEST_TIMEOUT, wire.recv())
            .await
            .context("timed out waiting for the handshake's answer")??
            .context("closed before answering the handshake")?;
        Ok((wire, answer.0))
    }

    pub async fn send(&mut self, msg: &Message) -> anyhow::Result<()> {
        self.send.send(msg).await
    }

    /// Sends a `Piece` message for `block` and its bytes, `data`.
    pub async fn send_piece(&mut self, block: Block, data: &[u8]) -> anyhow::Result<()> {
        self.send.send_piece(block, data).await
    }

    /// The next message, with a `Piece`'s bytes (empty for any other), or
    /// `None` if the other end finished the stream.
    pub async fn recv(&mut self) -> anyhow::Result<Option<(Message, Vec<u8>)>> {
        self.recv.recv().await
    }

    /// Finishes this end of the stream.
    pub fn finish(&mut self) -> anyhow::Result<()> {
        self.send.finish()
    }

    /// The halves, to send and receive from separate tasks.
    pub fn split(self) -> (WireSend, WireRecv) {
        (self.send, self.recv)
    }

    /// Closes the stream both ways with `BAD_REQUEST`, for a message that
    /// breaks the protocol.
    fn reset(&mut self) {
        let _ = self.recv.0.stop(VarInt::from_u32(BAD_REQUEST));
        self.send.reset();
    }
}

impl WireSend {
    pub async fn send(&mut self, msg: &Message) -> anyhow::Result<()> {
        write_message(&mut self.0, msg).await
    }

    /// Sends a `Piece` message for `block` and its bytes, `data`.
    pub async fn send_piece(&mut self, block: Block, data: &[u8]) -> anyhow::Result<()> {
        debug_assert_eq!(data.len(), block.length as usize);
        write_message(&mut self.0, &Message::Piece(block)).await?;
        self.0.write_all(data).await?;
        Ok(())
    }

    pub fn finish(&mut self) -> anyhow::Result<()> {
        Ok(self.0.finish()?)
    }

    /// Stops sending at once, with `BAD_REQUEST`.
    pub fn reset(&mut self) {
        let _ = self.0.reset(VarInt::from_u32(BAD_REQUEST));
    }
}

impl WireRecv {
    /// The next message, with a `Piece`'s bytes (empty for any other), or
    /// `None` if the other end finished the stream.
    pub async fn recv(&mut self) -> anyhow::Result<Option<(Message, Vec<u8>)>> {
        let mut len = [0; 4];
        match self.0.read_exact(&mut len).await {
            Ok(()) => {}
            Err(ReadExactError::FinishedEarly(0)) => return Ok(None),
            Err(err) => return Err(err.into()),
        }
        let len = u32::from_le_bytes(len);
        anyhow::ensure!(len <= MAX_HEADER, "message of {len} bytes is too large");
        let mut body = vec![0; len as usize];
        self.0.read_exact(&mut body).await?;
        let msg: Message = postcard::from_bytes(&body)?;
        let mut data = Vec::new();
        if let Message::Piece(block) = &msg {
            anyhow::ensure!(
                block.length <= MAX_BLOCK_LENGTH,
                "block of {} bytes is too large",
                block.length
            );
            data.resize(block.length as usize, 0);
            self.0.read_exact(&mut data).await?;
        }
        Ok(Some((msg, data)))
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
    /// This node's running downloads, to serve as a leecher.
    swarms: Swarms,
    uploads: Uploads,
    /// The choker's rounds, stopped with the last clone.
    _rounds: Arc<AbortOnDropHandle<()>>,
    /// `--direct-only`: requests that arrive over a relay are rejected
    /// with `Policy`.
    direct_only: bool,
}

/// The last piece verified on one stream, for its next blocks.
struct Verified {
    piece: u32,
    data: Vec<u8>,
}

impl PeerWireProtocol {
    pub(crate) fn new(folders: Folders, direct_only: bool) -> Self {
        let uploads = Uploads::default();
        Self {
            swarms: folders.swarms(),
            _rounds: Arc::new(uploads.run()),
            uploads,
            folders,
            direct_only,
        }
    }

    /// Runs one swarm stream on `conn` until the other end finishes it.
    /// Holds `conn` weakly: a strong handle would keep the connection open
    /// while the stream waits.
    async fn swarm(
        self,
        remote: EndpointId,
        conn: WeakConnectionHandle,
        send: SendStream,
        recv: RecvStream,
    ) {
        let mut wire = Wire {
            send: WireSend(send),
            recv: WireRecv(recv),
        };
        let handshake =
            tokio::time::timeout(REQUEST_TIMEOUT, read_message::<Handshake>(&mut wire.recv.0));
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
        let (mut send, recv) = wire.split();
        if let Err(err) = self.serve(remote, &conn, &handshake, &mut send, recv).await {
            tracing::debug!(
                "{}: swarm for {}: {err:#}",
                remote.fmt_short(),
                handshake.path
            );
            send.reset();
        }
    }

    async fn serve(
        &self,
        remote: EndpointId,
        conn: &WeakConnectionHandle,
        handshake: &Handshake,
        send: &mut WireSend,
        recv: WireRecv,
    ) -> anyhow::Result<()> {
        let (folder, leeching) = match self.join(remote, handshake) {
            Ok(joined) => joined,
            Err(refusal) => {
                send.send(&Message::Refused(refusal)).await?;
                send.finish()?;
                return Ok(());
            }
        };
        // A leecher sends its bitfield, then `Have` for each piece its
        // download verifies, as long as the download runs.
        let mut haves = leeching.as_ref().map(|l| l.have.subscribe());
        let mut sent = match &mut haves {
            Some(haves) => {
                let have = haves.borrow_and_update().clone();
                send.send(&bitfield_message(&have)).await?;
                have
            }
            None => {
                send.send(&Message::HaveAll).await?;
                Vec::new()
            }
        };
        // Messages, read on their own task so `Have` can go out meanwhile.
        let (messages, mut incoming) = mpsc::channel(16);
        let _reader = AbortOnDropHandle::new(tokio::spawn(read_all(recv, messages)));
        // The stream is unchoked while it is interested and the choker has
        // its node unchoked (for this or another of its swarms).
        let (mut upload, mut unchoked) = self.uploads.join(remote);
        let mut choked = true;
        let mut verified: Option<Verified> = None;
        loop {
            let want = upload.interested && *unchoked.borrow_and_update();
            if want == choked {
                choked = !want;
                let say = if want {
                    Message::Unchoke
                } else {
                    Message::Choke
                };
                send.send(&say).await?;
            }
            let msg = tokio::select! {
                changed = unchoked.changed() => {
                    changed?;
                    continue;
                }
                msg = incoming.recv() => match msg {
                    Some(msg) => msg?.0,
                    None => break,
                },
                changed = changed(&mut haves) => {
                    match changed {
                        Some(have) => {
                            for (i, now) in have.iter().enumerate() {
                                if *now && !sent[i] {
                                    sent[i] = true;
                                    send.send(&Message::Have(i as u32)).await?;
                                }
                            }
                        }
                        // The download ended: placed, or given up.
                        None => haves = None,
                    }
                    continue;
                }
            };
            match msg {
                Message::Interested => upload.interested(true),
                Message::NotInterested => upload.interested(false),
                Message::Request(block) => {
                    let relayed = self.direct_only
                        && !conn.upgrade().is_some_and(|c| crate::peers::is_direct(&c));
                    let answer = self
                        .block(&folder, handshake, block, choked, relayed, &mut verified)
                        .await;
                    match answer {
                        Ok(data) => {
                            send.send_piece(block, data).await?;
                            upload.sent(block.length);
                        }
                        Err(why) => {
                            tracing::debug!(
                                "{}: rejected {block:?} of {}: {why}",
                                remote.fmt_short(),
                                handshake.path
                            );
                            send.send(&Message::Reject(block, why)).await?;
                        }
                    }
                }
                // The other end's own pieces and choking matter to its
                // server, on the stream it opened, not here. Requests are
                // answered in order, so a cancel always comes too late.
                Message::Bitfield(_)
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
        send.finish()?;
        Ok(())
    }

    /// The folder to serve the swarm `handshake` names from, if this node
    /// is in it for `remote`: a seeder (its entry has the version), or a
    /// leecher (it is downloading it).
    fn join(
        &self,
        remote: EndpointId,
        handshake: &Handshake,
    ) -> Result<(Serving, Option<Arc<Leeching>>), Refusal> {
        let folder = self.folders.serving(handshake.folder, remote)?;
        if !safe_path(&handshake.path) {
            return Err(Refusal::NoSuchFile);
        }
        let seeding = folder
            .entries
            .get(&handshake.path)
            .is_some_and(|e| e.content == Some(handshake.content));
        if seeding {
            return Ok((folder, None));
        }
        let leeching = self.swarms.get(handshake).ok_or(Refusal::NoSuchFile)?;
        Ok((folder, Some(leeching)))
    }

    /// The bytes of `block`, from the verified piece if it is that one's,
    /// else read from disk (the file if this node seeds the version now,
    /// else its staging file) and checked; or why not.
    async fn block<'a>(
        &self,
        folder: &Serving,
        handshake: &Handshake,
        block: Block,
        choked: bool,
        relayed: bool,
        verified: &'a mut Option<Verified>,
    ) -> Result<&'a [u8], Why> {
        // Where the version is now: a leecher's download may have finished
        // since the handshake, a seeder's file changed.
        let entry = folder
            .entries
            .get(&handshake.path)
            .filter(|e| e.content == Some(handshake.content));
        let leeching = match entry {
            Some(_) => None,
            None => Some(self.swarms.get(handshake).ok_or(Why::NotHave)?),
        };
        let meta: &FileMeta = match (&entry, &leeching) {
            (Some(entry), _) => entry,
            (None, Some(leeching)) => &leeching.meta,
            (None, None) => unreachable!("one of the two"),
        };
        if block.piece as usize >= meta.pieces.len() {
            return Err(Why::Invalid);
        }
        let end = u64::from(block.begin) + u64::from(block.length);
        if block.length == 0
            || block.length > MAX_BLOCK_LENGTH
            || end > span(meta, block.piece).1 as u64
        {
            return Err(Why::Invalid);
        }
        if let Some(leeching) = &leeching {
            if !leeching.have.borrow()[block.piece as usize] {
                return Err(Why::NotHave);
            }
        }
        if relayed {
            return Err(Why::Policy);
        }
        if choked {
            return Err(Why::Choked);
        }
        if verified.as_ref().is_none_or(|v| v.piece != block.piece) {
            *verified = None;
            let piece = block.piece;
            let read = match (entry, &leeching) {
                (Some(entry), _) => {
                    let (root, path) = (folder.root.clone(), handshake.path.clone());
                    tokio::task::spawn_blocking(move || read_piece(&root, &path, &entry, piece))
                        .await
                }
                (None, Some(leeching)) => {
                    let leeching = leeching.clone();
                    tokio::task::spawn_blocking(move || {
                        read_staged(&leeching.file, &leeching.meta, piece)
                    })
                    .await
                }
                (None, None) => unreachable!("one of the two"),
            };
            match read {
                Ok(Ok(Some(data))) => *verified = Some(Verified { piece, data }),
                other => {
                    if let Ok(Err(err)) = &other {
                        tracing::debug!("reading piece {piece} of {}: {err}", handshake.path);
                    }
                    let Some(leeching) = leeching else {
                        rehash(folder, &handshake.path);
                        return Err(Why::Changed);
                    };
                    // The staging file no longer has it: the download
                    // fetches it again.
                    leeching
                        .have
                        .send_modify(|have| have[piece as usize] = false);
                    let _ = leeching.lost.send(piece);
                    return Err(Why::NotHave);
                }
            }
        }
        let data = &verified.as_ref().expect("verified above").data;
        Ok(&data[block.begin as usize..end as usize])
    }
}

/// This node's uploads, per peer node over all its swarms: what the
/// [`Choker`] decides on, every [`ROUND`] and whenever a peer's interest
/// changes.
#[derive(Debug, Clone, Default)]
struct Uploads(Arc<Mutex<UploadState>>);

#[derive(Debug, Default)]
struct UploadState {
    peers: HashMap<EndpointId, Uploading>,
    choker: Choker<EndpointId>,
    rounds: u32,
}

#[derive(Debug)]
struct Uploading {
    /// Its swarm streams to this node's server, and how many are interested.
    streams: usize,
    interested: usize,
    /// What this node uploads to it.
    rate: Rate,
    /// Whether it is unchoked: every stream follows it.
    unchoked: watch::Sender<bool>,
}

/// One swarm stream in [`Uploads`], until dropped.
struct UploadStream {
    uploads: Uploads,
    remote: EndpointId,
    interested: bool,
}

impl Uploads {
    /// A new stream from `remote`, choked until the choker says otherwise:
    /// whether `remote` is unchoked, to follow.
    fn join(&self, remote: EndpointId) -> (UploadStream, watch::Receiver<bool>) {
        let mut state = self.0.lock().unwrap();
        let peer = state.peers.entry(remote).or_insert_with(|| Uploading {
            streams: 0,
            interested: 0,
            rate: Rate::default(),
            unchoked: watch::channel(false).0,
        });
        peer.streams += 1;
        let unchoked = peer.unchoked.subscribe();
        let stream = UploadStream {
            uploads: self.clone(),
            remote,
            interested: false,
        };
        (stream, unchoked)
    }

    /// Runs a choker round: on the [`ROUND`] timer (`timer`), which also
    /// rotates the optimistic unchoke every [`ROTATE_EVERY`] rounds, or
    /// because a peer's interest changed.
    fn round(&self, timer: bool) {
        let mut guard = self.0.lock().unwrap();
        let state = &mut *guard;
        let rotate = timer && {
            state.rounds += 1;
            state.rounds.is_multiple_of(ROTATE_EVERY)
        };
        let now = Instant::now();
        let interested: Vec<(EndpointId, f64)> = state
            .peers
            .iter_mut()
            .filter(|(_, p)| p.interested > 0)
            .map(|(id, p)| (*id, p.rate.per_second(now)))
            .collect();
        let unchoked = state.choker.round(&interested, rotate);
        for (id, peer) in &state.peers {
            let now = unchoked.contains(id);
            peer.unchoked
                .send_if_modified(|was| std::mem::replace(was, now) != now);
        }
    }

    /// Runs choker rounds every [`ROUND`], until dropped.
    fn run(&self) -> AbortOnDropHandle<()> {
        let uploads = self.clone();
        AbortOnDropHandle::new(tokio::spawn(async move {
            let mut every = tokio::time::interval(ROUND);
            every.tick().await;
            loop {
                every.tick().await;
                uploads.round(true);
            }
        }))
    }
}

impl UploadStream {
    fn interested(&mut self, interested: bool) {
        if self.interested == interested {
            return;
        }
        self.interested = interested;
        {
            let mut state = self.uploads.0.lock().unwrap();
            if let Some(peer) = state.peers.get_mut(&self.remote) {
                if interested {
                    peer.interested += 1;
                } else {
                    peer.interested -= 1;
                }
            }
        }
        self.uploads.round(false);
    }

    fn sent(&self, bytes: u32) {
        let mut state = self.uploads.0.lock().unwrap();
        if let Some(peer) = state.peers.get_mut(&self.remote) {
            peer.rate.add(u64::from(bytes), Instant::now());
        }
    }
}

impl Drop for UploadStream {
    fn drop(&mut self) {
        self.interested(false);
        let mut state = self.uploads.0.lock().unwrap();
        if let Some(peer) = state.peers.get_mut(&self.remote) {
            peer.streams -= 1;
            if peer.streams == 0 {
                state.peers.remove(&self.remote);
            }
        }
    }
}

/// The next value of `haves`, once it changes; `None` once its sender is
/// gone. Never, without `haves`.
async fn changed(haves: &mut Option<watch::Receiver<Vec<bool>>>) -> Option<Vec<bool>> {
    let haves = match haves {
        Some(haves) => haves,
        None => std::future::pending().await,
    };
    haves.changed().await.ok()?;
    Some(haves.borrow_and_update().clone())
}

/// Reads every message on `recv` into `messages`, until the stream ends (a
/// last `Ok(None)` is not sent: the channel closes) or fails.
async fn read_all(mut recv: WireRecv, messages: mpsc::Sender<anyhow::Result<(Message, Vec<u8>)>>) {
    loop {
        let got = match recv.recv().await {
            Ok(Some(got)) => Ok(got),
            Ok(None) => return,
            Err(err) => Err(err),
        };
        let failed = got.is_err();
        if messages.send(got).await.is_err() || failed {
            return;
        }
    }
}

/// The swarms this node is downloading, so its server can serve the
/// pieces they have verified from their staging files: a leecher serves.
#[derive(Debug, Clone, Default)]
pub struct Swarms(Arc<Mutex<HashMap<SwarmKey, Arc<Leeching>>>>);

/// A swarm: folder, path and version, as a [`Handshake`] names it.
type SwarmKey = (NamespaceId, String, Hash);

/// One running download, as its swarm's server sees it.
#[derive(Debug)]
struct Leeching {
    meta: FileMeta,
    staging: PathBuf,
    file: Arc<std::fs::File>,
    /// The pieces verified so far; servers follow it to send `Have`.
    have: watch::Sender<Vec<bool>>,
    /// Pieces a server found no longer match in the staging file.
    lost: mpsc::UnboundedSender<u32>,
}

/// Keeps a download in [`Swarms`] until dropped.
struct Registered {
    swarms: Swarms,
    key: SwarmKey,
}

impl Drop for Registered {
    fn drop(&mut self) {
        self.swarms.0.lock().unwrap().remove(&self.key);
    }
}

impl Swarms {
    fn register(&self, handshake: &Handshake, leeching: Leeching) -> Registered {
        let key = (handshake.folder, handshake.path.clone(), handshake.content);
        self.0
            .lock()
            .unwrap()
            .insert(key.clone(), Arc::new(leeching));
        Registered {
            swarms: self.clone(),
            key,
        }
    }

    fn get(&self, handshake: &Handshake) -> Option<Arc<Leeching>> {
        let key = (handshake.folder, handshake.path.clone(), handshake.content);
        self.0.lock().unwrap().get(&key).cloned()
    }

    /// Whether a running download fetches into the staging file `path`.
    pub(crate) fn uses(&self, path: &Path) -> bool {
        self.0.lock().unwrap().values().any(|l| l.staging == path)
    }
}

impl ProtocolHandler for PeerWireProtocol {
    async fn accept(&self, conn: Connection) -> Result<(), AcceptError> {
        let remote = conn.remote_id();
        while let Ok((send, recv)) = conn.accept_bi().await {
            tokio::spawn(self.clone().swarm(remote, conn.weak_handle(), send, recv));
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

/// Which pieces of a file of `count` pieces `have` says this node has, as
/// the message that tells a peer: `HaveAll`, `HaveNone` or a `Bitfield`.
pub fn bitfield_message(have: &[bool]) -> Message {
    if have.iter().all(|h| *h) {
        return Message::HaveAll;
    }
    if !have.iter().any(|h| *h) {
        return Message::HaveNone;
    }
    let mut bits = vec![0u8; have.len().div_ceil(8)];
    for (i, _) in have.iter().enumerate().filter(|(_, h)| **h) {
        bits[i / 8] |= 0x80 >> (i % 8);
    }
    Message::Bitfield(bits)
}

/// The pieces a peer's bitfield message says it has, for a file of `count`
/// pieces; `None` if it isn't a bitfield message, or not one for `count`
/// pieces (wrong length, or spare bits set).
pub fn holds(msg: &Message, count: u32) -> Option<Holds> {
    let count = count as usize;
    match msg {
        Message::HaveAll => Some(Holds::All),
        Message::HaveNone => Some(Holds::These(vec![false; count])),
        Message::Bitfield(bits) => {
            if bits.len() != count.div_ceil(8) {
                return None;
            }
            let have: Vec<bool> = (0..bits.len() * 8)
                .map(|i| bits[i / 8] & (0x80 >> (i % 8)) != 0)
                .collect();
            if have[count..].iter().any(|h| *h) {
                return None;
            }
            Some(Holds::These(have[..count].to_vec()))
        }
        _ => None,
    }
}

/// How often a download asks the nodes it isn't connected to again: a
/// node that got the version since (its entry hadn't synced), or came back,
/// joins the swarm.
pub const REJOIN: Duration = Duration::from_secs(2);

/// How long a download goes on with no peer joined before it gives up, for
/// a later try.
pub const GIVE_UP: Duration = Duration::from_secs(30);

/// The downloading side of the peer wire: one connection per peer, kept
/// while it stays open, and one stream per swarm on it.
#[derive(Debug, Clone)]
pub struct Client {
    endpoint: Endpoint,
    /// `--direct-only`: no requests to a peer reached through a relay.
    direct_only: bool,
    conns: Arc<Mutex<HashMap<EndpointId, Connection>>>,
    /// Bytes of verified pieces written, per peer.
    fetched: Arc<Mutex<HashMap<EndpointId, u64>>>,
    /// The downloads running, for the server.
    swarms: Swarms,
}

/// One node a download asks.
struct Remote {
    id: EndpointId,
    /// Bumped on each join, so events from an earlier one are ignored.
    generation: u64,
    state: State,
}

enum State {
    /// Not in the swarm; asked again every [`REJOIN`] unless `banned`.
    Out {
        banned: bool,
    },
    Joining,
    In {
        conn: Connection,
        send: WireSend,
        /// Its number in the [`Picker`].
        peer: usize,
        unchoked: bool,
        interested: bool,
        /// What it sends this node, which sizes its request queue.
        rate: Rate,
        /// When it last sent a block, or was unchoked: for snubbing.
        last_block: Instant,
    },
}

enum Event {
    Joined(anyhow::Result<(Connection, Wire, Message)>),
    Received(anyhow::Result<Option<(Message, Vec<u8>)>>),
}

impl Client {
    pub fn new(endpoint: Endpoint, direct_only: bool) -> Self {
        Self {
            endpoint,
            direct_only,
            conns: Arc::default(),
            fetched: Arc::default(),
            swarms: Swarms::default(),
        }
    }

    /// The downloads running, which the server serves from as a leecher.
    pub(crate) fn swarms(&self) -> Swarms {
        self.swarms.clone()
    }

    /// Bytes of file data fetched from each peer since this node started:
    /// verified pieces written, each block counted once, for the peer that
    /// sent it.
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

    /// Downloads what `staged` lacks of version `meta` of `path` in `folder`:
    /// joins the swarm with every node `peers` names, says it is interested
    /// in those that have pieces it lacks, and once unchoked asks for blocks
    /// as the [`Picker`] hands them out, as many at a time as
    /// [`queue_length`] says for the speed measured from that peer. Each
    /// piece is checked against its hash before it is written to the
    /// staging file. A peer that sends a piece that doesn't match, or
    /// rejects a request because it no longer has the file, is dropped for
    /// this download. `peers` is asked again every [`REJOIN`].
    ///
    /// Returns whether the file is complete (and synced to disk); if not,
    /// the staging file keeps the pieces that arrived, for a later try.
    pub async fn download(
        &self,
        folder: NamespaceId,
        path: &str,
        meta: &FileMeta,
        mut staged: Staged,
        peers: impl Fn() -> Vec<EndpointId>,
    ) -> anyhow::Result<bool> {
        let content = meta.content.context("fetching a tombstone")?;
        let handshake = Handshake {
            folder,
            path: path.to_string(),
            content,
        };
        let mut picker = Picker::new(meta.piece_length, meta.size, staged.missing.drain(..));
        // In the swarm as a leecher from the start: the server serves the
        // pieces verified so far, and those to come.
        let (have, _) = watch::channel(picker.have());
        let (lost, mut lost_pieces) = mpsc::unbounded_channel();
        let _registered = self.swarms.register(
            &handshake,
            Leeching {
                meta: meta.clone(),
                staging: staged.path.clone(),
                file: staged.file.clone(),
                have: have.clone(),
                lost,
            },
        );
        let (events, mut received) = mpsc::unbounded_channel::<(usize, u64, Event)>();
        // Joins and readers: stopped with the download.
        let mut tasks = JoinSet::new();
        let mut remotes: Vec<Remote> = Vec::new();
        // The node behind each picker peer.
        let mut ids: Vec<EndpointId> = Vec::new();
        let mut rejoin = tokio::time::interval(REJOIN);
        rejoin.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut last_in = Instant::now();
        let me = self.endpoint.id();
        while !picker.is_complete() {
            self.request(&mut remotes, &mut picker).await;
            let now = Instant::now();
            if remotes.iter().any(|r| matches!(r.state, State::In { .. })) {
                last_in = now;
            }
            let stuck = remotes
                .iter()
                .all(|r| matches!(r.state, State::Out { banned: true }));
            if now - last_in > GIVE_UP || (stuck && !remotes.is_empty()) {
                return Ok(false);
            }
            let wake = picker.next_overdue(now);
            tokio::select! {
                _ = rejoin.tick() => {
                    for id in peers() {
                        if id != me && !remotes.iter().any(|r| r.id == id) {
                            remotes.push(Remote { id, generation: 0, state: State::Out { banned: false } });
                        }
                    }
                    for (i, remote) in remotes.iter_mut().enumerate() {
                        if matches!(remote.state, State::Out { banned: false }) {
                            remote.generation += 1;
                            remote.state = State::Joining;
                            let (client, handshake, events) = (self.clone(), handshake.clone(), events.clone());
                            let (id, generation) = (remote.id, remote.generation);
                            tasks.spawn(async move {
                                let joined = async {
                                    let conn = client.connect(id).await?;
                                    let (wire, answer) = Wire::open(&conn, &handshake).await?;
                                    anyhow::Ok((conn, wire, answer))
                                };
                                let _ = events.send((i, generation, Event::Joined(joined.await)));
                            });
                        }
                    }
                }
                Some((i, generation, event)) = received.recv() => {
                    if remotes[i].generation != generation {
                        continue;
                    }
                    match event {
                        Event::Joined(joined) => {
                            self.joined(i, joined, &mut remotes, &mut picker, &mut ids, &events, &mut tasks).await;
                        }
                        Event::Received(Ok(Some((msg, data)))) => {
                            self.on_message(i, msg, &data, &mut remotes, &mut picker, &ids, meta, &staged, &have, path).await?;
                        }
                        Event::Received(ended) => {
                            if let Err(err) = ended {
                                tracing::debug!("{}: {path}: {err:#}", remotes[i].id.fmt_short());
                            }
                            leave(&mut remotes[i], &mut picker, false);
                        }
                    }
                }
                Some(piece) = lost_pieces.recv() => {
                    tracing::debug!("{path}: piece {piece} no longer matches in the staging file");
                    picker.lost(piece);
                }
                () = tokio::time::sleep_until(wake.unwrap_or(now).into()), if wake.is_some() => {}
            }
        }
        for remote in &mut remotes {
            if let State::In { send, .. } = &mut remote.state {
                let _ = send.send(&Message::NotInterested).await;
                let _ = send.finish();
            }
        }
        tokio::task::spawn_blocking(move || staged.sync()).await??;
        Ok(true)
    }

    /// Asks every unchoked peer in the swarm for blocks, up to its
    /// [`queue_length`] open: one if it has sent nothing for [`SNUB_AFTER`]
    /// with requests open (snubbed).
    async fn request(&self, remotes: &mut [Remote], picker: &mut Picker) {
        let now = Instant::now();
        for remote in remotes.iter_mut() {
            let State::In {
                conn,
                send,
                peer,
                unchoked: true,
                rate,
                last_block,
                ..
            } = &mut remote.state
            else {
                continue;
            };
            if self.direct_only && !crate::peers::is_direct(conn) {
                continue;
            }
            let snubbed = picker.open(*peer) > 0 && now - *last_block > SNUB_AFTER;
            let queue = queue_length(rate.per_second(now), snubbed);
            let mut failed = false;
            while picker.open(*peer) < queue {
                let Some(block) = picker.next(*peer, Instant::now()) else {
                    break;
                };
                if let Err(err) = send.send(&Message::Request(block)).await {
                    tracing::debug!("{}: requesting: {err:#}", remote.id.fmt_short());
                    failed = true;
                    break;
                }
            }
            if failed {
                leave(remote, picker, false);
            }
        }
    }

    /// A join started at the last [`REJOIN`] ended: the peer is in the
    /// swarm (it sent its bitfield) or not.
    #[allow(clippy::too_many_arguments)]
    async fn joined(
        &self,
        i: usize,
        joined: anyhow::Result<(Connection, Wire, Message)>,
        remotes: &mut [Remote],
        picker: &mut Picker,
        ids: &mut Vec<EndpointId>,
        events: &mpsc::UnboundedSender<(usize, u64, Event)>,
        tasks: &mut JoinSet<()>,
    ) {
        let remote = &mut remotes[i];
        let (conn, wire, answer) = match joined {
            Ok(joined) => joined,
            Err(err) => {
                tracing::debug!("{}: joining the swarm: {err:#}", remote.id.fmt_short());
                let mut conns = self.conns.lock().unwrap();
                if conns
                    .get(&remote.id)
                    .is_some_and(|c| c.close_reason().is_some())
                {
                    conns.remove(&remote.id);
                }
                remote.state = State::Out { banned: false };
                return;
            }
        };
        let Some(holds) = holds(&answer, picker.piece_count()) else {
            // Refused (it doesn't have the version, or doesn't keep the
            // folder): it may later.
            tracing::debug!("{}: not in the swarm: {answer:?}", remote.id.fmt_short());
            remote.state = State::Out { banned: false };
            return;
        };
        let interested = picker.wants(&holds);
        let peer = picker.add_peer(holds);
        ids.push(remote.id);
        let (mut send, mut recv) = wire.split();
        let mut said = send.send(&bitfield_message(&picker.have())).await;
        if said.is_ok() && interested {
            said = send.send(&Message::Interested).await;
        }
        if let Err(err) = said {
            tracing::debug!("{}: joining the swarm: {err:#}", remote.id.fmt_short());
            picker.drop_peer(peer);
            remote.state = State::Out { banned: false };
            return;
        }
        let (events, generation) = (events.clone(), remote.generation);
        tasks.spawn(async move {
            loop {
                let got = recv.recv().await;
                let more = matches!(got, Ok(Some(_)));
                if events.send((i, generation, Event::Received(got))).is_err() || !more {
                    return;
                }
            }
        });
        remote.state = State::In {
            conn,
            send,
            peer,
            unchoked: false,
            interested,
            rate: Rate::default(),
            last_block: Instant::now(),
        };
    }

    /// Handles one message from the peer of `remotes[i]`.
    #[allow(clippy::too_many_arguments)]
    async fn on_message(
        &self,
        i: usize,
        msg: Message,
        data: &[u8],
        remotes: &mut [Remote],
        picker: &mut Picker,
        ids: &[EndpointId],
        meta: &FileMeta,
        staged: &Staged,
        have: &watch::Sender<Vec<bool>>,
        path: &str,
    ) -> anyhow::Result<()> {
        let id = remotes[i].id;
        let State::In {
            send,
            peer,
            unchoked,
            interested,
            rate,
            last_block,
            ..
        } = &mut remotes[i].state
        else {
            return Ok(());
        };
        let peer = *peer;
        match msg {
            Message::Unchoke => {
                *unchoked = true;
                *last_block = Instant::now();
            }
            Message::Choke => *unchoked = false,
            Message::Have(piece) => {
                picker.has(peer, piece);
                if !*interested && picker.wants(&Holds::These(one_piece(picker, piece))) {
                    *interested = true;
                    let _ = send.send(&Message::Interested).await;
                }
            }
            Message::Piece(block) => {
                let now = Instant::now();
                rate.add(u64::from(block.length), now);
                *last_block = now;
                let got = picker.received(peer, block, data, now);
                for other in got.cancel {
                    let owner = remotes
                        .iter_mut()
                        .find(|r| matches!(&r.state, State::In { peer, .. } if *peer == other));
                    if let Some(Remote {
                        state: State::In { send, .. },
                        ..
                    }) = owner
                    {
                        let _ = send.send(&Message::Cancel(block)).await;
                    }
                }
                if let Some(complete) = got.complete {
                    self.check(complete, remotes, picker, ids, meta, staged, have, path)
                        .await?;
                }
            }
            Message::Reject(block, why) => {
                picker.rejected(peer, block);
                match why {
                    // Asked again once unchoked, or once the path is direct.
                    Why::Choked | Why::Policy => {}
                    // A leecher whose staging file lost the piece, or whose
                    // download ended: asked for the others.
                    Why::NotHave => picker.lacks(peer, block.piece),
                    Why::Changed | Why::Invalid => {
                        tracing::debug!("{}: {path}: rejected {block:?}: {why}", id.fmt_short());
                        leave(&mut remotes[i], picker, true);
                    }
                }
            }
            // Requests are for leechers that serve: BitTorrent plan step 5.
            Message::Refused(_)
            | Message::Bitfield(_)
            | Message::HaveAll
            | Message::HaveNone
            | Message::Interested
            | Message::NotInterested
            | Message::Request(_)
            | Message::Cancel(_) => {}
        }
        Ok(())
    }

    /// Checks a piece that is complete in memory against its hash: writes
    /// it to the staging file if it matches, else drops every peer that
    /// sent part of it and asks for it again.
    #[allow(clippy::too_many_arguments)]
    async fn check(
        &self,
        complete: Complete,
        remotes: &mut [Remote],
        picker: &mut Picker,
        ids: &[EndpointId],
        meta: &FileMeta,
        staged: &Staged,
        have: &watch::Sender<Vec<bool>>,
        path: &str,
    ) -> anyhow::Result<()> {
        let Complete {
            piece,
            data,
            senders,
        } = complete;
        let expected = meta.pieces[piece as usize];
        let (offset, _) = span(meta, piece);
        let file = staged.file.clone();
        let written = tokio::task::spawn_blocking(move || {
            if Hash::new(&data) != expected {
                return Ok(false);
            }
            file.write_all_at(&data, offset).map(|()| true)
        })
        .await?
        .context("writing the staging file")?;
        if written {
            picker.verified(piece);
            have.send_modify(|have| have[piece as usize] = true);
            let mut fetched = self.fetched.lock().unwrap();
            for (peer, bytes) in senders {
                *fetched.entry(ids[peer]).or_default() += u64::from(bytes);
            }
            return Ok(());
        }
        picker.failed(piece);
        for (peer, _) in senders {
            let bad = remotes
                .iter_mut()
                .find(|r| matches!(&r.state, State::In { peer: p, .. } if *p == peer));
            if let Some(remote) = bad {
                tracing::debug!(
                    "{}: {path}: piece {piece} doesn't match its hash",
                    remote.id.fmt_short()
                );
                leave(remote, picker, true);
            }
        }
        Ok(())
    }
}

/// A file's pieces with only `piece` held, to ask the picker whether it
/// wants it.
fn one_piece(picker: &Picker, piece: u32) -> Vec<bool> {
    (0..picker.piece_count()).map(|i| i == piece).collect()
}

/// Takes `remote` out of the swarm: its requests go back to the picker.
/// `banned`: not asked again during this download.
fn leave(remote: &mut Remote, picker: &mut Picker, banned: bool) {
    if let State::In { peer, send, .. } = &mut remote.state {
        picker.drop_peer(*peer);
        send.reset();
    }
    remote.state = State::Out { banned };
}
