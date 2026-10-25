//! Utilities for iroh-gossip networking

use std::{
    collections::{hash_map, HashMap},
    io,
    time::Duration,
};

use bytes::{Bytes, BytesMut};
use iroh::{
    endpoint::{Connection, RecvStream, SendStream},
    EndpointId,
};
use n0_error::{e, stack_error};
use n0_future::{
    task::JoinSet,
    time::{sleep_until, Instant},
    FuturesUnordered, StreamExt,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::{mpsc, oneshot},
};
use tracing::{debug, trace, Instrument};

use super::{InEvent, ProtoMessage};
use crate::proto::{util::TimerMap, TopicId};

/// Errors related to message writing
#[allow(missing_docs)]
#[stack_error(derive, add_meta, from_sources)]
#[non_exhaustive]
pub(crate) enum WriteError {
    /// Connection error
    #[error("Connection error")]
    Connection {
        #[error(std_err)]
        source: iroh::endpoint::ConnectionError,
    },
    /// Serialization failed
    #[error("Serialization failed")]
    Ser {
        #[error(std_err)]
        source: postcard::Error,
    },
    /// IO error
    #[error("IO error")]
    Io {
        #[error(std_err)]
        source: std::io::Error,
    },
    /// Message was larger than the configured maximum message size
    #[error("message too large")]
    TooLarge {},
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct StreamHeader {
    pub(crate) topic_id: TopicId,
}

impl StreamHeader {
    pub(crate) async fn read(
        stream: &mut RecvStream,
        buffer: &mut BytesMut,
        max_message_size: usize,
    ) -> Result<Self, ReadError> {
        let header: Self = read_frame(stream, buffer, max_message_size)
            .await?
            .ok_or_else(|| {
                ReadError::from(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "stream ended before header",
                ))
            })?;
        Ok(header)
    }

    pub(crate) async fn write(
        self,
        stream: &mut SendStream,
        buffer: &mut Vec<u8>,
        max_message_size: usize,
    ) -> Result<(), WriteError> {
        write_frame(stream, &self, buffer, max_message_size).await?;
        Ok(())
    }
}

pub(crate) struct RecvLoop {
    remote_endpoint_id: EndpointId,
    conn: Connection,
    max_message_size: usize,
    in_event_tx: mpsc::Sender<InEvent>,
}

impl RecvLoop {
    pub(crate) fn new(
        remote_endpoint_id: EndpointId,
        conn: Connection,
        in_event_tx: mpsc::Sender<InEvent>,
        max_message_size: usize,
    ) -> Self {
        Self {
            remote_endpoint_id,
            conn,
            max_message_size,
            in_event_tx,
        }
    }

    /// Reads messages until the connection is closed, or until `send_done`
    /// fires and the remote, having opened streams, has finished them all.
    ///
    /// sync-node patch: in 0.101.0 this ran until the connection was closed,
    /// and the connection was closed only once this returned. A connection
    /// both sides had stopped using (the peer dropped, or another connection
    /// to it taken for sending) stayed open for good, kept alive by iroh,
    /// and a new one was dialed on top: one leaked per peer disconnect. A
    /// side that stops using a connection finishes its streams. So once our
    /// send loop has ended (`send_done`) and the remote has finished every
    /// stream it opened, neither side uses it: this returns and the caller
    /// closes it. A remote that has opened none yet may be about to use it
    /// (both sides dialed at once, and each sends on the other's), so the
    /// connection is kept until it does.
    pub(crate) async fn run(&mut self, send_done: oneshot::Receiver<()>) -> Result<(), ReadError> {
        let mut read_futures = FuturesUnordered::new();
        let mut conn_is_closed = false;
        let mut sending = true;
        let mut remote_opened = false;
        let closed = self.conn.closed();
        tokio::pin!(closed, send_done);
        while !conn_is_closed || !read_futures.is_empty() {
            if !sending && remote_opened && read_futures.is_empty() {
                debug!("neither side uses the connection");
                break;
            }
            tokio::select! {
                _ = &mut closed, if !conn_is_closed => {
                    conn_is_closed = true;
                }
                _ = &mut send_done, if sending => {
                    sending = false;
                }
                stream = self.conn.accept_uni(), if !conn_is_closed => {
                    let stream = match stream {
                        Ok(stream) => stream,
                        Err(_) => {
                            conn_is_closed = true;
                            continue;
                        }
                    };
                    remote_opened = true;
                    let state = RecvStreamState::new(stream, self.max_message_size).await?;
                    debug!(topic=%state.header.topic_id.fmt_short(), "stream opened");
                    read_futures.push(state.next());
                }
                Some(res) = read_futures.next(), if !read_futures.is_empty() => {
                    let (state, msg) = match res {
                        Ok((state, msg)) => (state, msg),
                        Err(err) => {
                            debug!("recv stream closed with error: {err:#}");
                            continue;
                        }
                    };
                    match msg {
                        None => debug!(topic=%state.header.topic_id.fmt_short(), "stream closed"),
                        Some(msg) => {
                            if self.in_event_tx.send(InEvent::RecvMessage(self.remote_endpoint_id, msg)).await.is_err() {
                                debug!("stop recv loop: actor closed");
                                break;
                            }
                            read_futures.push(state.next());
                        }
                    }
                }
            }
        }
        debug!("recv loop closed");
        Ok(())
    }
}

#[derive(Debug)]
struct RecvStreamState {
    stream: RecvStream,
    header: StreamHeader,
    buffer: BytesMut,
    max_message_size: usize,
}

impl RecvStreamState {
    async fn new(mut stream: RecvStream, max_message_size: usize) -> Result<Self, ReadError> {
        let mut buffer = BytesMut::new();
        let header = StreamHeader::read(&mut stream, &mut buffer, max_message_size).await?;
        Ok(Self {
            buffer: BytesMut::new(),
            max_message_size,
            stream,
            header,
        })
    }

    /// Reads the next message from the stream.
    ///
    /// Returns `self` and the next message, or `None` if the stream ended gracefully.
    ///
    /// ## Cancellation safety
    ///
    /// This function is not cancellation-safe.
    async fn next(mut self) -> Result<(Self, Option<ProtoMessage>), ReadError> {
        let msg = read_frame(&mut self.stream, &mut self.buffer, self.max_message_size).await?;
        let msg = msg.map(|msg| ProtoMessage {
            topic: self.header.topic_id,
            message: msg,
        });
        Ok((self, msg))
    }
}

pub(crate) struct SendLoop {
    conn: Connection,
    streams: HashMap<TopicId, SendStream>,
    buffer: Vec<u8>,
    max_message_size: usize,
    finishing: JoinSet<()>,
    send_rx: mpsc::Receiver<ProtoMessage>,
}

impl SendLoop {
    pub(crate) fn new(
        conn: Connection,
        send_rx: mpsc::Receiver<ProtoMessage>,
        max_message_size: usize,
    ) -> Self {
        Self {
            conn,
            max_message_size,
            buffer: Default::default(),
            streams: Default::default(),
            finishing: Default::default(),
            send_rx,
        }
    }

    pub(crate) async fn run(&mut self, queue: Vec<ProtoMessage>) -> Result<(), WriteError> {
        for msg in queue {
            self.write_message(&msg).await?;
        }
        let conn_clone = self.conn.clone();
        let closed = conn_clone.closed();
        tokio::pin!(closed);
        loop {
            tokio::select! {
                biased;
                _ = &mut closed => break,
                // sync-node patch: ends when the actor drops the sender. In
                // 0.101.0 a closed channel only disabled this branch, and
                // `else` never ran while `closed` was pending, so a dropped
                // connection kept its send loop (and streams) until closed.
                msg = self.send_rx.recv() => match msg {
                    Some(msg) => self.write_message(&msg).await?,
                    None => break,
                },
                _ = self.finishing.join_next(), if !self.finishing.is_empty() => {}
            }
        }

        // Close remaining streams.
        for (topic_id, mut stream) in self.streams.drain() {
            stream.finish().ok();
            self.finishing.spawn(
                async move {
                    stream.stopped().await.ok();
                    debug!(topic=%topic_id.fmt_short(), "stream closed");
                }
                .instrument(tracing::Span::current()),
            );
        }
        if !self.finishing.is_empty() {
            trace!(
                "send loop closing, waiting for {} send streams to finish",
                self.finishing.len()
            );
            // Wait for the remote to acknowledge all streams are finished.
            if let Err(_elapsed) = n0_future::time::timeout(Duration::from_secs(5), async move {
                while self.finishing.join_next().await.is_some() {}
            })
            .await
            {
                debug!("not all send streams finished within timeout, abort")
            }
        }
        debug!("send loop closed");
        Ok(())
    }

    /// Write a [`ProtoMessage`] as a length-prefixed, postcard-encoded message on its stream.
    ///
    /// If no stream is opened yet, this opens a new stream for the topic and writes the topic header.
    ///
    /// This function is not cancellation-safe.
    pub async fn write_message(&mut self, message: &ProtoMessage) -> Result<(), WriteError> {
        let ProtoMessage { topic, message } = message;
        let topic_id = *topic;
        let is_last = message.is_disconnect();

        let mut entry = match self.streams.entry(topic_id) {
            hash_map::Entry::Occupied(entry) => entry,
            hash_map::Entry::Vacant(entry) => {
                let mut stream = self.conn.open_uni().await?;
                let header = StreamHeader { topic_id };
                header
                    .write(&mut stream, &mut self.buffer, self.max_message_size)
                    .await?;
                debug!(topic=%topic_id.fmt_short(), "stream opened");
                entry.insert_entry(stream)
            }
        };
        let stream = entry.get_mut();

        write_frame(stream, message, &mut self.buffer, self.max_message_size).await?;

        if is_last {
            trace!(topic=%topic_id.fmt_short(), "stream closing");
            let mut stream = entry.remove();
            if stream.finish().is_ok() {
                self.finishing.spawn(
                    async move {
                        stream.stopped().await.ok();
                        debug!(topic=%topic_id.fmt_short(), "stream closed");
                    }
                    .instrument(tracing::Span::current()),
                );
            }
        }

        Ok(())
    }
}

/// Errors related to message reading
#[allow(missing_docs)]
#[stack_error(derive, add_meta, from_sources)]
#[non_exhaustive]
pub(crate) enum ReadError {
    /// Deserialization failed
    #[error("Deserialization failed")]
    De {
        #[error(std_err)]
        source: postcard::Error,
    },
    /// IO error
    #[error("IO error")]
    Io {
        #[error(std_err)]
        source: std::io::Error,
    },
    /// Message was larger than the configured maximum message size
    #[error("message too large")]
    TooLarge {},
}

/// Read a length-prefixed frame and decode with postcard.
pub async fn read_frame<T: DeserializeOwned>(
    reader: &mut RecvStream,
    buffer: &mut BytesMut,
    max_message_size: usize,
) -> Result<Option<T>, ReadError> {
    match read_lp(reader, buffer, max_message_size).await? {
        None => Ok(None),
        Some(data) => {
            let message = postcard::from_bytes(&data)?;
            Ok(Some(message))
        }
    }
}

/// Reads a length prefixed buffer.
///
/// Returns the frame as raw bytes.  If the end of the stream is reached before
/// the frame length starts, `None` is returned.
pub async fn read_lp(
    reader: &mut RecvStream,
    buffer: &mut BytesMut,
    max_message_size: usize,
) -> Result<Option<Bytes>, ReadError> {
    let size = match reader.read_u32().await {
        Ok(size) => size,
        Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(err) => return Err(err.into()),
    };
    let size = usize::try_from(size).map_err(|_| e!(ReadError::TooLarge))?;
    if size > max_message_size {
        return Err(e!(ReadError::TooLarge));
    }
    buffer.resize(size, 0u8);
    reader
        .read_exact(&mut buffer[..])
        .await
        .map_err(io::Error::other)?;
    Ok(Some(buffer.split_to(size).freeze()))
}

/// Writes a length-prefixed frame.
pub async fn write_frame<T: Serialize>(
    stream: &mut SendStream,
    message: &T,
    buffer: &mut Vec<u8>,
    max_message_size: usize,
) -> Result<(), WriteError> {
    let len = postcard::experimental::serialized_size(&message)?;
    if len >= max_message_size {
        return Err(e!(WriteError::TooLarge));
    }
    buffer.clear();
    buffer.resize(len, 0u8);
    let slice = postcard::to_slice(&message, buffer)?;
    stream.write_u32(len as u32).await?;
    stream.write_all(slice).await.map_err(io::Error::other)?;
    Ok(())
}

/// A [`TimerMap`] with an async method to wait for the next timer expiration.
#[derive(Debug)]
pub struct Timers<T> {
    map: TimerMap<T>,
}

impl<T> Default for Timers<T> {
    fn default() -> Self {
        Self {
            map: TimerMap::default(),
        }
    }
}

impl<T> Timers<T> {
    /// Creates a new timer map.
    pub fn new() -> Self {
        Self::default()
    }

    /// Inserts a new entry at the specified instant
    pub fn insert(&mut self, instant: Instant, item: T) {
        self.map.insert(instant, item);
    }

    /// Waits for the next timer to elapse.
    pub async fn wait_next(&mut self) -> Instant {
        match self.map.first() {
            None => std::future::pending::<Instant>().await,
            Some(instant) => {
                sleep_until(*instant).await;
                *instant
            }
        }
    }

    /// Pops the earliest timer that expires at or before `now`.
    pub fn pop_before(&mut self, now: Instant) -> Option<(Instant, T)> {
        self.map.pop_before(now)
    }
}
