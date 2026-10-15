//! TCP connection to the rendezvous server.

use std::io;
use std::net::SocketAddr;

use protocol::{read_frame, write_frame, ClientMessage, PeerId, ServerMessage};
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

pub struct Rendezvous {
    writer: OwnedWriteHalf,
    /// Fed by a dedicated reader task: `read_frame` isn't cancel-safe, so it
    /// must not race a timer in `select!`, but receiving from a channel is.
    inbox: mpsc::Receiver<io::Result<ServerMessage>>,
    reader: JoinHandle<()>,
}

impl Rendezvous {
    pub async fn connect(addr: SocketAddr) -> io::Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        let (mut read_half, writer) = stream.into_split();
        let (tx, inbox) = mpsc::channel(32);
        let reader = tokio::spawn(async move {
            loop {
                let msg = match read_frame(&mut read_half).await {
                    Ok(Some(msg)) => Ok(msg),
                    Ok(None) => break,
                    Err(e) => Err(e),
                };
                let failed = msg.is_err();
                if tx.send(msg).await.is_err() || failed {
                    break;
                }
            }
        });
        Ok(Self {
            writer,
            inbox,
            reader,
        })
    }

    /// Registers under `token` and returns the id the server assigned.
    pub async fn register(&mut self, token: &str) -> io::Result<PeerId> {
        self.send(&ClientMessage::Register {
            token: token.to_owned(),
        })
        .await?;
        match self.recv().await? {
            ServerMessage::Registered { peer_id } => Ok(peer_id),
            ServerMessage::Error { reason } => {
                Err(io::Error::other(format!("registration rejected: {reason}")))
            }
            other => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("expected Registered, got {other:?}"),
            )),
        }
    }

    pub async fn send(&mut self, msg: &ClientMessage) -> io::Result<()> {
        write_frame(&mut self.writer, msg).await
    }

    /// The next message from the server. Cancel-safe.
    pub async fn recv(&mut self) -> io::Result<ServerMessage> {
        self.inbox.recv().await.unwrap_or_else(|| {
            Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "rendezvous server closed the connection",
            ))
        })
    }
}

impl Drop for Rendezvous {
    fn drop(&mut self) {
        self.reader.abort();
    }
}
