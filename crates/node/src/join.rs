//! The join protocol: a new node gets its record and the membership doc.
//!
//! The joiner opens one stream and sends a [`JoinRequest`], either:
//!
//! * [`JoinRequest::Redeem`]: an invite. The member checks it, signs the
//!   joiner's record and writes it to the membership doc; or
//! * [`JoinRequest::Fetch`]: "I've been added by ID". The member looks up the
//!   joiner's record in its copy of the membership doc, so any member the
//!   record has reached can answer. The joiner needs only that member's ID.
//!
//! Either way the member replies with a [`JoinReply::Welcome`]: the joiner's
//! chain and a ticket for the membership doc. That is the whole exchange. It
//! is the only protocol an unknown ID can use besides the chain exchange, so
//! requests are handled one at a time and a refusal is delayed. Reading the
//! request has a time limit and happens before taking a turn, so an idle
//! connection can't hold up other joins.

use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context};
use iroh::endpoint::{Connection, VarInt};
use iroh::protocol::{AcceptError, ProtocolHandler};
use iroh::{Endpoint, EndpointAddr, EndpointId, SecretKey};
use iroh_blobs::store::fs::FsStore;
use iroh_docs::api::protocol::{AddrInfoOptions, ShareMode};
use iroh_docs::protocol::Docs;
use iroh_docs::DocTicket;
use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;

use crate::gate::{Gate, JOIN_ALPN, MAX_MESSAGE};
use crate::invite::{self, InviteTicket};
use crate::member::{self, Names};
use crate::node::read_members;
use crate::record::{chain_of, GroupId, SignedRecord};

/// How long a refused request waits before the reply, to slow guessing.
const REFUSAL_DELAY: Duration = Duration::from_secs(1);

/// How long the joiner has to send its request, and to close the connection
/// once it has the reply.
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// What `join` takes: an invite, or the node ID of a member that added this
/// node by ID.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Ticket {
    Invite(InviteTicket),
    /// A member to fetch this node's record from. Nothing secret: only a node
    /// whose ID is in the membership doc gets anything back.
    Member(EndpointAddr),
}

impl Ticket {
    /// The member to ask.
    pub fn member(&self) -> &EndpointAddr {
        match self {
            Ticket::Invite(ticket) => &ticket.inviter,
            Ticket::Member(addr) => addr,
        }
    }

    fn request(&self) -> JoinRequest {
        match self {
            Ticket::Invite(ticket) => JoinRequest::Redeem {
                group: ticket.group,
                secret: ticket.secret,
            },
            Ticket::Member(_) => JoinRequest::Fetch,
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("not an invite ticket or a node ID")]
pub struct ParseJoinError;

impl FromStr for Ticket {
    type Err = ParseJoinError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Ok(ticket) = s.parse() {
            return Ok(Ticket::Invite(ticket));
        }
        s.trim()
            .parse::<EndpointId>()
            .map(|id| Ticket::Member(EndpointAddr::new(id)))
            .map_err(|_| ParseJoinError)
    }
}

impl From<InviteTicket> for Ticket {
    fn from(ticket: InviteTicket) -> Self {
        Ticket::Invite(ticket)
    }
}

impl From<&InviteTicket> for Ticket {
    fn from(ticket: &InviteTicket) -> Self {
        Ticket::Invite(ticket.clone())
    }
}

impl From<EndpointAddr> for Ticket {
    fn from(addr: EndpointAddr) -> Self {
        Ticket::Member(addr)
    }
}

impl From<EndpointId> for Ticket {
    fn from(id: EndpointId) -> Self {
        Ticket::Member(EndpointAddr::new(id))
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum JoinRequest {
    /// Redeem an invite.
    Redeem { group: GroupId, secret: [u8; 32] },
    /// Send me my record: a member added me by ID. The joiner learns the
    /// group from the reply.
    Fetch,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum JoinReply {
    Welcome {
        /// The joiner's record, then its signer's, up to the founder's.
        chain: Vec<SignedRecord>,
        /// Write access to the membership doc.
        doc: Box<DocTicket>,
    },
    Refused(String),
}

/// The member's side.
#[derive(Debug, Clone)]
pub struct JoinProtocol {
    pub(crate) config_dir: PathBuf,
    pub(crate) secret_key: SecretKey,
    pub(crate) gate: Gate,
    pub(crate) docs: Docs,
    pub(crate) blobs: FsStore,
    pub(crate) names: Names,
    pub(crate) one_at_a_time: Arc<Semaphore>,
}

impl JoinProtocol {
    async fn handle(&self, joiner: EndpointId, request: JoinRequest) -> anyhow::Result<JoinReply> {
        let group = self.gate.group().context("not in a group")?;
        let chain = match request {
            JoinRequest::Redeem {
                group: wanted,
                secret,
            } => {
                if wanted != group.id {
                    bail!("invite is for another group");
                }
                let name = invite::redeem(&self.config_dir, &secret)?;
                let record =
                    member::write_record(&self.docs, group.id, &self.secret_key, joiner).await?;
                if let Some(name) = name {
                    match self.names.holder(&name) {
                        Some(holder) if holder != joiner => tracing::warn!(
                            "not naming {} {name:?}: {} already has that name",
                            joiner.fmt_short(),
                            holder.fmt_short()
                        ),
                        _ => member::write_name(&self.docs, group.id, joiner, &name).await?,
                    }
                }
                self.gate.admit(joiner, "redeemed an invite");
                let mut chain = vec![record];
                chain.extend(group.chain);
                chain
            }
            JoinRequest::Fetch => {
                let members = read_members(&self.docs, &self.blobs, &group).await?;
                if !members.iter().any(|s| s.record.member == joiner) {
                    bail!(
                        "{} is not a member in this node's copy of the group; \
                         if it was just added, retry once the add has synced",
                        joiner.fmt_short()
                    );
                }
                self.gate.admit(joiner, "fetched its record");
                chain_of(joiner, &members)
            }
        };
        let doc = self
            .docs
            .open(group.id)
            .await?
            .context("membership doc missing")?;
        let ticket = doc
            .share(ShareMode::Write, AddrInfoOptions::RelayAndAddresses)
            .await?;
        tracing::info!("{} joined the group", self.names.display(joiner));
        Ok(JoinReply::Welcome {
            chain,
            doc: Box::new(ticket),
        })
    }
}

impl ProtocolHandler for JoinProtocol {
    async fn accept(&self, conn: Connection) -> Result<(), AcceptError> {
        let joiner = conn.remote_id();
        let read = tokio::time::timeout(REQUEST_TIMEOUT, async {
            let (send, mut recv) = conn.accept_bi().await?;
            let request = recv
                .read_to_end(MAX_MESSAGE)
                .await
                .map_err(AcceptError::from_err)?;
            Ok::<_, AcceptError>((send, request))
        })
        .await;
        let Ok(read) = read else {
            tracing::debug!("join request from {} timed out", joiner.fmt_short());
            conn.close(VarInt::from_u32(0), b"timed out");
            return Ok(());
        };
        let (mut send, request) = read?;

        let reply = {
            let _turn = self.one_at_a_time.acquire().await;
            let reply = match postcard::from_bytes::<JoinRequest>(&request) {
                Ok(request) => self.handle(joiner, request).await,
                Err(err) => Err(err.into()),
            };
            let reply = reply.unwrap_or_else(|err| {
                tracing::info!("refused join from {}: {err:#}", joiner.fmt_short());
                JoinReply::Refused(format!("{err:#}"))
            });
            if matches!(reply, JoinReply::Refused(_)) {
                tokio::time::sleep(REFUSAL_DELAY).await;
            }
            reply
        };

        let bytes = postcard::to_stdvec(&reply).map_err(AcceptError::from_err)?;
        send.write_all(&bytes)
            .await
            .map_err(AcceptError::from_err)?;
        send.finish()?;
        // The joiner closes once it has read the reply.
        let _ = tokio::time::timeout(REQUEST_TIMEOUT, conn.closed()).await;
        Ok(())
    }
}

/// The joiner's side: sends `ticket`'s request to its member and returns the
/// reply.
pub async fn request(endpoint: &Endpoint, ticket: &Ticket) -> anyhow::Result<JoinReply> {
    let conn = endpoint
        .connect(ticket.member().clone(), JOIN_ALPN)
        .await
        .context("connecting to the member")?;
    let (mut send, mut recv) = conn.open_bi().await?;
    send.write_all(&postcard::to_stdvec(&ticket.request())?)
        .await?;
    send.finish()?;
    let reply = recv.read_to_end(MAX_MESSAGE).await?;
    conn.close(VarInt::from_u32(0), b"done");
    Ok(postcard::from_bytes(&reply)?)
}
