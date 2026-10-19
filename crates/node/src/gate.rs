//! Accept-time gating: only group members get the docs, gossip and blobs
//! protocols, in either direction.
//!
//! An `after_handshake` hook checks every connection's remote ID. The join
//! and members ALPNs are open to anyone; every other ALPN needs the remote to
//! be admitted. A remote is admitted once this node has checked its chain of
//! records back to the founder:
//!
//! * the members of this node's own chain are admitted when the group is set;
//! * a node that redeems an invite here is admitted by the join protocol;
//! * any other node presents its chain over the members ALPN.
//!
//! The side that dials presents first. On every outgoing connection the hook
//! opens a members connection, sends this node's chain and gets the remote's
//! back, even if it already knows the remote: the remote may have restarted
//! and forgotten us. When it sees an incoming one from an unknown ID, it waits
//! for that remote to present. So a node only shows its chain to nodes it
//! chose to dial, or that have already shown theirs.

use std::collections::HashSet;
use std::future::Future;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use anyhow::Context;
use iroh::endpoint::{AfterHandshakeOutcome, Connection, EndpointHooks, Side, VarInt};
use iroh::protocol::{AcceptError, ProtocolHandler};
use iroh::{Endpoint, EndpointId};
use tokio::sync::Notify;

use crate::group::Group;
use crate::member::Names;
use crate::peers::Peers;
use crate::record::{validate_chain, SignedRecord};

/// The chain exchange. Open to unknown IDs.
pub const MEMBERS_ALPN: &[u8] = b"/sync-node/members/0";
/// Invite redemption. Open to unknown IDs.
pub const JOIN_ALPN: &[u8] = b"/sync-node/join/0";

/// How long an incoming connection from an unknown ID waits for it to present
/// its chain, and how long presenting ours may take.
pub const ADMIT_TIMEOUT: Duration = Duration::from_secs(5);

/// Largest chain or join message accepted.
pub(crate) const MAX_MESSAGE: usize = 64 * 1024;

/// Close code for a connection refused by the gate.
pub const NOT_A_MEMBER: u32 = 403;

#[derive(Debug, Clone, Default)]
pub struct Gate(Arc<Inner>);

#[derive(Debug, Default)]
struct Inner {
    group: RwLock<Option<Group>>,
    admitted: Mutex<HashSet<EndpointId>>,
    admitted_changed: Notify,
    names: Names,
    /// Set once the endpoint is bound, and cleared on shutdown: the endpoint
    /// holds the hooks, which hold this.
    endpoint: Mutex<Option<Endpoint>>,
}

impl Gate {
    /// A gate that shows members by `names` in its logs.
    pub fn new(names: Names) -> Self {
        Self(Arc::new(Inner {
            names,
            ..Default::default()
        }))
    }

    pub fn group(&self) -> Option<Group> {
        self.0.group.read().unwrap().clone()
    }

    /// Sets this node's group and admits the members of its own chain.
    pub fn set_group(&self, group: Group) {
        for signed in &group.chain {
            self.admit(signed.record.member, "own chain");
        }
        *self.0.group.write().unwrap() = Some(group);
    }

    /// Admits `id`; `how` says why, for the log.
    pub fn admit(&self, id: EndpointId, how: &str) {
        if self.0.admitted.lock().unwrap().insert(id) {
            tracing::info!("admitted {} ({how})", self.0.names.display(id));
            self.0.admitted_changed.notify_waiters();
        }
    }

    pub fn is_admitted(&self, id: EndpointId) -> bool {
        self.0.admitted.lock().unwrap().contains(&id)
    }

    pub(crate) fn set_endpoint(&self, endpoint: Option<Endpoint>) {
        *self.0.endpoint.lock().unwrap() = endpoint;
    }

    /// Checks `id`'s presented chain against this node's group and admits it.
    fn check_presented(&self, id: EndpointId, chain: &[SignedRecord]) -> anyhow::Result<()> {
        let group = self.group().context("not in a group")?;
        validate_chain(group.id, group.founder, id, chain)?;
        self.admit(id, "presented chain");
        Ok(())
    }

    /// Waits until `id` is admitted, up to `timeout`.
    async fn wait_admitted(&self, id: EndpointId, timeout: Duration) -> bool {
        tokio::time::timeout(timeout, async {
            loop {
                let changed = self.0.admitted_changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if self.is_admitted(id) {
                    return;
                }
                changed.await;
            }
        })
        .await
        .is_ok()
    }

    /// Sends this node's chain to `id` over the members ALPN, and checks and
    /// admits the chain it sends back.
    async fn present_to(&self, id: EndpointId) -> anyhow::Result<()> {
        let group = self.group().context("not in a group")?;
        let endpoint = self
            .0
            .endpoint
            .lock()
            .unwrap()
            .clone()
            .context("endpoint closed")?;
        let conn = endpoint.connect(id, MEMBERS_ALPN).await?;
        let (mut send, mut recv) = conn.open_bi().await?;
        send.write_all(&postcard::to_stdvec(&group.chain)?).await?;
        send.finish()?;
        let reply = recv.read_to_end(MAX_MESSAGE).await?;
        conn.close(VarInt::from_u32(0), b"done");
        let chain: Vec<SignedRecord> = postcard::from_bytes(&reply)?;
        self.check_presented(id, &chain)
    }
}

/// The hook half of the gate, installed with `Builder::hooks`. Hands every
/// admitted connection to `peers` to be followed.
#[derive(Debug)]
pub struct GateHooks {
    pub gate: Gate,
    pub peers: Peers,
}

impl EndpointHooks for GateHooks {
    fn after_handshake<'a>(
        &'a self,
        conn: &'a Connection,
    ) -> impl Future<Output = AfterHandshakeOutcome> + Send + 'a {
        let gate = &self.gate;
        let alpn = conn.alpn().to_vec();
        let remote = conn.remote_id();
        let side = conn.side();
        async move {
            if alpn == JOIN_ALPN || alpn == MEMBERS_ALPN {
                return AfterHandshakeOutcome::Accept;
            }
            let admitted = if gate.group().is_none() {
                false
            } else if side == Side::Client {
                match tokio::time::timeout(ADMIT_TIMEOUT, gate.present_to(remote)).await {
                    Ok(Ok(())) => true,
                    Ok(Err(err)) => {
                        tracing::debug!("{remote} did not prove membership: {err:#}");
                        false
                    }
                    Err(_) => false,
                }
            } else {
                gate.is_admitted(remote) || gate.wait_admitted(remote, ADMIT_TIMEOUT).await
            };
            if admitted {
                self.peers.track(conn);
                AfterHandshakeOutcome::Accept
            } else {
                tracing::info!(
                    "refused {remote} on {}: not a member",
                    String::from_utf8_lossy(&alpn)
                );
                AfterHandshakeOutcome::Reject {
                    error_code: VarInt::from_u32(NOT_A_MEMBER),
                    reason: b"not a member".to_vec(),
                }
            }
        }
    }
}

/// The accepting side of the chain exchange: checks the dialer's chain, admits
/// it and replies with this node's chain. Sends nothing to a dialer whose
/// chain doesn't check out.
#[derive(Debug, Clone)]
pub struct MembersProtocol(pub Gate);

impl ProtocolHandler for MembersProtocol {
    async fn accept(&self, conn: Connection) -> Result<(), AcceptError> {
        let remote = conn.remote_id();
        let (mut send, mut recv) = conn.accept_bi().await?;
        let request = recv
            .read_to_end(MAX_MESSAGE)
            .await
            .map_err(AcceptError::from_err)?;
        let checked = postcard::from_bytes::<Vec<SignedRecord>>(&request)
            .map_err(anyhow::Error::from)
            .and_then(|chain| self.0.check_presented(remote, &chain));
        if let Err(err) = checked {
            tracing::info!("{remote} presented an invalid chain: {err:#}");
            conn.close(VarInt::from_u32(NOT_A_MEMBER), b"not a member");
            return Ok(());
        }
        let chain = self.0.group().map(|group| group.chain).unwrap_or_default();
        let reply = postcard::to_stdvec(&chain).map_err(AcceptError::from_err)?;
        send.write_all(&reply)
            .await
            .map_err(AcceptError::from_err)?;
        send.finish()?;
        conn.closed().await;
        Ok(())
    }
}
