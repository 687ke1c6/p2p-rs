//! Presence: each member regularly gossips "I'm online, and this is how I
//! reach each member I'm connected to". From these every node builds the same
//! picture of the group: who is online and each pair's connection type.
//!
//! Messages travel on a gossip topic derived from the group ID. Gossip only
//! runs between admitted members (see the gate), and each message is signed
//! by its sender, since gossip forwards messages and `delivered_from` is only
//! the last hop.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use iroh::{EndpointId, SecretKey, Signature};
use iroh_gossip::TopicId;
use serde::{Deserialize, Serialize};

use crate::peers::LinkKind;
use crate::record::{GroupId, SignedRecord};

/// How often a node announces itself, by default.
pub const PRESENCE_INTERVAL: Duration = Duration::from_secs(5);
/// A member not heard from for this many intervals is offline.
pub const OFFLINE_AFTER_INTERVALS: u32 = 4;

const SIGNING_TAG: &[u8] = b"sync-node presence v0\0";

/// The group's presence topic.
pub fn topic(group: GroupId) -> TopicId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"sync-node presence topic v0\0");
    hasher.update(group.as_bytes());
    TopicId::from_bytes(*hasher.finalize().as_bytes())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Presence {
    pub group: GroupId,
    pub node: EndpointId,
    /// Sender's clock, unix milliseconds. Makes each message unique, so
    /// gossip doesn't drop a repeat as a duplicate.
    pub sent_at_ms: u64,
    /// The members the sender has a connection to, and its selected path's
    /// kind for each.
    pub links: Vec<(EndpointId, LinkKind)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignedPresence {
    pub presence: Presence,
    pub signature: Signature,
}

impl Presence {
    fn signing_bytes(&self) -> Vec<u8> {
        let mut bytes = SIGNING_TAG.to_vec();
        bytes.extend(postcard::to_stdvec(self).expect("presence serializes"));
        bytes
    }

    pub fn sign(self, key: &SecretKey) -> SignedPresence {
        assert_eq!(key.public(), self.node, "signer must be the node");
        let signature = key.sign(&self.signing_bytes());
        SignedPresence {
            presence: self,
            signature,
        }
    }
}

impl SignedPresence {
    pub fn verify(&self) -> bool {
        self.presence
            .node
            .verify(&self.presence.signing_bytes(), &self.signature)
            .is_ok()
    }
}

/// A pair's connection type, as the group reports it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(test, derive(ts_rs::TS))]
pub enum LinkState {
    Direct,
    Relay,
    /// Both are online and neither reports a connection to the other.
    NotConnected,
    /// At least one of the two is offline, so nobody can say.
    Unknown,
}

impl From<LinkKind> for LinkState {
    fn from(kind: LinkKind) -> Self {
        match kind {
            LinkKind::Direct => LinkState::Direct,
            LinkKind::Relay => LinkState::Relay,
        }
    }
}

impl fmt::Display for LinkState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            LinkState::Direct => "direct",
            LinkState::Relay => "relay",
            LinkState::NotConnected => "not connected",
            LinkState::Unknown => "unknown (offline)",
        })
    }
}

/// What this node has heard: the member list and the latest presence of
/// each member.
#[derive(Debug, Clone)]
pub struct View(Arc<Mutex<ViewState>>);

#[derive(Debug)]
struct ViewState {
    offline_after: Duration,
    members: Vec<SignedRecord>,
    heard: HashMap<EndpointId, (Presence, Instant)>,
    /// Online as of the last [`View::update_online`], for logging changes.
    online: HashSet<EndpointId>,
}

impl View {
    pub fn new(presence_interval: Duration) -> Self {
        Self(Arc::new(Mutex::new(ViewState {
            offline_after: presence_interval * OFFLINE_AFTER_INTERVALS,
            members: Vec::new(),
            heard: HashMap::new(),
            online: HashSet::new(),
        })))
    }

    pub fn members(&self) -> Vec<SignedRecord> {
        self.0.lock().unwrap().members.clone()
    }

    /// Replaces the member list and returns the members that are new.
    pub fn set_members(&self, members: Vec<SignedRecord>) -> Vec<SignedRecord> {
        let mut state = self.0.lock().unwrap();
        let known: HashSet<_> = state.members.iter().map(|s| s.record.member).collect();
        let added = members
            .iter()
            .filter(|s| !known.contains(&s.record.member))
            .cloned()
            .collect();
        state.members = members;
        added
    }

    pub fn is_member(&self, id: EndpointId) -> bool {
        self.0
            .lock()
            .unwrap()
            .members
            .iter()
            .any(|s| s.record.member == id)
    }

    /// Records a presence, keeping only the newest per node.
    pub fn heard(&self, presence: Presence) {
        let mut state = self.0.lock().unwrap();
        let newer = state
            .heard
            .get(&presence.node)
            .is_none_or(|(old, _)| presence.sent_at_ms >= old.sent_at_ms);
        if newer {
            state
                .heard
                .insert(presence.node, (presence, Instant::now()));
        }
    }

    /// How long ago `id` was last heard from, if ever.
    pub fn last_heard(&self, id: EndpointId) -> Option<Duration> {
        let state = self.0.lock().unwrap();
        state.heard.get(&id).map(|(_, at)| at.elapsed())
    }

    pub fn is_online(&self, id: EndpointId) -> bool {
        let state = self.0.lock().unwrap();
        Self::online_in(&state, id)
    }

    fn online_in(state: &ViewState, id: EndpointId) -> bool {
        state
            .heard
            .get(&id)
            .is_some_and(|(_, at)| at.elapsed() < state.offline_after)
    }

    /// Recomputes who is online and returns who came online and who went
    /// offline since the last call.
    pub fn update_online(&self) -> (Vec<EndpointId>, Vec<EndpointId>) {
        let mut state = self.0.lock().unwrap();
        let now: HashSet<EndpointId> = state
            .members
            .iter()
            .map(|s| s.record.member)
            .filter(|id| Self::online_in(&state, *id))
            .collect();
        let came: Vec<_> = now.difference(&state.online).copied().collect();
        let went: Vec<_> = state.online.difference(&now).copied().collect();
        state.online = now;
        (came, went)
    }

    /// The connection type between `a` and `b`. The newest report from either
    /// of them wins; reports from offline nodes are ignored.
    pub fn link(&self, a: EndpointId, b: EndpointId) -> LinkState {
        let state = self.0.lock().unwrap();
        let (a_online, b_online) = (Self::online_in(&state, a), Self::online_in(&state, b));
        let report = |from: EndpointId, to: EndpointId| {
            state.heard.get(&from).map(|(presence, at)| {
                let kind = presence
                    .links
                    .iter()
                    .find(|(id, _)| *id == to)
                    .map(|(_, kind)| *kind);
                (kind, *at)
            })
        };
        let reports: Vec<_> = [
            a_online.then(|| report(a, b)).flatten(),
            b_online.then(|| report(b, a)).flatten(),
        ]
        .into_iter()
        .flatten()
        .collect();
        if !(a_online && b_online) {
            return LinkState::Unknown;
        }
        reports
            .iter()
            .filter_map(|(kind, at)| kind.map(|kind| (kind, *at)))
            .max_by_key(|(_, at)| *at)
            .map(|(kind, _)| kind.into())
            .unwrap_or(LinkState::NotConnected)
    }
}
