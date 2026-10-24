//! A running node: an iroh endpoint with the blobs, gossip and docs protocols,
//! gated to group members, plus the join and members protocols.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{bail, Context};
use iroh::address_lookup::memory::MemoryLookup;
use iroh::endpoint::presets;
use iroh::protocol::Router;
use iroh::{Endpoint, EndpointAddr, EndpointId, SecretKey};
use iroh_blobs::store::fs::FsStore;
use iroh_blobs::BlobsProtocol;
use iroh_docs::engine::LiveEvent;
use iroh_docs::protocol::Docs;
use iroh_docs::store::Query;
use iroh_docs::NamespaceId;
use iroh_gossip::net::Gossip;
use iroh_gossip::proto::HyparviewConfig;
use n0_future::StreamExt;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;

use crate::folder::{FolderState, Folders, ViewFile};
use crate::gate::{Gate, GateHooks, MembersProtocol, JOIN_ALPN, MEMBERS_ALPN};
use crate::group::{Group, GroupError};
use crate::index::{self, FileMeta, DEBOUNCE};
use crate::invite::{self, InviteTicket};
use crate::join::{self, JoinProtocol, JoinReply, Ticket};
use crate::member::{self, Names};
use crate::monitor::Monitor;
use crate::peer_wire::{self, PeerWireProtocol, Transfers};
use crate::peers::{PeerSnapshot, Peers};
use crate::presence::{View, PRESENCE_INTERVAL};
use crate::record::{
    GroupId, Record, Removal, SignedRecord, SignedRemoval, MEMBERS_PREFIX, REMOVED_PREFIX,
};
use crate::status::Status;
use crate::view::FolderView;

/// Live gossip connections per node (HyParView active view).
pub const ACTIVE_VIEW: usize = 8;
/// Members known but not connected (HyParView passive view).
pub const PASSIVE_VIEW: usize = 125;
/// How long `join` waits for the first sync of the membership doc.
pub const FIRST_SYNC_TIMEOUT: Duration = Duration::from_secs(30);

/// How the endpoint finds and reaches peers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Network {
    /// n0's relays and DNS address lookup.
    N0,
    /// No relays or address lookup: peers are dialled by their direct
    /// addresses only. For tests on one machine.
    Local,
}

/// How a node runs.
#[derive(Debug, Clone)]
pub struct Config {
    pub network: Network,
    /// How often the node gossips its presence, re-reads the membership doc
    /// and rewrites the status file. A member is offline after four of these
    /// without a word.
    pub presence_interval: Duration,
    /// How long a changed path in a folder must be quiet before it is
    /// indexed.
    pub debounce: Duration,
    /// For tests: an address book shared by every node in the test, which
    /// each node adds itself to when it starts, as n0's DNS does outside
    /// tests. Lets a node reach another by ID alone, whenever it started.
    pub address_book: Option<MemoryLookup>,
    /// `--direct-only`: file contents move only over direct paths, never
    /// through a relay (for testing; see `plans/bittorrent/expectations.md`).
    pub direct_only: bool,
}

impl Config {
    pub fn new(network: Network) -> Self {
        Self {
            network,
            presence_interval: PRESENCE_INTERVAL,
            debounce: DEBOUNCE,
            address_book: None,
            direct_only: false,
        }
    }
}

pub struct Node {
    config_dir: PathBuf,
    config: Config,
    gate: Gate,
    peers: Peers,
    view: View,
    names: Names,
    folders: Folders,
    /// The peer-wire server, for its choker's state.
    wire: PeerWireProtocol,
    monitor: Mutex<Option<JoinHandle<()>>>,
    router: Router,
    blobs: FsStore,
    gossip: Gossip,
    docs: Docs,
}

impl Node {
    /// Starts a node with the default [`Config`] for `network`.
    pub async fn spawn(
        config_dir: &Path,
        secret_key: SecretKey,
        network: Network,
    ) -> anyhow::Result<Self> {
        Self::spawn_with(config_dir, secret_key, Config::new(network)).await
    }

    /// Starts a node keeping its stores under `config_dir`. If the node is in
    /// a group, it resumes syncing the membership doc with the members it
    /// knows, and starts following the group (see `monitor`).
    pub async fn spawn_with(
        config_dir: &Path,
        secret_key: SecretKey,
        config: Config,
    ) -> anyhow::Result<Self> {
        let builder = match config.network {
            Network::N0 => Endpoint::builder(presets::N0),
            Network::Local => Endpoint::builder(presets::Minimal),
        };
        let names = Names::default();
        let gate = Gate::new(names.clone());
        let peers = Peers::new(names.clone());
        if let Some(group) = Group::load(config_dir)? {
            group
                .check(secret_key.public())
                .map_err(GroupError::Chain)?;
            gate.set_group(group);
        }
        let endpoint = builder
            .secret_key(secret_key.clone())
            .hooks(GateHooks {
                gate: gate.clone(),
                peers: peers.clone(),
            })
            .bind()
            .await
            .context("binding endpoint")?;
        gate.set_endpoint(Some(endpoint.clone()));
        if let Some(book) = &config.address_book {
            endpoint.address_lookup()?.add(book.clone());
            book.add_endpoint_info(endpoint.addr());
        }

        let blobs = FsStore::load(store_dir(config_dir, "blobs")?)
            .await
            .context("opening blob store")?;
        let gossip = Gossip::builder()
            .membership_config(HyparviewConfig {
                active_view_capacity: ACTIVE_VIEW,
                passive_view_capacity: PASSIVE_VIEW,
                ..Default::default()
            })
            .spawn(endpoint.clone());
        let docs = Docs::persistent(store_dir(config_dir, "docs")?)
            .spawn(endpoint.clone(), (*blobs).clone(), gossip.clone())
            .await
            .context("opening docs store")?;

        let view = View::new(config.presence_interval);
        let folders = Folders::new(
            config_dir,
            docs.clone(),
            blobs.clone(),
            &endpoint,
            config.debounce,
            config.direct_only,
            names.clone(),
            view.clone(),
        );
        let wire = PeerWireProtocol::new(folders.clone(), config.direct_only);
        let router = Router::builder(endpoint)
            .accept(iroh_blobs::ALPN, BlobsProtocol::new(&blobs, None))
            .accept(peer_wire::ALPN, wire.clone())
            .accept(iroh_gossip::ALPN, gossip.clone())
            .accept(iroh_docs::ALPN, docs.clone())
            .accept(MEMBERS_ALPN, MembersProtocol(gate.clone()))
            .accept(
                JOIN_ALPN,
                JoinProtocol {
                    config_dir: config_dir.to_owned(),
                    secret_key,
                    gate: gate.clone(),
                    docs: docs.clone(),
                    blobs: blobs.clone(),
                    names: names.clone(),
                    one_at_a_time: Arc::new(Semaphore::new(1)),
                },
            )
            .spawn();

        let node = Self {
            config_dir: config_dir.to_owned(),
            gate,
            peers,
            view,
            names,
            folders,
            wire,
            config,
            monitor: Mutex::new(None),
            router,
            blobs,
            gossip,
            docs,
        };
        if let Some(group) = node.group() {
            node.resume_membership_sync(&group).await?;
            if let Err(err) = node.folders.check(group.id).await {
                tracing::warn!("starting folders: {err:#}");
            }
            node.start_monitor(group);
        }
        Ok(node)
    }

    /// Founds a new group with this node as its only member.
    pub async fn create_group(&self) -> anyhow::Result<GroupId> {
        if let Some(group) = self.group() {
            bail!(GroupError::AlreadyInGroup(group.id));
        }
        let doc = self
            .docs
            .create()
            .await
            .context("creating membership doc")?;
        // Our own record, self-signed, founds the group.
        let key = self.endpoint().secret_key();
        let record = member::write_record(&self.docs, doc.id(), key, self.id()).await?;
        let group = Group::founded(record);
        group.save_new(&self.config_dir)?;
        self.gate.set_group(group.clone());
        self.start_monitor(group);
        Ok(doc.id())
    }

    /// Makes a one-time invite to this node's group, valid for `ttl`, that
    /// names the new member `name`. The ticket carries this node's current
    /// address.
    pub fn create_invite(&self, ttl: Duration, name: Option<&str>) -> anyhow::Result<InviteTicket> {
        let group = self.group().ok_or(GroupError::NotInGroup)?;
        let name = name.map(member::check_name).transpose()?;
        Ok(invite::create(
            &self.config_dir,
            self.addr(),
            group.id,
            ttl,
            name.as_deref(),
        )?)
    }

    /// Joins a group through `ticket`'s member, by redeeming an invite or by
    /// fetching the record a member wrote for this node's ID, then starts
    /// syncing the membership doc. Joining by member ID takes whichever group
    /// that member is in.
    pub async fn join(&self, ticket: impl Into<Ticket>) -> anyhow::Result<GroupId> {
        let ticket = ticket.into();
        if let Some(group) = self.group() {
            bail!(GroupError::AlreadyInGroup(group.id));
        }
        let (chain, doc) = match join::request(self.endpoint(), &ticket).await? {
            JoinReply::Welcome { chain, doc } => (chain, doc),
            JoinReply::Refused(reason) => bail!("join refused: {reason}"),
        };
        let group = Group::from_chain(self.id(), chain).context("checking our new record")?;
        if let Ticket::Invite(invite) = &ticket {
            if group.id != invite.group {
                bail!("inviter put us in group {}, not {}", group.id, invite.group);
            }
        }
        group.save_new(&self.config_dir)?;
        self.gate.set_group(group.clone());
        let (_doc, mut events) = self
            .docs
            .import_and_subscribe(*doc)
            .await
            .context("importing membership doc")?;
        // Wait for the first sync and its content, so the doc is filled in and
        // a one-shot `join` doesn't cut the sync off by exiting.
        let synced = tokio::time::timeout(FIRST_SYNC_TIMEOUT, async {
            while let Some(event) = events.next().await {
                if let LiveEvent::PendingContentReady = event? {
                    return anyhow::Ok(());
                }
            }
            bail!("membership doc events ended")
        })
        .await;
        match synced {
            Ok(result) => result.context("first membership sync")?,
            Err(_) => tracing::warn!("first membership sync didn't finish; `run` will retry it"),
        }
        let id = group.id;
        self.start_monitor(group);
        Ok(id)
    }

    /// This node's group, if it is in one.
    pub fn group(&self) -> Option<Group> {
        self.gate.group()
    }

    /// The valid records in this node's copy of the membership doc, one per
    /// member. Entries that don't parse, or whose chain doesn't reach the
    /// founder, are left out.
    pub async fn members(&self) -> anyhow::Result<Vec<SignedRecord>> {
        let group = self.group().ok_or(GroupError::NotInGroup)?;
        read_members(&self.docs, &self.blobs, &group).await
    }

    /// The members and the removals that count, in this node's copy of the
    /// membership doc.
    pub async fn membership(&self) -> anyhow::Result<Membership> {
        let group = self.group().ok_or(GroupError::NotInGroup)?;
        read_membership(&self.docs, &self.blobs, &group).await
    }

    /// The group's name, as of the last presence interval.
    pub fn group_name(&self) -> Option<String> {
        self.names.group()
    }

    /// How to show `id`: its name if it has one, otherwise its short ID. Names
    /// are re-read from the membership doc every presence interval.
    pub fn display(&self, id: EndpointId) -> String {
        self.names.display(id)
    }

    /// `id`'s name, as of the last presence interval.
    pub fn name(&self, id: EndpointId) -> Option<String> {
        self.names.get(id)
    }

    /// Bytes of file data this node has fetched from each node since it
    /// started: verified pieces only, each counted once. Unlike the traffic
    /// in [`peers`](Self::peers), no protocol overhead or other traffic.
    pub fn fetched(&self) -> HashMap<EndpointId, u64> {
        self.folders.fetched()
    }

    /// The nodes this node's choker has unchoked now: it uploads file data
    /// to them, over all swarms.
    pub fn unchoked(&self) -> Vec<EndpointId> {
        self.wire.unchoked()
    }

    /// The downloads running (swarms, pieces, peers) and the uploads per
    /// peer (choked or not, rate), for the web UI's Transfers view.
    pub fn transfers(&self) -> Transfers {
        self.wire.transfers()
    }

    /// This node's connections and traffic to each node it has connected
    /// to, member or not, whether or not its membership doc lists it yet.
    pub fn peers(&self) -> Vec<PeerSnapshot> {
        self.peers.snapshot()
    }

    /// The group as this node sees it: members, who is online, this node's
    /// connections, and each pair's connection type. Up to date as of the
    /// last presence interval.
    pub fn status(&self) -> Option<Status> {
        let group = self.group()?;
        Some(Status::build(
            self.id(),
            &group,
            &self.view,
            &self.peers.snapshot(),
            &self.names,
            &self.folders.list(),
            &self.folders.available(),
        ))
    }

    /// Creates a folder named `name` for the group, kept at `path` on this
    /// node, and starts indexing it. `folder add` queues the same thing for
    /// the running node.
    pub async fn add_folder(&self, name: &str, path: &Path) -> anyhow::Result<NamespaceId> {
        let group = self.group().ok_or(GroupError::NotInGroup)?;
        self.folders.add(group.id, name, path).await
    }

    /// The merged view of folder `id`: what it looks like once every node
    /// keeping it has synced.
    pub async fn folder_view(&self, id: NamespaceId) -> anyhow::Result<FolderView> {
        let group = self.group().ok_or(GroupError::NotInGroup)?;
        self.folders.view(group.id, id).await
    }

    /// The group's folders as this node's copy of the membership doc lists
    /// them: doc ID and name. Several may share a name until they settle.
    pub async fn group_folders(&self) -> anyhow::Result<Vec<(NamespaceId, String)>> {
        let group = self.group().ok_or(GroupError::NotInGroup)?;
        let listed = crate::folder::group_folders(&self.docs, &self.blobs, group.id).await?;
        Ok(listed.into_iter().map(|f| (f.id, f.name)).collect())
    }

    /// The folders this node is indexing.
    pub fn folders(&self) -> Vec<FolderState> {
        self.folders.list()
    }

    /// The view `folder show` prints for folder `name`, as this node last
    /// wrote it. `None` if there is none yet.
    pub fn view_file(&self, name: &str) -> anyhow::Result<Option<ViewFile>> {
        crate::folder::read_view(&self.config_dir, name)
    }

    /// This node's index of folder `id`, tombstones included, as stored in
    /// the folder's doc.
    pub async fn folder_index(&self, id: NamespaceId) -> anyhow::Result<HashMap<String, FileMeta>> {
        index::read_index(&self.docs, &self.blobs, id).await
    }

    /// Starts the monitor task (see `monitor`), once.
    fn start_monitor(&self, group: Group) {
        let mut slot = self.monitor.lock().unwrap();
        if slot.is_some() {
            return;
        }
        let monitor = Monitor {
            config_dir: self.config_dir.clone(),
            key: self.endpoint().secret_key().clone(),
            group,
            docs: self.docs.clone(),
            blobs: self.blobs.clone(),
            gossip: self.gossip.clone(),
            gate: self.gate.clone(),
            peers: self.peers.clone(),
            view: self.view.clone(),
            names: self.names.clone(),
            folders: self.folders.clone(),
            interval: self.config.presence_interval,
        };
        *slot = Some(tokio::spawn(monitor.run()));
    }

    /// Reopens the membership doc and syncs it with every member this node
    /// knows of, from its own chain and its copy of the doc.
    async fn resume_membership_sync(&self, group: &Group) -> anyhow::Result<()> {
        // A missing replica comes back as an error rather than `None`.
        let Ok(Some(doc)) = self.docs.open(group.id).await else {
            tracing::warn!("membership doc {} is missing", group.id);
            return Ok(());
        };
        let mut peers: Vec<EndpointId> = group
            .chain
            .iter()
            .map(|signed| signed.record.member)
            .chain(
                self.members()
                    .await?
                    .into_iter()
                    .map(|signed| signed.record.member),
            )
            .filter(|id| *id != self.id())
            .collect();
        peers.sort();
        peers.dedup();
        doc.start_sync(peers.into_iter().map(EndpointAddr::new).collect())
            .await?;
        Ok(())
    }

    pub fn id(&self) -> EndpointId {
        self.endpoint().id()
    }

    /// This node's current address: its relay and direct addresses.
    pub fn addr(&self) -> EndpointAddr {
        self.endpoint().addr()
    }

    pub fn endpoint(&self) -> &Endpoint {
        self.router.endpoint()
    }

    pub fn blobs(&self) -> &FsStore {
        &self.blobs
    }

    pub fn gossip(&self) -> &Gossip {
        &self.gossip
    }

    pub fn docs(&self) -> &Docs {
        &self.docs
    }

    /// Stops accepting connections and shuts the protocols down. The blobs
    /// protocol closes the blob store as part of this.
    pub async fn shutdown(self) -> anyhow::Result<()> {
        if let Some(monitor) = self.monitor.lock().unwrap().take() {
            monitor.abort();
        }
        self.folders.stop_all();
        self.router.shutdown().await.context("stopping router")?;
        self.gate.set_endpoint(None);
        Ok(())
    }
}

/// The membership doc as a node reads it.
#[derive(Debug, Clone, Default)]
pub struct Membership {
    /// One valid record per member, oldest first.
    pub members: Vec<SignedRecord>,
    /// The removals that count, one per removed node, oldest first.
    pub removed: Vec<SignedRemoval>,
}

/// The valid records in a copy of the membership doc, one per member.
pub(crate) async fn read_members(
    docs: &Docs,
    blobs: &FsStore,
    group: &Group,
) -> anyhow::Result<Vec<SignedRecord>> {
    Ok(read_membership(docs, blobs, group).await?.members)
}

/// Reads the members and removals in a copy of the membership doc.
///
/// A removal counts if it is for this group, its signature checks out, it
/// doesn't remove the founder, and its signer has a chain to the founder.
/// The signer's own removal doesn't undo it: a removal always wins, and a
/// removed node can't come back. A node is a member if it isn't removed and
/// has a record signed by a member, from the founder out: so a member added
/// by a removed node is no longer one, unless another member adds it again.
pub(crate) async fn read_membership(
    docs: &Docs,
    blobs: &FsStore,
    group: &Group,
) -> anyhow::Result<Membership> {
    let doc = docs
        .open(group.id)
        .await?
        .context("membership doc missing")?;
    let mut records = Vec::new();
    for entry in doc
        .get_many(Query::key_prefix(MEMBERS_PREFIX))
        .await?
        .collect::<Vec<_>>()
        .await
    {
        let entry = entry?;
        let Ok(bytes) = blobs.get_bytes(entry.content_hash()).await else {
            continue; // Content not fetched yet.
        };
        if let Ok(signed) = SignedRecord::from_bytes(&bytes) {
            if entry.key() == Record::doc_key(signed.record.member).as_bytes() {
                records.push(signed);
            }
        }
    }
    let mut removals = Vec::new();
    for entry in doc
        .get_many(Query::key_prefix(REMOVED_PREFIX))
        .await?
        .collect::<Vec<_>>()
        .await
    {
        let entry = entry?;
        let Ok(bytes) = blobs.get_bytes(entry.content_hash()).await else {
            continue;
        };
        if let Ok(signed) = SignedRemoval::from_bytes(&bytes) {
            if entry.key() == Removal::doc_key(signed.removal.member).as_bytes() {
                removals.push(signed);
            }
        }
    }

    let chained = reachable(group, &records, &HashSet::new());
    let mut removed: HashMap<EndpointId, SignedRemoval> = HashMap::new();
    for signed in removals {
        let removal = &signed.removal;
        if removal.group == group.id
            && removal.member != group.founder
            && chained.contains_key(&removal.removed_by)
            && signed.verify_signature()
        {
            let first = removed
                .entry(removal.member)
                .or_insert_with(|| signed.clone());
            if signed.removal.removed_at < first.removal.removed_at {
                *first = signed;
            }
        }
    }
    let removed_ids = removed.keys().copied().collect();
    let mut members: Vec<_> = reachable(group, &records, &removed_ids)
        .into_values()
        .collect();
    members.sort_by_key(|signed| signed.record.added_at);
    let mut removed: Vec<_> = removed.into_values().collect();
    removed.sort_by_key(|signed| signed.removal.removed_at);
    Ok(Membership { members, removed })
}

/// The nodes not in `removed` that `records` link to the founder, from the
/// founder out, each with the record that links it.
fn reachable(
    group: &Group,
    records: &[SignedRecord],
    removed: &HashSet<EndpointId>,
) -> HashMap<EndpointId, SignedRecord> {
    let mut members: HashMap<EndpointId, SignedRecord> = HashMap::new();
    loop {
        let before = members.len();
        for signed in records {
            let record = &signed.record;
            if record.group != group.id
                || members.contains_key(&record.member)
                || removed.contains(&record.member)
            {
                continue;
            }
            let linked = if record.added_by == record.member {
                record.member == group.founder
            } else {
                members.contains_key(&record.added_by)
            };
            if linked && signed.verify_signature() {
                members.insert(record.member, signed.clone());
            }
        }
        if members.len() == before {
            return members;
        }
    }
}

fn store_dir(config_dir: &Path, name: &str) -> anyhow::Result<PathBuf> {
    let dir = config_dir.join(name);
    std::fs::create_dir_all(&dir).with_context(|| format!("creating {}", dir.display()))?;
    Ok(dir)
}
