//! A running node: an iroh endpoint with the blobs, gossip and docs protocols,
//! gated to group members, plus the join and members protocols.

use std::collections::HashMap;
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

use crate::blocks::{self, BlocksProtocol};
use crate::folder::{FolderState, Folders};
use crate::gate::{Gate, GateHooks, MembersProtocol, JOIN_ALPN, MEMBERS_ALPN};
use crate::group::{Group, GroupError};
use crate::index::{self, FileMeta, DEBOUNCE};
use crate::invite::{self, InviteTicket};
use crate::join::{self, JoinProtocol, JoinReply, Ticket};
use crate::member::{self, Names};
use crate::monitor::Monitor;
use crate::peers::{PeerSnapshot, Peers};
use crate::presence::{View, PRESENCE_INTERVAL};
use crate::record::{validate_chain, GroupId, Record, SignedRecord, MEMBERS_PREFIX};
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
}

impl Config {
    pub fn new(network: Network) -> Self {
        Self {
            network,
            presence_interval: PRESENCE_INTERVAL,
            debounce: DEBOUNCE,
            address_book: None,
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

        let folders = Folders::new(
            config_dir,
            docs.clone(),
            blobs.clone(),
            &endpoint,
            config.debounce,
            names.clone(),
        );
        let router = Router::builder(endpoint)
            .accept(iroh_blobs::ALPN, BlobsProtocol::new(&blobs, None))
            .accept(blocks::ALPN, BlocksProtocol::new(folders.clone()))
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
            view: View::new(config.presence_interval),
            names,
            folders,
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
    /// started: verified blocks only, each counted once. Unlike the traffic
    /// in [`peers`](Self::peers), no protocol overhead or other traffic.
    pub fn fetched(&self) -> HashMap<EndpointId, u64> {
        self.folders.fetched()
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

/// The valid records in a copy of the membership doc, one per member.
pub(crate) async fn read_members(
    docs: &Docs,
    blobs: &FsStore,
    group: &Group,
) -> anyhow::Result<Vec<SignedRecord>> {
    let doc = docs
        .open(group.id)
        .await?
        .context("membership doc missing")?;
    let entries = doc
        .get_many(Query::key_prefix(MEMBERS_PREFIX))
        .await?
        .collect::<Vec<_>>()
        .await;
    let mut records = Vec::new();
    for entry in entries {
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
    let mut valid = HashMap::new();
    for signed in &records {
        let member = signed.record.member;
        let candidates = records
            .iter()
            .filter(|other| other.record.member != member)
            .chain([signed]);
        if validate_chain(group.id, group.founder, member, candidates).is_ok() {
            valid.entry(member).or_insert_with(|| signed.clone());
        }
    }
    let mut valid: Vec<_> = valid.into_values().collect();
    valid.sort_by_key(|signed| signed.record.added_at);
    Ok(valid)
}

fn store_dir(config_dir: &Path, name: &str) -> anyhow::Result<PathBuf> {
    let dir = config_dir.join(name);
    std::fs::create_dir_all(&dir).with_context(|| format!("creating {}", dir.display()))?;
    Ok(dir)
}
