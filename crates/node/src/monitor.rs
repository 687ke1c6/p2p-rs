//! The running node's view of its group, kept up to date once per presence
//! interval: apply adds queued by `member add`, re-read the membership doc
//! and log new members and renames, gossip this
//! node's presence, log members coming online or going offline, and rewrite
//! the status file.

use std::collections::HashSet;
use std::path::PathBuf;
use std::time::Duration;

use bytes::Bytes;
use iroh::{EndpointAddr, EndpointId, SecretKey};
use iroh_blobs::store::fs::FsStore;
use iroh_docs::protocol::Docs;
use iroh_gossip::api::{Event, GossipSender};
use iroh_gossip::net::Gossip;
use n0_future::StreamExt;

use crate::folder::Folders;
use crate::gate::Gate;
use crate::group::Group;
use crate::member::{self, Names, Rename};
use crate::node::{read_members, ACTIVE_VIEW};
use crate::peers::Peers;
use crate::presence::{topic, Presence, SignedPresence, View};
use crate::record::{unix_now, SignedRecord};
use crate::status::{self, Status};

pub(crate) struct Monitor {
    pub config_dir: PathBuf,
    pub key: SecretKey,
    pub group: Group,
    pub docs: Docs,
    pub blobs: FsStore,
    pub gossip: Gossip,
    pub gate: Gate,
    pub peers: Peers,
    pub view: View,
    pub names: Names,
    pub folders: Folders,
    pub interval: Duration,
}

impl Monitor {
    fn me(&self) -> EndpointId {
        self.key.public()
    }

    pub async fn run(self) {
        let members = read_members(&self.docs, &self.blobs, &self.group)
            .await
            .unwrap_or_default();
        self.view.set_members(members.clone());
        self.refresh_names(&members, false).await;
        tracing::info!(
            "group {} founded by {}; {} members: {}",
            self.group.id.fmt_short(),
            self.names.display(self.group.founder),
            members.len(),
            self.list(&members)
        );

        let bootstrap = self.others();
        let topic = match self.gossip.subscribe(topic(self.group.id), bootstrap).await {
            Ok(topic) => topic,
            Err(err) => {
                tracing::warn!("can't join the presence topic: {err:#}");
                return;
            }
        };
        let (sender, mut receiver) = topic.split();
        let mut neighbours = HashSet::new();
        let mut tick = tokio::time::interval(self.interval);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = tick.tick() => self.on_tick(&sender, &neighbours).await,
                event = receiver.next() => match event {
                    Some(Ok(event)) => self.on_gossip(event, &mut neighbours),
                    Some(Err(err)) => tracing::warn!("presence topic: {err:#}"),
                    None => {
                        tracing::warn!("presence topic closed");
                        break;
                    }
                },
            }
        }
    }

    /// Members other than this node.
    fn others(&self) -> Vec<EndpointId> {
        self.view
            .members()
            .iter()
            .map(|s| s.record.member)
            .filter(|id| *id != self.me())
            .collect()
    }

    /// The nodes to fetch missing membership records from: the members this
    /// node can read, and its chain up to the founder, which it holds
    /// itself (with no member readable, it would have no one to ask).
    fn record_sources(&self) -> Vec<EndpointId> {
        let mut ids = self.others();
        ids.extend(
            self.group
                .chain
                .iter()
                .map(|signed| signed.record.member)
                .filter(|id| *id != self.me()),
        );
        ids.sort();
        ids.dedup();
        ids
    }

    async fn on_tick(&self, sender: &GossipSender, neighbours: &HashSet<EndpointId>) {
        self.apply_queued_adds().await;
        // Records whose content didn't arrive with them (members, folders)
        // are unreadable until it does.
        if let Err(err) = self
            .folders
            .fetch_missing(self.group.id, self.record_sources())
            .await
        {
            tracing::debug!("fetching membership records: {err:#}");
        }
        self.folders.apply_queued(self.group.id).await;
        if let Err(err) = self.folders.check(self.group.id).await {
            tracing::warn!("checking folders: {err:#}");
        }
        self.folders.write_views(self.group.id, &self.names).await;
        match read_members(&self.docs, &self.blobs, &self.group).await {
            Ok(members) => {
                let added = self.view.set_members(members.clone());
                self.refresh_names(&members, true).await;
                for added in added {
                    tracing::info!(
                        "member added: {} (by {})",
                        self.names.display(added.record.member),
                        self.names.display(added.record.added_by)
                    );
                }
            }
            Err(err) => tracing::warn!("reading the membership doc: {err:#}"),
        }

        // Keep trying to reach members we have no gossip link to, up to the
        // active view size: they may have come online, or just joined.
        if neighbours.len() < ACTIVE_VIEW {
            let missing: Vec<_> = self
                .others()
                .into_iter()
                .filter(|id| !neighbours.contains(id))
                .collect();
            if !missing.is_empty() {
                if let Err(err) = sender.join_peers(missing).await {
                    tracing::debug!("joining presence peers: {err:#}");
                }
            }
        }

        let links = self
            .peers
            .snapshot()
            .into_iter()
            .filter(|peer| peer.connections > 0 && self.view.is_member(peer.id))
            .filter_map(|peer| peer.kind.map(|kind| (peer.id, kind)))
            .collect();
        let presence = Presence {
            group: self.group.id,
            node: self.me(),
            sent_at_ms: unix_now_ms(),
            links,
        };
        self.view.heard(presence.clone());
        let signed = presence.sign(&self.key);
        let bytes = postcard::to_stdvec(&signed).expect("presence serializes");
        if let Err(err) = sender.broadcast(Bytes::from(bytes)).await {
            tracing::debug!("broadcasting presence: {err:#}");
        }

        let (came, went) = self.view.update_online();
        for id in came.into_iter().filter(|id| *id != self.me()) {
            tracing::info!("{} is online", self.names.display(id));
            self.resync_with(id).await;
        }
        for id in went {
            tracing::info!("{} went offline", self.names.display(id));
        }

        let status = self.status();
        if let Err(err) = status::write(&self.config_dir, &status) {
            tracing::warn!("writing the status file: {err:#}");
        }
    }

    fn on_gossip(&self, event: Event, neighbours: &mut HashSet<EndpointId>) {
        match event {
            Event::NeighborUp(id) => {
                neighbours.insert(id);
                tracing::debug!("presence neighbour up: {}", id.fmt_short());
            }
            Event::NeighborDown(id) => {
                neighbours.remove(&id);
                tracing::debug!("presence neighbour down: {}", id.fmt_short());
            }
            Event::Received(message) => {
                let Ok(signed) = postcard::from_bytes::<SignedPresence>(&message.content) else {
                    tracing::debug!(
                        "unreadable presence from {}",
                        message.delivered_from.fmt_short()
                    );
                    return;
                };
                let node = signed.presence.node;
                let known = self.view.is_member(node) || self.gate.is_admitted(node);
                if !signed.verify() || signed.presence.group != self.group.id || !known {
                    tracing::debug!(
                        "dropped presence claiming {} from {}",
                        node.fmt_short(),
                        message.delivered_from.fmt_short()
                    );
                    return;
                }
                if node != self.me() {
                    self.view.heard(signed.presence);
                }
            }
            Event::Lagged => tracing::debug!("presence topic lagged"),
        }
    }

    /// Syncs the membership doc and every folder doc shared with `id` with
    /// it right away. Docs only syncs directly with a peer when it starts
    /// syncing a doc, so a first attempt that failed (the peer was offline,
    /// or its address unknown) would otherwise wait for some later change.
    async fn resync_with(&self, id: EndpointId) {
        let peer = vec![EndpointAddr::new(id)];
        match self.docs.open(self.group.id).await {
            Ok(Some(doc)) => {
                if let Err(err) = doc.start_sync(peer).await {
                    tracing::debug!(
                        "syncing the membership doc with {}: {err:#}",
                        id.fmt_short()
                    );
                }
            }
            _ => tracing::debug!("membership doc missing"),
        }
        self.folders.resync_with(id).await;
    }

    /// Applies the adds `member add` queued in `adds/`: adds each node that
    /// isn't a member yet, then sets its name if one was given and no other
    /// member has it. A file that can't be used is logged and removed; one
    /// that fails to apply is kept for the next interval.
    async fn apply_queued_adds(&self) {
        let queued = match member::queued_adds(&self.config_dir) {
            Ok(queued) => queued,
            Err(err) => {
                tracing::warn!("reading queued adds: {err:#}");
                return;
            }
        };
        for queued in queued {
            let done = match queued.add {
                Ok((id, name)) => match self.apply_add(id, name.as_deref()).await {
                    Ok(()) => true,
                    Err(err) => {
                        tracing::warn!("adding {}: {err:#}", id.fmt_short());
                        false
                    }
                },
                Err(why) => {
                    tracing::warn!("ignoring queued add {}: {why}", queued.path.display());
                    true
                }
            };
            if done {
                if let Err(err) = std::fs::remove_file(&queued.path) {
                    tracing::warn!("removing {}: {err}", queued.path.display());
                }
            }
        }
    }

    async fn apply_add(&self, id: EndpointId, name: Option<&str>) -> anyhow::Result<()> {
        if !self.view.is_member(id) {
            member::write_record(&self.docs, self.group.id, &self.key, id).await?;
            self.gate.admit(id, "added by ID");
        }
        let Some(name) = name else { return Ok(()) };
        match self.names.holder(name) {
            Some(holder) if holder == id => {}
            Some(holder) => tracing::warn!(
                "not naming {} {name:?}: {} already has that name",
                id.fmt_short(),
                holder.fmt_short()
            ),
            None => member::write_name(&self.docs, self.group.id, id, name).await?,
        }
        Ok(())
    }

    /// Re-reads members' names, logging changes if `log` is set.
    async fn refresh_names(&self, members: &[SignedRecord], log: bool) {
        let names = match member::read_names(&self.docs, &self.blobs, self.group.id, members).await
        {
            Ok(names) => names,
            Err(err) => {
                tracing::warn!("reading names: {err:#}");
                return;
            }
        };
        let renames = self.names.replace(names);
        if !log {
            return;
        }
        for Rename { id, old, new } in renames {
            let short = id.fmt_short();
            match (old, new) {
                (None, Some(new)) => tracing::info!("{short} is now named {new}"),
                (Some(old), Some(new)) => tracing::info!("{old} is now {new} ({short})"),
                (Some(old), None) => tracing::info!("{old} ({short}) no longer has a name"),
                (None, None) => {}
            }
        }
    }

    fn status(&self) -> Status {
        Status::build(
            self.me(),
            &self.group,
            &self.view,
            &self.peers.snapshot(),
            &self.names,
            &self.folders.list(),
        )
    }

    fn list(&self, members: &[SignedRecord]) -> String {
        members
            .iter()
            .map(|s| self.names.display(s.record.member))
            .collect::<Vec<_>>()
            .join(", ")
    }
}

fn unix_now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or_else(|_| unix_now() * 1000)
}
