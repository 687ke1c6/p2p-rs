//! The web UI's server: a JSON API under `/api` and the built page from the UI
//! dir. Part of `sync-node run` (ADR 0004); see `plans/ui/spec.md`.
//!
//! The handlers take a [`UiBackend`], not a [`Node`], so the tests drive the
//! router with a fake through `oneshot`: no node, no port.

pub mod actions;
pub mod topics;

use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

use axum::extract::ws::rejection::WebSocketUpgradeRejection;
use axum::extract::ws::{
    close_code, CloseFrame, Message as WsMessage, WebSocket, WebSocketUpgrade,
};
use axum::extract::{Path, Request, State};
use axum::http::{header, HeaderMap, Method, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use iroh::EndpointId;
use serde::Serialize;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tower::ServiceExt as _;
use tower_http::services::{ServeDir, ServeFile};

use crate::folder::{FolderState, ViewFile};
use crate::group::GroupError;
use crate::join::Ticket;
use crate::node::Node;
use crate::peer_wire::{SwarmPeerState, SwarmSnapshot, Transfers, UploadSnapshot};
use crate::peers::{LinkKind, PeerSnapshot, Traffic};
use crate::presence::LinkState;
use crate::record::GroupId;
use crate::status::{Link, MemberStatus, Status};
use crate::view::{NodeState, Outcome, PathView};
use topics::{ClientMessage, Topics, TOPIC_INTERVAL};

/// What the handlers need from the running node. The node implements it; the
/// tests implement a fake.
pub trait UiBackend: Send + Sync + 'static {
    fn id(&self) -> EndpointId;
    /// The group as this node sees it, or `None` in no group.
    fn status(&self) -> Option<Status>;
    /// The folders this node keeps.
    fn folders(&self) -> Vec<FolderState>;
    /// The view `folder show` prints for folder `name`, or `None` if there is
    /// none yet.
    fn folder(&self, name: &str) -> anyhow::Result<Option<ViewFile>>;
    /// The downloads and uploads running now.
    fn transfers(&self) -> Transfers;
    /// How to show node `id`: its name, or its short ID.
    fn label(&self, id: EndpointId) -> String;
    /// Founds a new group with this node as its only member.
    /// Founds a group, named `name` if given (already checked).
    fn create_group(&self, name: Option<String>) -> BoxFuture<'_, anyhow::Result<GroupId>>;
    /// Joins a group through `ticket`: the group and its founder.
    fn join(&self, ticket: Ticket) -> BoxFuture<'_, anyhow::Result<(GroupId, EndpointId)>>;
}

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

impl UiBackend for Node {
    fn id(&self) -> EndpointId {
        Node::id(self)
    }

    fn status(&self) -> Option<Status> {
        Node::status(self)
    }

    fn folders(&self) -> Vec<FolderState> {
        Node::folders(self)
    }

    fn folder(&self, name: &str) -> anyhow::Result<Option<ViewFile>> {
        Node::view_file(self, name)
    }

    fn transfers(&self) -> Transfers {
        Node::transfers(self)
    }

    fn label(&self, id: EndpointId) -> String {
        Node::display(self, id)
    }

    fn create_group(&self, name: Option<String>) -> BoxFuture<'_, anyhow::Result<GroupId>> {
        Box::pin(async move { Node::create_named_group(self, name.as_deref()).await })
    }

    fn join(&self, ticket: Ticket) -> BoxFuture<'_, anyhow::Result<(GroupId, EndpointId)>> {
        Box::pin(async move {
            let group = Node::join(self, ticket).await?;
            let founder = self.group().ok_or(GroupError::NotInGroup)?.founder;
            Ok((group, founder))
        })
    }
}

/// How the server runs.
#[derive(Debug, Clone)]
pub struct UiConfig {
    /// The UI address. A request whose `Host` isn't this address (or
    /// `localhost` with its port, for a loopback address) is refused.
    pub addr: SocketAddr,
    /// The built page.
    pub ui_dir: PathBuf,
    /// The node's config dir, for invites and the queues.
    pub config_dir: PathBuf,
}

impl UiConfig {
    /// The `ui/` directory next to the executable.
    pub fn default_ui_dir() -> std::io::Result<PathBuf> {
        let exe = std::env::current_exe()?;
        let dir = exe.parent().unwrap_or(&exe);
        Ok(dir.join("ui"))
    }
}

#[derive(Clone)]
struct AppState {
    backend: Arc<dyn UiBackend>,
    config: Arc<UiConfig>,
    topics: Topics,
}

/// The whole server: the API, the page, and the checks in front of both.
/// Before the server stops, [`Topics::stop`] ends every WebSocket, which
/// graceful shutdown would otherwise wait for.
pub fn router(backend: Arc<dyn UiBackend>, config: UiConfig) -> (Router, Topics) {
    let topics = Topics::new(backend.clone(), TOPIC_INTERVAL);
    let state = AppState {
        backend,
        config: Arc::new(config),
        topics: topics.clone(),
    };
    let api = Router::new()
        .route("/status", get(status))
        .route("/group", post(actions::create_group))
        .route("/join", post(actions::join))
        .route("/invites", post(actions::create_invite))
        .route("/group/name", post(actions::name_group))
        .route("/members", post(actions::add_member))
        .route("/members/remove", post(actions::remove_member))
        .route("/folders", get(folders).post(actions::add_folder))
        .route("/folders/{name}", get(folder))
        .route("/folders/{name}/enabled", post(actions::set_folder_enabled))
        .route("/folders/{name}/remove", post(actions::remove_folder))
        .route("/transfers", get(transfers))
        .route("/ws", get(socket))
        .fallback(|| async { ApiError::new(StatusCode::NOT_FOUND, "no such API route") });
    let router = Router::new()
        .nest("/api", api)
        .fallback(page)
        .layer(middleware::from_fn_with_state(state.clone(), checks))
        .with_state(state);
    (router, topics)
}

/// Refuses an unknown `Host` (403, against DNS rebinding) and a `POST`
/// without a JSON body (415, so a plain HTML form can't send one).
async fn checks(State(state): State<AppState>, request: Request, next: Next) -> Response {
    let host = request
        .headers()
        .get(header::HOST)
        .and_then(|host| host.to_str().ok());
    if !host.is_some_and(|host| host_allowed(state.config.addr, host)) {
        return ApiError::new(StatusCode::FORBIDDEN, "unknown Host").into_response();
    }
    if request.method() == Method::POST {
        let json = request
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|kind| kind.to_str().ok())
            .and_then(|kind| kind.split(';').next())
            .is_some_and(|kind| kind.trim().eq_ignore_ascii_case("application/json"));
        if !json {
            return ApiError::new(
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "Content-Type must be application/json",
            )
            .into_response();
        }
    }
    next.run(request).await
}

fn host_allowed(addr: SocketAddr, host: &str) -> bool {
    host.eq_ignore_ascii_case(&addr.to_string())
        || (addr.ip().is_loopback()
            && host.eq_ignore_ascii_case(&format!("localhost:{}", addr.port())))
}

fn origin_allowed(addr: SocketAddr, origin: &str) -> bool {
    origin.eq_ignore_ascii_case(&format!("http://{addr}"))
        || (addr.ip().is_loopback()
            && origin.eq_ignore_ascii_case(&format!("http://localhost:{}", addr.port())))
}

/// `GET /api/ws`: the page's WebSocket for the UI topics. A WebSocket isn't
/// covered by CORS, so only a page from the UI address may open one (403).
async fn socket(
    State(state): State<AppState>,
    headers: HeaderMap,
    upgrade: Result<WebSocketUpgrade, WebSocketUpgradeRejection>,
) -> Response {
    let origin = headers
        .get(header::ORIGIN)
        .and_then(|origin| origin.to_str().ok());
    if !origin.is_some_and(|origin| origin_allowed(state.config.addr, origin)) {
        return ApiError::new(StatusCode::FORBIDDEN, "unknown Origin").into_response();
    }
    match upgrade {
        Ok(upgrade) => {
            let topics = state.topics;
            upgrade.on_upgrade(move |socket| topics.track(socket_loop(socket, topics.clone())))
        }
        Err(rejection) => rejection.into_response(),
    }
}

/// Moves messages between one WebSocket and the UI topics, until either
/// side closes or the node stops.
async fn socket_loop(mut socket: WebSocket, topics: Topics) {
    let (out, mut outbox) = mpsc::channel::<topics::Message>(8);
    // One task per topic, forwarding its messages to `out`, and how many
    // subscribes it has had that no unsubscribe has matched yet: a page can
    // subscribe to a topic twice (a view mounting before the one it replaces
    // goes), and its first unsubscribe must not end the other's.
    let mut subscriptions: HashMap<String, (JoinHandle<()>, usize)> = HashMap::new();
    loop {
        tokio::select! {
            _ = topics.stopped() => {
                let close = CloseFrame {
                    code: close_code::AWAY,
                    reason: "the node is stopping".into(),
                };
                let _ = socket.send(WsMessage::Close(Some(close))).await;
                break;
            }
            Some(text) = outbox.recv() => {
                if socket.send(WsMessage::Text(text.as_ref().into())).await.is_err() {
                    break;
                }
            }
            received = socket.recv() => match received {
                Some(Ok(WsMessage::Text(text))) => match serde_json::from_str(&text) {
                    Ok(ClientMessage::Subscribe(name)) => {
                        if let Some((_, count)) = subscriptions.get_mut(&name) {
                            *count += 1;
                            continue;
                        }
                        match topics.subscribe(&name) {
                            Ok(mut subscription) => {
                                let out = out.clone();
                                let task = tokio::spawn(async move {
                                    while let Some(text) = subscription.next().await {
                                        if out.send(text).await.is_err() {
                                            break;
                                        }
                                    }
                                });
                                subscriptions.insert(name, (task, 1));
                            }
                            Err(error) => {
                                if socket.send(WsMessage::Text(error.as_ref().into())).await.is_err() {
                                    break;
                                }
                            }
                        }
                    }
                    Ok(ClientMessage::Unsubscribe(name)) => {
                        if let Some((_, count)) = subscriptions.get_mut(&name) {
                            *count -= 1;
                            if *count == 0 {
                                if let Some((task, _)) = subscriptions.remove(&name) {
                                    task.abort();
                                }
                            }
                        }
                    }
                    Err(err) => tracing::warn!("web UI: bad WebSocket message: {err}"),
                },
                Some(Ok(WsMessage::Close(_))) | Some(Err(_)) | None => break,
                Some(Ok(_)) => {}
            },
        }
    }
    for (task, _) in subscriptions.into_values() {
        task.abort();
    }
}

/// A file from the UI dir; any other path gets `index.html`, since the page
/// routes in the browser.
async fn page(State(state): State<AppState>, request: Request) -> Response {
    let dir = &state.config.ui_dir;
    let index = dir.join("index.html");
    if !index.is_file() {
        return (
            StatusCode::NOT_FOUND,
            format!("UI not found at {}", dir.display()),
        )
            .into_response();
    }
    let files = ServeDir::new(dir).fallback(ServeFile::new(index));
    match files.oneshot(request).await {
        Ok(response) => response.into_response(),
        Err(err) => match err {},
    }
}

/// A non-2xx answer: `{ "error": "<text>" }`.
#[derive(Debug)]
pub struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    pub fn new(status: StatusCode, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
        }
    }
}

impl From<anyhow::Error> for ApiError {
    fn from(err: anyhow::Error) -> Self {
        match err.downcast::<GroupError>() {
            Ok(err) => err.into(),
            Err(err) => Self::new(StatusCode::INTERNAL_SERVER_ERROR, format!("{err:#}")),
        }
    }
}

/// 409 for the wrong state: in a group already, or in none.
impl From<GroupError> for ApiError {
    fn from(err: GroupError) -> Self {
        let status = match err {
            GroupError::AlreadyInGroup(_) | GroupError::NotInGroup => StatusCode::CONFLICT,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        Self::new(status, err.to_string())
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(ErrorAnswer {
                error: self.message,
            }),
        )
            .into_response()
    }
}

/// The body of every non-2xx answer.
#[derive(Debug, Clone, Serialize)]
#[cfg_attr(test, derive(ts_rs::TS))]
pub struct ErrorAnswer {
    /// What the CLI prints for the same fault.
    pub error: String,
}

/// `GET /api/status`.
#[derive(Debug, Clone, Serialize)]
#[cfg_attr(test, derive(ts_rs::TS))]
pub struct StatusAnswer {
    pub node: String,
    /// `null` when this node is in no group.
    pub group: Option<GroupStatus>,
}

/// The data of `sync-node status`.
#[derive(Debug, Clone, Serialize)]
#[cfg_attr(test, derive(ts_rs::TS))]
pub struct GroupStatus {
    pub id: String,
    pub name: Option<String>,
    pub founder: String,
    /// Unix seconds.
    pub generated_at: u64,
    /// Founder first, then by when they joined.
    pub members: Vec<MemberInfo>,
    /// Nodes removed from the group, oldest removal first.
    pub removed: Vec<RemovedInfo>,
    /// Every pair of members, each once.
    pub links: Vec<LinkInfo>,
    pub folders: Vec<FolderInfo>,
    /// The group's folders this node doesn't keep, by name.
    pub available: Vec<AvailableInfo>,
}

#[derive(Debug, Clone, Serialize)]
#[cfg_attr(test, derive(ts_rs::TS))]
pub struct AvailableInfo {
    /// The folder's doc.
    pub id: String,
    pub name: String,
    /// The nodes that keep it.
    pub nodes: usize,
}

#[derive(Debug, Clone, Serialize)]
#[cfg_attr(test, derive(ts_rs::TS))]
pub struct RemovedInfo {
    pub id: String,
    pub removed_by: String,
    /// Unix seconds, by the remover's clock.
    pub removed_at: u64,
}

#[derive(Debug, Clone, Serialize)]
#[cfg_attr(test, derive(ts_rs::TS))]
pub struct MemberInfo {
    pub id: String,
    pub name: Option<String>,
    /// How to show it: its name, or its short ID.
    pub label: String,
    pub added_by: String,
    pub this_node: bool,
    pub online: bool,
    /// Seconds since its last presence arrived.
    pub last_heard_secs: Option<u64>,
    /// This node's own connection to it, if it ever had one.
    pub peer: Option<PeerInfo>,
}

#[derive(Debug, Clone, Serialize)]
#[cfg_attr(test, derive(ts_rs::TS))]
pub struct PeerInfo {
    pub connections: usize,
    /// The selected path's kind, if a connection is open.
    pub kind: Option<LinkKind>,
    /// The selected path's address: an IP and port, or a relay URL.
    pub path: Option<String>,
    pub rtt_ms: Option<u64>,
    /// Totals since this node started.
    pub traffic: Traffic,
}

#[derive(Debug, Clone, Serialize)]
#[cfg_attr(test, derive(ts_rs::TS))]
pub struct LinkInfo {
    pub a: String,
    pub b: String,
    pub state: LinkState,
}

#[derive(Debug, Clone, Serialize)]
#[cfg_attr(test, derive(ts_rs::TS))]
pub struct FolderInfo {
    /// The folder's doc.
    pub id: String,
    pub name: String,
    pub path: String,
    pub files: usize,
    pub bytes: u64,
    /// Not being indexed: the folder's root or its marker is missing.
    pub paused: bool,
    /// Turned off on this node: not watched, fetched or served.
    pub disabled: bool,
    /// The other nodes this node syncs the folder's doc with.
    pub peers: usize,
}

impl From<&Status> for GroupStatus {
    fn from(status: &Status) -> Self {
        Self {
            id: status.group.to_string(),
            name: status.group_name.clone(),
            founder: status.founder.to_string(),
            generated_at: status.generated_at,
            members: status
                .members
                .iter()
                .map(|member| MemberInfo::new(member, status.display(member.id)))
                .collect(),
            removed: status
                .removed
                .iter()
                .map(|r| RemovedInfo {
                    id: r.id.to_string(),
                    removed_by: r.removed_by.to_string(),
                    removed_at: r.removed_at,
                })
                .collect(),
            links: status.links.iter().map(LinkInfo::from).collect(),
            folders: status.folders.iter().map(FolderInfo::from).collect(),
            available: status
                .available
                .iter()
                .map(|f| AvailableInfo {
                    id: f.id.to_string(),
                    name: f.name.clone(),
                    nodes: f.nodes,
                })
                .collect(),
        }
    }
}

impl MemberInfo {
    fn new(member: &MemberStatus, label: String) -> Self {
        Self {
            id: member.id.to_string(),
            name: member.name.clone(),
            label,
            added_by: member.added_by.to_string(),
            this_node: member.this_node,
            online: member.online,
            last_heard_secs: member.last_heard.map(|ago| ago.as_secs()),
            peer: member.peer.as_ref().map(PeerInfo::from),
        }
    }
}

impl From<&PeerSnapshot> for PeerInfo {
    fn from(peer: &PeerSnapshot) -> Self {
        Self {
            connections: peer.connections,
            kind: peer.kind,
            path: peer.path.clone(),
            rtt_ms: peer.rtt.map(|rtt| rtt.as_millis() as u64),
            traffic: peer.traffic,
        }
    }
}

impl From<&Link> for LinkInfo {
    fn from(link: &Link) -> Self {
        Self {
            a: link.a.to_string(),
            b: link.b.to_string(),
            state: link.state,
        }
    }
}

impl From<&FolderState> for FolderInfo {
    fn from(folder: &FolderState) -> Self {
        Self {
            id: folder.config.id.to_string(),
            name: folder.config.name.clone(),
            path: folder.config.path.display().to_string(),
            files: folder.stats.files,
            bytes: folder.stats.bytes,
            paused: folder.paused,
            disabled: folder.disabled,
            peers: folder.peers,
        }
    }
}

/// `GET /api/folders/{name}`: the data of `sync-node folder show`.
#[derive(Debug, Clone, Serialize)]
#[cfg_attr(test, derive(ts_rs::TS))]
pub struct FolderViewAnswer {
    /// The folder's doc.
    pub id: String,
    pub name: String,
    /// The nodes keeping the folder.
    pub nodes: Vec<NodeLabel>,
    pub counts: ViewCounts,
    /// Every path any node has an entry for, in order, in sync or not.
    pub paths: Vec<PathInfo>,
    /// Unix seconds: when this node last wrote the view.
    pub written_at: u64,
}

#[derive(Debug, Clone, Serialize)]
#[cfg_attr(test, derive(ts_rs::TS))]
pub struct NodeLabel {
    pub id: String,
    /// Its name, or its short ID.
    pub label: String,
}

/// How many paths are in each class, as `folder show` counts them.
#[derive(Debug, Clone, Default, Serialize)]
#[cfg_attr(test, derive(ts_rs::TS))]
pub struct ViewCounts {
    pub in_sync: usize,
    pub to_fetch: usize,
    pub to_delete: usize,
    pub conflicts: usize,
    pub deleted_everywhere: usize,
}

/// Where a path stands, once the folder has synced against now.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[cfg_attr(test, derive(ts_rs::TS))]
pub enum PathClass {
    InSync,
    DeletedEverywhere,
    /// Some node will fetch it.
    ToFetch,
    /// Some node will delete it.
    ToDelete,
    Conflict,
}

#[derive(Debug, Clone, Serialize)]
#[cfg_attr(test, derive(ts_rs::TS))]
pub struct PathInfo {
    pub path: String,
    pub class: PathClass,
    /// The size of the version the path ends up with; `null` if deleted.
    pub size: Option<u64>,
    /// The node whose version the path ends up with; `null` if deleted.
    pub source: Option<String>,
    /// Where each node stands on the path.
    pub nodes: Vec<PathNode>,
    /// For a conflict: the losing versions, kept as conflict copies.
    pub copies: Vec<ConflictCopy>,
}

#[derive(Debug, Clone, Serialize)]
#[cfg_attr(test, derive(ts_rs::TS))]
pub struct PathNode {
    pub id: String,
    pub state: NodeState,
}

#[derive(Debug, Clone, Serialize)]
#[cfg_attr(test, derive(ts_rs::TS))]
pub struct ConflictCopy {
    /// The node whose version it is.
    pub node: String,
    /// The name it is kept under.
    pub name: String,
}

impl From<&ViewFile> for FolderViewAnswer {
    fn from(file: &ViewFile) -> Self {
        let label = |id: EndpointId| {
            file.names
                .get(&id)
                .cloned()
                .unwrap_or_else(|| id.fmt_short().to_string())
        };
        let view = &file.view;
        let paths: Vec<PathInfo> = view
            .paths
            .iter()
            .map(|p| PathInfo::new(p, &label))
            .collect();
        let mut counts = ViewCounts::default();
        for p in &paths {
            *match p.class {
                PathClass::InSync => &mut counts.in_sync,
                PathClass::DeletedEverywhere => &mut counts.deleted_everywhere,
                PathClass::ToFetch => &mut counts.to_fetch,
                PathClass::ToDelete => &mut counts.to_delete,
                PathClass::Conflict => &mut counts.conflicts,
            } += 1;
        }
        Self {
            id: view.folder.to_string(),
            name: view.name.clone(),
            nodes: view
                .nodes
                .iter()
                .map(|id| NodeLabel {
                    id: id.to_string(),
                    label: label(*id),
                })
                .collect(),
            counts,
            paths,
            written_at: file.written_at,
        }
    }
}

impl PathInfo {
    fn new(p: &PathView, label: &dyn Fn(EndpointId) -> String) -> Self {
        let class = match (&p.outcome, p.in_sync()) {
            (Outcome::Deleted, true) => PathClass::DeletedEverywhere,
            (_, true) => PathClass::InSync,
            (Outcome::Conflict { .. }, false) => PathClass::Conflict,
            (Outcome::Deleted, false) => PathClass::ToDelete,
            (Outcome::File(_), false) => PathClass::ToFetch,
        };
        let (winner, others) = match &p.outcome {
            Outcome::Deleted => (None, &[][..]),
            Outcome::File(c) => (Some(c), &[][..]),
            Outcome::Conflict { winner, others } => (Some(winner), &others[..]),
        };
        Self {
            path: p.path.clone(),
            class,
            size: winner.map(|c| c.size),
            source: winner.map(|c| c.node.to_string()),
            nodes: p
                .nodes
                .iter()
                .map(|(id, state)| PathNode {
                    id: id.to_string(),
                    state: *state,
                })
                .collect(),
            copies: others
                .iter()
                .map(|c| ConflictCopy {
                    node: c.node.to_string(),
                    name: crate::view::conflict_name(&p.path, c, &label(c.node)),
                })
                .collect(),
        }
    }
}

/// `GET /api/transfers`: what this node downloads and uploads now.
#[derive(Debug, Clone, Serialize)]
#[cfg_attr(test, derive(ts_rs::TS))]
pub struct TransfersAnswer {
    /// The downloads running, by folder and path.
    pub swarms: Vec<SwarmInfo>,
    /// Each peer with swarm streams to this node, by ID.
    pub uploads: Vec<UploadInfo>,
}

/// One running download: a swarm for one version of one file.
#[derive(Debug, Clone, Serialize)]
#[cfg_attr(test, derive(ts_rs::TS))]
pub struct SwarmInfo {
    /// The folder's doc.
    pub folder_id: String,
    /// The folder's name, or its doc ID if this node no longer keeps it.
    pub folder: String,
    pub path: String,
    /// The version's content hash.
    pub content: String,
    pub size: u64,
    pub piece_length: u32,
    pub pieces: u32,
    /// Verified, in the staging file.
    pub have: u32,
    /// Started but not verified yet.
    pub active: u32,
    /// Every node the download asks, in the swarm or not.
    pub peers: Vec<SwarmPeerInfo>,
    /// No peer in the swarm both has a piece this node lacks and unchokes
    /// it.
    pub stuck: bool,
}

#[derive(Debug, Clone, Serialize)]
#[cfg_attr(test, derive(ts_rs::TS))]
pub struct SwarmPeerInfo {
    pub id: String,
    /// Its name, or its short ID.
    pub label: String,
    pub state: SwarmPeerKind,
    /// Only when `state` is `In`.
    pub in_swarm: Option<InSwarmInfo>,
}

/// Where a peer stands in a download's swarm.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[cfg_attr(test, derive(ts_rs::TS))]
pub enum SwarmPeerKind {
    /// Not in the swarm; asked again every 2 s.
    Out,
    /// Not asked again during this download: it sent a bad piece, no longer
    /// has the file, or is no longer a member.
    Banned,
    Joining,
    In,
}

#[derive(Debug, Clone, Serialize)]
#[cfg_attr(test, derive(ts_rs::TS))]
pub struct InSwarmInfo {
    /// It serves this node's requests.
    pub unchoked: bool,
    /// This node wants pieces it has.
    pub interested: bool,
    /// Bytes per second it sends this node.
    pub rate: u64,
    /// Requests open to it.
    pub open: usize,
    /// It sent no block for 60 s with requests open.
    pub snubbed: bool,
    /// How many pieces it has.
    pub pieces: u32,
}

/// What this node uploads to one peer, over all its swarms.
#[derive(Debug, Clone, Serialize)]
#[cfg_attr(test, derive(ts_rs::TS))]
pub struct UploadInfo {
    pub id: String,
    /// Its name, or its short ID.
    pub label: String,
    /// Its swarm streams to this node.
    pub streams: usize,
    /// It wants pieces on at least one of them.
    pub interested: bool,
    /// The choker uploads to it.
    pub unchoked: bool,
    /// It holds the optimistic unchoke.
    pub optimistic: bool,
    /// Bytes per second this node sends it.
    pub rate: u64,
}

impl SwarmInfo {
    fn new(swarm: &SwarmSnapshot, folder: String, backend: &dyn UiBackend) -> Self {
        Self {
            folder_id: swarm.folder.to_string(),
            folder,
            path: swarm.path.clone(),
            content: swarm.content.to_string(),
            size: swarm.size,
            piece_length: swarm.piece_length,
            pieces: swarm.pieces,
            have: swarm.have,
            active: swarm.active,
            peers: swarm
                .peers
                .iter()
                .map(|peer| {
                    let (state, in_swarm) = match &peer.state {
                        SwarmPeerState::Out => (SwarmPeerKind::Out, None),
                        SwarmPeerState::Banned => (SwarmPeerKind::Banned, None),
                        SwarmPeerState::Joining => (SwarmPeerKind::Joining, None),
                        SwarmPeerState::In(s) => (
                            SwarmPeerKind::In,
                            Some(InSwarmInfo {
                                unchoked: s.unchoked,
                                interested: s.interested,
                                rate: s.rate as u64,
                                open: s.open,
                                snubbed: s.snubbed,
                                pieces: s.pieces,
                            }),
                        ),
                    };
                    SwarmPeerInfo {
                        id: peer.id.to_string(),
                        label: backend.label(peer.id),
                        state,
                        in_swarm,
                    }
                })
                .collect(),
            stuck: swarm.stuck,
        }
    }
}

impl UploadInfo {
    fn new(upload: &UploadSnapshot, backend: &dyn UiBackend) -> Self {
        Self {
            id: upload.id.to_string(),
            label: backend.label(upload.id),
            streams: upload.streams,
            interested: upload.interested,
            unchoked: upload.unchoked,
            optimistic: upload.optimistic,
            rate: upload.rate as u64,
        }
    }
}

/// The data of `GET /api/status` and UI topic `status`.
fn read_status(backend: &dyn UiBackend) -> StatusAnswer {
    let status = backend.status();
    StatusAnswer {
        node: backend.id().to_string(),
        group: status.as_ref().map(GroupStatus::from),
    }
}

/// The data of `GET /api/folders` and UI topic `folders`.
fn read_folders(backend: &dyn UiBackend) -> Vec<FolderInfo> {
    backend.folders().iter().map(FolderInfo::from).collect()
}

/// The data of `GET /api/folders/{name}` and UI topic `folders/<name>`.
fn read_folder(backend: &dyn UiBackend, name: &str) -> Result<FolderViewAnswer, ApiError> {
    if !backend.folders().iter().any(|f| f.config.name == name) {
        return Err(ApiError::new(
            StatusCode::NOT_FOUND,
            format!("this node doesn't keep folder {name}"),
        ));
    }
    match backend.folder(name)? {
        Some(file) => Ok(FolderViewAnswer::from(&file)),
        None => Err(ApiError::new(
            StatusCode::NOT_FOUND,
            format!("no view of folder {name} yet"),
        )),
    }
}

/// The data of `GET /api/transfers` and UI topic `transfers`.
fn read_transfers(backend: &dyn UiBackend) -> TransfersAnswer {
    let transfers = backend.transfers();
    let names: HashMap<_, _> = backend
        .folders()
        .into_iter()
        .map(|f| (f.config.id, f.config.name))
        .collect();
    TransfersAnswer {
        swarms: transfers
            .swarms
            .iter()
            .map(|swarm| {
                let folder = names
                    .get(&swarm.folder)
                    .cloned()
                    .unwrap_or_else(|| swarm.folder.to_string());
                SwarmInfo::new(swarm, folder, backend)
            })
            .collect(),
        uploads: transfers
            .uploads
            .iter()
            .map(|upload| UploadInfo::new(upload, backend))
            .collect(),
    }
}

async fn status(State(state): State<AppState>) -> Json<StatusAnswer> {
    Json(read_status(&*state.backend))
}

async fn folders(State(state): State<AppState>) -> Json<Vec<FolderInfo>> {
    Json(read_folders(&*state.backend))
}

async fn transfers(State(state): State<AppState>) -> Json<TransfersAnswer> {
    Json(read_transfers(&*state.backend))
}

async fn folder(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<FolderViewAnswer>, ApiError> {
    read_folder(&*state.backend, &name).map(Json)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use ts_rs::{Config, TS};

    use super::*;

    /// Writes the API types for the page to `ui/src/api/`, or to
    /// `SYNC_NODE_UI_TYPES_DIR` (`just ui-check` compares the two). Every
    /// `.ts` file there is generated: a removed type's file goes too.
    #[test]
    fn export_api_types() {
        let dir = match std::env::var_os("SYNC_NODE_UI_TYPES_DIR") {
            Some(dir) => PathBuf::from(dir),
            None => PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../ui/src/api"),
        };
        std::fs::create_dir_all(&dir).unwrap();
        for entry in std::fs::read_dir(&dir).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().is_some_and(|ext| ext == "ts") {
                std::fs::remove_file(path).unwrap();
            }
        }
        // Byte counts and seconds stay far below 2^53, so a JS number holds
        // them exactly.
        let config = Config::new().with_large_int("number").with_out_dir(&dir);
        StatusAnswer::export_all(&config).unwrap();
        FolderViewAnswer::export_all(&config).unwrap();
        TransfersAnswer::export_all(&config).unwrap();
        topics::ClientMessage::export_all(&config).unwrap();
        topics::ServerMessage::export_all(&config).unwrap();
        actions::CreateRequest::export_all(&config).unwrap();
        actions::CreateAnswer::export_all(&config).unwrap();
        actions::JoinRequest::export_all(&config).unwrap();
        actions::JoinAnswer::export_all(&config).unwrap();
        actions::InviteRequest::export_all(&config).unwrap();
        actions::InviteAnswer::export_all(&config).unwrap();
        actions::MemberRequest::export_all(&config).unwrap();
        actions::RemoveRequest::export_all(&config).unwrap();
        actions::GroupNameRequest::export_all(&config).unwrap();
        actions::FolderRequest::export_all(&config).unwrap();
        actions::FolderEnabledRequest::export_all(&config).unwrap();
        actions::QueuedAnswer::export_all(&config).unwrap();
        ErrorAnswer::export_all(&config).unwrap();
    }
}
