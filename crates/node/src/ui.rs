//! The web UI's server: a JSON API under `/api` and the built page from the UI
//! dir. Part of `sync-node run` (ADR 0004); see `plans/ui/spec.md`.
//!
//! The handlers take a [`UiBackend`], not a [`Node`], so the tests drive the
//! router with a fake through `oneshot`: no node, no port.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use axum::extract::{Request, State};
use axum::http::{header, Method, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use iroh::EndpointId;
use serde::Serialize;
use tower::ServiceExt as _;
use tower_http::services::{ServeDir, ServeFile};

use crate::folder::FolderState;
use crate::node::Node;
use crate::peers::{LinkKind, PeerSnapshot, Traffic};
use crate::presence::LinkState;
use crate::status::{Link, MemberStatus, Status};

/// What the handlers need from the running node. The node implements it; the
/// tests implement a fake.
pub trait UiBackend: Send + Sync + 'static {
    fn id(&self) -> EndpointId;
    /// The group as this node sees it, or `None` in no group.
    fn status(&self) -> Option<Status>;
}

impl UiBackend for Node {
    fn id(&self) -> EndpointId {
        Node::id(self)
    }

    fn status(&self) -> Option<Status> {
        Node::status(self)
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
}

/// The whole server: the API, the page, and the checks in front of both.
pub fn router(backend: Arc<dyn UiBackend>, config: UiConfig) -> Router {
    let state = AppState {
        backend,
        config: Arc::new(config),
    };
    let api = Router::new()
        .route("/status", get(status))
        .fallback(|| async { ApiError::new(StatusCode::NOT_FOUND, "no such API route") });
    Router::new()
        .nest("/api", api)
        .fallback(page)
        .layer(middleware::from_fn_with_state(state.clone(), checks))
        .with_state(state)
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

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        #[derive(Serialize)]
        struct Body {
            error: String,
        }
        (
            self.status,
            Json(Body {
                error: self.message,
            }),
        )
            .into_response()
    }
}

/// `GET /api/status`.
#[derive(Debug, Clone, Serialize)]
pub struct StatusAnswer {
    pub node: String,
    /// `None` when this node is in no group.
    pub group: Option<GroupStatus>,
}

/// The data of `sync-node status`.
#[derive(Debug, Clone, Serialize)]
pub struct GroupStatus {
    pub id: String,
    pub founder: String,
    /// Unix seconds.
    pub generated_at: u64,
    /// Founder first, then by when they joined.
    pub members: Vec<MemberInfo>,
    /// Every pair of members, each once.
    pub links: Vec<LinkInfo>,
    pub folders: Vec<FolderInfo>,
}

#[derive(Debug, Clone, Serialize)]
pub struct MemberInfo {
    pub id: String,
    pub name: Option<String>,
    pub added_by: String,
    pub this_node: bool,
    pub online: bool,
    /// Seconds since its last presence arrived.
    pub last_heard_secs: Option<u64>,
    /// This node's own connection to it, if it ever had one.
    pub peer: Option<PeerInfo>,
}

#[derive(Debug, Clone, Serialize)]
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
pub struct LinkInfo {
    pub a: String,
    pub b: String,
    pub state: LinkState,
}

#[derive(Debug, Clone, Serialize)]
pub struct FolderInfo {
    /// The folder's doc.
    pub id: String,
    pub name: String,
    pub path: String,
    pub files: usize,
    pub bytes: u64,
    /// Not being indexed: the folder's root or its marker is missing.
    pub paused: bool,
    /// The other nodes this node syncs the folder's doc with.
    pub peers: usize,
}

impl From<&Status> for GroupStatus {
    fn from(status: &Status) -> Self {
        Self {
            id: status.group.to_string(),
            founder: status.founder.to_string(),
            generated_at: status.generated_at,
            members: status.members.iter().map(MemberInfo::from).collect(),
            links: status.links.iter().map(LinkInfo::from).collect(),
            folders: status.folders.iter().map(FolderInfo::from).collect(),
        }
    }
}

impl From<&MemberStatus> for MemberInfo {
    fn from(member: &MemberStatus) -> Self {
        Self {
            id: member.id.to_string(),
            name: member.name.clone(),
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
            peers: folder.peers,
        }
    }
}

async fn status(State(state): State<AppState>) -> Json<StatusAnswer> {
    let status = state.backend.status();
    Json(StatusAnswer {
        node: state.backend.id().to_string(),
        group: status.as_ref().map(GroupStatus::from),
    })
}
