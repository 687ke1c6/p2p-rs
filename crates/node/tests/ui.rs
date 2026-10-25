use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::body::Body;
use axum::http::{header, Method, Request, StatusCode};
use axum::Router;
use iroh::{EndpointId, SecretKey};
use iroh_blobs::Hash;
use iroh_docs::NamespaceId;
use serde_json::{json, Value};
use sync_node::folder::{
    self, AvailableFolder, FolderConfig, FolderState, ViewFile, DISABLED_FILE, FOLDER_ADDS_DIR,
    FOLDER_REMOVES_DIR,
};
use sync_node::index::IndexStats;
use sync_node::invite::INVITES_DIR;
use sync_node::member::{ADDS_DIR, GROUP_NAME_FILE, REMOVES_DIR};
use sync_node::peer_wire::{
    InSwarm, SwarmPeer, SwarmPeerState, SwarmSnapshot, Transfers, UploadSnapshot,
};
use sync_node::peers::{LinkKind, PeerSnapshot, Traffic};
use sync_node::presence::LinkState;
use sync_node::record::GroupId;
use sync_node::status::{Link, MemberStatus, RemovedStatus};
use sync_node::ui::topics::{ClientMessage, Topics};
use sync_node::ui::{self, BoxFuture, UiBackend, UiConfig};
use sync_node::view::{Candidate, FolderView, NodeState, Outcome, PathView};
use sync_node::InviteTicket;
use sync_node::Status;
use sync_node::Ticket;
use tower::ServiceExt;

const ADDR: &str = "127.0.0.1:7777";

/// A node as the handlers see it, with whatever state the test gives it.
/// It keeps the folders in its status.
struct Fake {
    id: EndpointId,
    status: Mutex<Option<Status>>,
    /// The views written so far, by folder name.
    views: BTreeMap<String, ViewFile>,
    /// How often `status` was read.
    reads: AtomicUsize,
    /// `join` never finishes.
    join_hangs: bool,
    transfers: Mutex<Transfers>,
    /// The name `create_group` was last called with.
    created: Arc<Mutex<Option<Option<String>>>>,
}

impl Fake {
    fn new(id: EndpointId) -> Self {
        Self {
            id,
            status: Mutex::new(None),
            views: BTreeMap::new(),
            reads: AtomicUsize::new(0),
            join_hangs: false,
            transfers: Mutex::default(),
            created: Arc::default(),
        }
    }

    fn reads(&self) -> usize {
        self.reads.load(Ordering::SeqCst)
    }
}

impl UiBackend for Fake {
    fn id(&self) -> EndpointId {
        self.id
    }

    fn status(&self) -> Option<Status> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.status.lock().unwrap().clone()
    }

    fn folders(&self) -> Vec<FolderState> {
        self.status
            .lock()
            .unwrap()
            .as_ref()
            .map(|status| status.folders.clone())
            .unwrap_or_default()
    }

    fn folder(&self, name: &str) -> anyhow::Result<Option<ViewFile>> {
        Ok(self.views.get(name).cloned())
    }

    fn transfers(&self) -> Transfers {
        self.transfers.lock().unwrap().clone()
    }

    fn label(&self, id: EndpointId) -> String {
        match &*self.status.lock().unwrap() {
            Some(status) => status.display(id),
            None => id.fmt_short().to_string(),
        }
    }

    fn create_group(&self, name: Option<String>) -> BoxFuture<'_, anyhow::Result<GroupId>> {
        *self.created.lock().unwrap() = Some(name);
        Box::pin(async { Ok(new_group()) })
    }

    fn join(&self, ticket: Ticket) -> BoxFuture<'_, anyhow::Result<(GroupId, EndpointId)>> {
        Box::pin(async move {
            if self.join_hangs {
                std::future::pending::<()>().await;
            }
            // Joined through a member: say it founded the group.
            let founder = match ticket {
                Ticket::Member(addr) => addr.id,
                Ticket::Invite(invite) => invite.inviter.id,
            };
            Ok((new_group(), founder))
        })
    }
}

/// The group the fake creates or joins.
fn new_group() -> GroupId {
    NamespaceId::from(&[9u8; 32])
}

/// The node `in_group` lists as removed.
fn removed() -> EndpointId {
    SecretKey::from_bytes(&[7u8; 32]).public()
}

fn id() -> EndpointId {
    SecretKey::generate().public()
}

fn app(fake: Fake, ui_dir: &Path, config_dir: &Path) -> Router {
    let (router, _topics) = ui::router(
        Arc::new(fake),
        UiConfig {
            addr: ADDR.parse::<SocketAddr>().unwrap(),
            ui_dir: ui_dir.to_owned(),
            config_dir: config_dir.to_owned(),
        },
    );
    router
}

fn get(path: &str) -> Request<Body> {
    Request::get(path)
        .header(header::HOST, ADDR)
        .body(Body::empty())
        .unwrap()
}

async fn send(app: Router, request: Request<Body>) -> (StatusCode, String) {
    let response = app.oneshot(request).await.unwrap();
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, String::from_utf8(body.to_vec()).unwrap())
}

async fn send_json(app: Router, request: Request<Body>) -> (StatusCode, Value) {
    let (status, body) = send(app, request).await;
    let value = serde_json::from_str(&body).unwrap_or_else(|err| panic!("{err}: {body}"));
    (status, value)
}

/// A group of two named `home`: this node (the founder) and a member it is
/// connected to directly, keeping one folder. One node was removed.
fn in_group(this: EndpointId, other: EndpointId) -> Status {
    let traffic = Traffic {
        direct_sent: 10,
        direct_recv: 20,
        relay_sent: 3,
        relay_recv: 4,
    };
    Status {
        node: this,
        group: NamespaceId::from(&[1u8; 32]),
        group_name: Some("home".to_string()),
        founder: this,
        generated_at: 1_700_000_000,
        members: vec![
            MemberStatus {
                id: this,
                name: Some("laptop".to_string()),
                added_by: this,
                this_node: true,
                online: true,
                last_heard: None,
                peer: None,
            },
            MemberStatus {
                id: other,
                name: None,
                added_by: this,
                this_node: false,
                online: true,
                last_heard: Some(Duration::from_secs(3)),
                peer: Some(PeerSnapshot {
                    id: other,
                    connections: 2,
                    kind: Some(LinkKind::Direct),
                    path: Some("192.168.1.5:4433".to_string()),
                    rtt: Some(Duration::from_millis(12)),
                    traffic,
                }),
            },
        ],
        removed: vec![RemovedStatus {
            id: removed(),
            removed_by: this,
            removed_at: 1_699_999_000,
        }],
        links: vec![Link {
            a: this,
            b: other,
            state: LinkState::Direct,
        }],
        folders: vec![FolderState {
            config: FolderConfig {
                id: NamespaceId::from(&[2u8; 32]),
                name: "photos".to_string(),
                path: "/home/g/photos".into(),
            },
            stats: IndexStats {
                files: 5,
                bytes: 1234,
            },
            paused: false,
            disabled: false,
            peers: 1,
        }],
        available: vec![AvailableFolder {
            id: NamespaceId::from(&[3u8; 32]),
            name: "music".to_string(),
            nodes: 2,
        }],
    }
}

#[tokio::test]
async fn status_in_no_group() {
    let dir = tempfile::tempdir().unwrap();
    let this = id();
    let fake = Fake::new(this);
    let (code, body) = send_json(app(fake, dir.path(), dir.path()), get("/api/status")).await;
    assert_eq!(code, StatusCode::OK);
    assert_eq!(body, json!({ "node": this.to_string(), "group": null }));
}

#[tokio::test]
async fn status_in_a_group() {
    let dir = tempfile::tempdir().unwrap();
    let (this, other) = (id(), id());
    let status = in_group(this, other);
    let fake = Fake {
        status: Mutex::new(Some(status.clone())),
        ..Fake::new(this)
    };
    let (code, body) = send_json(app(fake, dir.path(), dir.path()), get("/api/status")).await;
    assert_eq!(code, StatusCode::OK);
    let other_short = other.fmt_short().to_string();
    let (this, other) = (this.to_string(), other.to_string());
    assert_eq!(
        body,
        json!({
            "node": this,
            "group": {
                "id": status.group.to_string(),
                "name": "home",
                "founder": this,
                "generated_at": 1_700_000_000,
                "members": [
                    {
                        "id": this,
                        "name": "laptop",
                        "label": "laptop",
                        "added_by": this,
                        "this_node": true,
                        "online": true,
                        "last_heard_secs": null,
                        "peer": null,
                    },
                    {
                        "id": other,
                        "name": null,
                        "label": other_short,
                        "added_by": this,
                        "this_node": false,
                        "online": true,
                        "last_heard_secs": 3,
                        "peer": {
                            "connections": 2,
                            "kind": "Direct",
                            "path": "192.168.1.5:4433",
                            "rtt_ms": 12,
                            "traffic": {
                                "direct_sent": 10,
                                "direct_recv": 20,
                                "relay_sent": 3,
                                "relay_recv": 4,
                            },
                        },
                    },
                ],
                "removed": [{
                    "id": removed().to_string(),
                    "removed_by": this,
                    "removed_at": 1_699_999_000,
                }],
                "links": [{ "a": this, "b": other, "state": "Direct" }],
                "folders": [{
                    "id": NamespaceId::from(&[2u8; 32]).to_string(),
                    "name": "photos",
                    "path": "/home/g/photos",
                    "files": 5,
                    "bytes": 1234,
                    "paused": false,
                    "disabled": false,
                    "peers": 1,
                }],
                "available": [{
                    "id": NamespaceId::from(&[3u8; 32]).to_string(),
                    "name": "music",
                    "nodes": 2,
                }],
            },
        })
    );
}

fn no_group(dir: &Path) -> Router {
    app(Fake::new(id()), dir, dir)
}

#[tokio::test]
async fn unknown_host_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    for host in [Some("evil.example:7777"), Some("127.0.0.1:8888"), None] {
        let mut request = Request::get("/api/status");
        if let Some(host) = host {
            request = request.header(header::HOST, host);
        }
        let request = request.body(Body::empty()).unwrap();
        let (code, body) = send_json(no_group(dir.path()), request).await;
        assert_eq!(code, StatusCode::FORBIDDEN, "Host {host:?}");
        assert_eq!(body, json!({ "error": "unknown Host" }));
    }
}

#[tokio::test]
async fn localhost_is_allowed_for_a_loopback_address() {
    let dir = tempfile::tempdir().unwrap();
    let request = Request::get("/api/status")
        .header(header::HOST, "localhost:7777")
        .body(Body::empty())
        .unwrap();
    let (code, _) = send(no_group(dir.path()), request).await;
    assert_eq!(code, StatusCode::OK);
}

#[tokio::test]
async fn post_without_json_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    for kind in [
        Some("text/plain"),
        Some("application/x-www-form-urlencoded"),
        None,
    ] {
        let mut request = Request::builder()
            .method(Method::POST)
            .uri("/api/group")
            .header(header::HOST, ADDR);
        if let Some(kind) = kind {
            request = request.header(header::CONTENT_TYPE, kind);
        }
        let request = request.body(Body::from("{}")).unwrap();
        let (code, body) = send_json(no_group(dir.path()), request).await;
        assert_eq!(code, StatusCode::UNSUPPORTED_MEDIA_TYPE, "{kind:?}");
        assert_eq!(
            body,
            json!({ "error": "Content-Type must be application/json" })
        );
    }
}

#[tokio::test]
async fn unknown_api_route_is_a_json_404() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("index.html"), "<html>page</html>").unwrap();
    let (code, body) = send_json(no_group(dir.path()), get("/api/nothing")).await;
    assert_eq!(code, StatusCode::NOT_FOUND);
    assert_eq!(body, json!({ "error": "no such API route" }));
}

#[tokio::test]
async fn ui_not_found() {
    let config = tempfile::tempdir().unwrap();
    let ui_dir = config.path().join("ui");
    for path in ["/", "/group"] {
        let (code, body) = send(no_group(&ui_dir), get(path)).await;
        assert_eq!(code, StatusCode::NOT_FOUND);
        assert_eq!(body, format!("UI not found at {}", ui_dir.display()));
    }
    // The API works without the page.
    let (code, _) = send(no_group(&ui_dir), get("/api/status")).await;
    assert_eq!(code, StatusCode::OK);
}

#[tokio::test]
async fn page_files_and_index_fallback() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("index.html"), "<html>page</html>").unwrap();
    std::fs::create_dir(dir.path().join("assets")).unwrap();
    std::fs::write(dir.path().join("assets/app.js"), "console.log(1)").unwrap();
    for (path, want) in [
        ("/", "<html>page</html>"),
        ("/assets/app.js", "console.log(1)"),
        // The page routes in the browser, so a reload on a view works.
        ("/folders", "<html>page</html>"),
        ("/folders/photos", "<html>page</html>"),
    ] {
        let (code, body) = send(no_group(dir.path()), get(path)).await;
        assert_eq!(code, StatusCode::OK, "{path}");
        assert_eq!(body, want, "{path}");
    }
}

/// A view of `photos` kept by `this` and `other`: one file in sync, one
/// `other` will fetch, one `this` will delete, one deleted everywhere and one
/// conflict.
fn photos_view(this: EndpointId, other: EndpointId) -> ViewFile {
    let candidate = |node, byte: u8, size| Candidate {
        node,
        content: Hash::new([byte]),
        size,
        mtime_ns: 1_700_000_000_000_000_000,
    };
    let path = |path: &str, outcome, states: [NodeState; 2]| PathView {
        path: path.to_string(),
        outcome,
        nodes: BTreeMap::from([(this, states[0]), (other, states[1])]),
    };
    use NodeState::*;
    ViewFile {
        view: FolderView {
            folder: NamespaceId::from(&[2u8; 32]),
            name: "photos".to_string(),
            nodes: BTreeSet::from([this, other]),
            paths: vec![
                path(
                    "a.jpg",
                    Outcome::File(candidate(this, 1, 100)),
                    [Current, Current],
                ),
                path(
                    "b.jpg",
                    Outcome::File(candidate(this, 2, 200)),
                    [Current, Missing],
                ),
                path("c.jpg", Outcome::Deleted, [WillDelete, Current]),
                path("d.jpg", Outcome::Deleted, [Current, Current]),
                path(
                    "e.txt",
                    Outcome::Conflict {
                        winner: candidate(this, 3, 300),
                        others: vec![candidate(other, 4, 400)],
                    },
                    [Outdated, Outdated],
                ),
            ],
        },
        names: BTreeMap::from([(this, "laptop".to_string())]),
        entries: BTreeMap::new(),
        written_at: 1_700_000_100,
    }
}

#[tokio::test]
async fn folders_list_the_kept_folders() {
    let dir = tempfile::tempdir().unwrap();
    let (this, other) = (id(), id());
    let fake = Fake {
        status: Mutex::new(Some(in_group(this, other))),
        ..Fake::new(this)
    };
    let (code, body) = send_json(app(fake, dir.path(), dir.path()), get("/api/folders")).await;
    assert_eq!(code, StatusCode::OK);
    assert_eq!(
        body,
        json!([{
            "id": NamespaceId::from(&[2u8; 32]).to_string(),
            "name": "photos",
            "path": "/home/g/photos",
            "files": 5,
            "bytes": 1234,
            "paused": false,
            "disabled": false,
            "peers": 1,
        }])
    );
    // In no group, no folders.
    let (code, body) = send_json(no_group(dir.path()), get("/api/folders")).await;
    assert_eq!(code, StatusCode::OK);
    assert_eq!(body, json!([]));
}

#[tokio::test]
async fn folder_view() {
    let dir = tempfile::tempdir().unwrap();
    let (this, other) = (id(), id());
    let fake = Fake {
        status: Mutex::new(Some(in_group(this, other))),
        views: BTreeMap::from([("photos".to_string(), photos_view(this, other))]),
        ..Fake::new(this)
    };
    let (code, body) = send_json(
        app(fake, dir.path(), dir.path()),
        get("/api/folders/photos"),
    )
    .await;
    assert_eq!(code, StatusCode::OK);
    let other_short = other.fmt_short().to_string();
    let (this, other) = (this.to_string(), other.to_string());
    let nodes = |a: &str, b: &str| json!([{ "id": this, "state": a }, { "id": other, "state": b }]);
    // Node lists come in node ID order, and the IDs are random.
    let ordered = |v: Value| {
        let mut list = v.as_array().unwrap().clone();
        list.sort_by(|a, b| a["id"].as_str().cmp(&b["id"].as_str()));
        Value::Array(list)
    };
    assert_eq!(body["id"], NamespaceId::from(&[2u8; 32]).to_string());
    assert_eq!(body["name"], "photos");
    assert_eq!(body["written_at"], 1_700_000_100);
    assert_eq!(
        ordered(body["nodes"].clone()),
        ordered(json!([
            { "id": this, "label": "laptop" },
            { "id": other, "label": other_short },
        ]))
    );
    assert_eq!(
        body["counts"],
        json!({ "in_sync": 1, "to_fetch": 1, "to_delete": 1, "conflicts": 1, "deleted_everywhere": 1 })
    );
    let paths = body["paths"].as_array().unwrap();
    let want = [
        (
            "a.jpg",
            "InSync",
            json!(100),
            json!(this),
            nodes("Current", "Current"),
        ),
        (
            "b.jpg",
            "ToFetch",
            json!(200),
            json!(this),
            nodes("Current", "Missing"),
        ),
        (
            "c.jpg",
            "ToDelete",
            json!(null),
            json!(null),
            nodes("WillDelete", "Current"),
        ),
        (
            "d.jpg",
            "DeletedEverywhere",
            json!(null),
            json!(null),
            nodes("Current", "Current"),
        ),
        (
            "e.txt",
            "Conflict",
            json!(300),
            json!(this),
            nodes("Outdated", "Outdated"),
        ),
    ];
    assert_eq!(paths.len(), want.len());
    for (got, (path, class, size, source, nodes)) in paths.iter().zip(want) {
        assert_eq!(got["path"], path);
        assert_eq!(got["class"], class, "{path}");
        assert_eq!(got["size"], size, "{path}");
        assert_eq!(got["source"], source, "{path}");
        assert_eq!(ordered(got["nodes"].clone()), ordered(nodes), "{path}");
    }
    assert_eq!(paths[0]["copies"], json!([]));
    assert_eq!(
        paths[4]["copies"],
        json!([{
            "node": other,
            "name": format!("e.sync-conflict-20231114-221320-{other_short}.txt"),
        }])
    );
}

#[tokio::test]
async fn folder_view_404s() {
    let dir = tempfile::tempdir().unwrap();
    let (this, other) = (id(), id());
    // Kept, but no view written yet.
    let fake = Fake {
        status: Mutex::new(Some(in_group(this, other))),
        ..Fake::new(this)
    };
    let (code, body) = send_json(
        app(fake, dir.path(), dir.path()),
        get("/api/folders/photos"),
    )
    .await;
    assert_eq!(code, StatusCode::NOT_FOUND);
    assert_eq!(body, json!({ "error": "no view of folder photos yet" }));
    // Not kept by this node, even with a view left over.
    let fake = Fake {
        status: Mutex::new(Some(in_group(this, other))),
        views: BTreeMap::from([("music".to_string(), photos_view(this, other))]),
        ..Fake::new(this)
    };
    let (code, body) =
        send_json(app(fake, dir.path(), dir.path()), get("/api/folders/music")).await;
    assert_eq!(code, StatusCode::NOT_FOUND);
    assert_eq!(
        body,
        json!({ "error": "this node doesn't keep folder music" })
    );
}

// UI topics (ADR 0005), without a socket. Time is paused, so the timers'
// ticks come when every task waits.

/// This node downloads `big.bin` of `photos` from `other`, with `third`
/// banned, and an older version from a folder it no longer keeps; it
/// uploads to both.
fn transfers(other: EndpointId, third: EndpointId) -> Transfers {
    let swarm = |folder: NamespaceId, path: &str, peers: Vec<SwarmPeer>| SwarmSnapshot {
        folder,
        path: path.to_string(),
        content: Hash::new(b"big"),
        size: 10 << 20,
        piece_length: 1 << 20,
        pieces: 10,
        have: 4,
        active: 2,
        stuck: peers.is_empty(),
        peers,
    };
    Transfers {
        swarms: vec![
            swarm(
                NamespaceId::from(&[2u8; 32]),
                "big.bin",
                vec![
                    SwarmPeer {
                        id: other,
                        state: SwarmPeerState::In(InSwarm {
                            unchoked: true,
                            interested: true,
                            rate: 1500.7,
                            open: 12,
                            snubbed: false,
                            pieces: 10,
                        }),
                    },
                    SwarmPeer {
                        id: third,
                        state: SwarmPeerState::Banned,
                    },
                ],
            ),
            swarm(NamespaceId::from(&[3u8; 32]), "old.bin", vec![]),
        ],
        uploads: vec![
            UploadSnapshot {
                id: other,
                streams: 1,
                interested: true,
                unchoked: true,
                optimistic: false,
                rate: 99.5,
            },
            UploadSnapshot {
                id: third,
                streams: 2,
                interested: true,
                unchoked: true,
                optimistic: true,
                rate: 0.0,
            },
        ],
    }
}

#[tokio::test]
async fn transfers_route() {
    let dir = tempfile::tempdir().unwrap();
    let (this, other, third) = (id(), id(), id());
    let fake = Fake {
        status: Mutex::new(Some(in_group(this, other))),
        transfers: Mutex::new(transfers(other, third)),
        ..Fake::new(this)
    };
    let (code, body) = send_json(app(fake, dir.path(), dir.path()), get("/api/transfers")).await;
    assert_eq!(code, StatusCode::OK);
    let content = Hash::new(b"big").to_string();
    let gone = NamespaceId::from(&[3u8; 32]).to_string();
    let (other_short, third_short) = (other.fmt_short().to_string(), third.fmt_short().to_string());
    let (other, third) = (other.to_string(), third.to_string());
    assert_eq!(
        body,
        json!({
            "swarms": [
                {
                    "folder_id": NamespaceId::from(&[2u8; 32]).to_string(),
                    "folder": "photos",
                    "path": "big.bin",
                    "content": content,
                    "size": 10 << 20,
                    "piece_length": 1 << 20,
                    "pieces": 10,
                    "have": 4,
                    "active": 2,
                    "peers": [
                        {
                            "id": other,
                            "label": other_short,
                            "state": "In",
                            "in_swarm": {
                                "unchoked": true,
                                "interested": true,
                                "rate": 1500,
                                "open": 12,
                                "snubbed": false,
                                "pieces": 10,
                            },
                        },
                        { "id": third, "label": third_short, "state": "Banned", "in_swarm": null },
                    ],
                    "stuck": false,
                },
                {
                    "folder_id": gone,
                    "folder": gone,
                    "path": "old.bin",
                    "content": content,
                    "size": 10 << 20,
                    "piece_length": 1 << 20,
                    "pieces": 10,
                    "have": 4,
                    "active": 2,
                    "peers": [],
                    "stuck": true,
                },
            ],
            "uploads": [
                {
                    "id": other,
                    "label": other_short,
                    "streams": 1,
                    "interested": true,
                    "unchoked": true,
                    "optimistic": false,
                    "rate": 99,
                },
                {
                    "id": third,
                    "label": third_short,
                    "streams": 2,
                    "interested": true,
                    "unchoked": true,
                    "optimistic": true,
                    "rate": 0,
                },
            ],
        })
    );
}

#[tokio::test]
async fn transfers_with_none_running() {
    let dir = tempfile::tempdir().unwrap();
    let (code, body) = send_json(no_group(dir.path()), get("/api/transfers")).await;
    assert_eq!(code, StatusCode::OK);
    assert_eq!(body, json!({ "swarms": [], "uploads": [] }));
}

const TICK: Duration = sync_node::ui::topics::TOPIC_INTERVAL;

fn topics_for(fake: &Arc<Fake>) -> Topics {
    Topics::new(fake.clone(), TICK)
}

fn parse(message: &str) -> Value {
    serde_json::from_str(message).unwrap()
}

/// Waits `ticks` timer ticks.
async fn wait(ticks: u32) {
    tokio::time::sleep(TICK * ticks + TICK / 2).await;
}

#[tokio::test(start_paused = true)]
async fn no_reads_without_a_subscription() {
    let fake = Arc::new(Fake::new(id()));
    let topics = topics_for(&fake);
    wait(5).await;
    assert_eq!(fake.reads(), 0);
    assert_eq!(topics.running(), 0);
}

#[tokio::test(start_paused = true)]
async fn subscriptions_share_one_timer() {
    let this = id();
    let fake = Arc::new(Fake::new(this));
    let topics = topics_for(&fake);
    let mut a = topics.subscribe("status").unwrap();
    let mut b = topics.subscribe("status").unwrap();
    let first = a.next().await.unwrap();
    assert_eq!(
        parse(&first),
        json!({
            "topic": "status",
            "update": { "kind": "Status", "data": { "node": this.to_string(), "group": null } },
        })
    );
    assert_eq!(b.next().await.unwrap(), first);
    assert_eq!(topics.running(), 1);
    wait(3).await;
    // The first read, then one per tick: not one per subscription.
    assert_eq!(fake.reads(), 4);
}

#[tokio::test(start_paused = true)]
async fn only_changes_are_sent() {
    let (this, other) = (id(), id());
    let fake = Arc::new(Fake::new(this));
    let topics = topics_for(&fake);
    let mut sub = topics.subscribe("status").unwrap();
    sub.next().await.unwrap();
    // Read every tick, but the same answer isn't sent again.
    let unchanged = tokio::time::timeout(TICK * 5, sub.next()).await;
    assert!(unchanged.is_err(), "sent an unchanged answer");
    assert!(fake.reads() >= 5);
    *fake.status.lock().unwrap() = Some(in_group(this, other));
    let changed = parse(&sub.next().await.unwrap());
    assert_eq!(
        changed["update"]["data"]["group"]["founder"],
        this.to_string()
    );
}

#[tokio::test(start_paused = true)]
async fn a_new_subscription_gets_the_latest_at_once() {
    let fake = Arc::new(Fake::new(id()));
    let topics = topics_for(&fake);
    let mut first = topics.subscribe("status").unwrap();
    let latest = first.next().await.unwrap();
    wait(2).await;
    let reads = fake.reads();
    let mut second = topics.subscribe("status").unwrap();
    assert_eq!(second.next().await.unwrap(), latest);
    assert_eq!(fake.reads(), reads, "read again for the new subscription");
}

#[tokio::test(start_paused = true)]
async fn the_timer_stops_after_the_last_subscription() {
    let fake = Arc::new(Fake::new(id()));
    let topics = topics_for(&fake);
    let mut a = topics.subscribe("status").unwrap();
    let b = topics.subscribe("status").unwrap();
    a.next().await.unwrap();
    drop(a);
    wait(2).await;
    assert_eq!(topics.running(), 1, "stopped with a subscription left");
    drop(b);
    wait(1).await;
    assert_eq!(topics.running(), 0);
    let reads = fake.reads();
    wait(5).await;
    assert_eq!(fake.reads(), reads);
    // A new subscription starts it again.
    let mut c = topics.subscribe("status").unwrap();
    c.next().await.unwrap();
    assert_eq!(topics.running(), 1);
}

#[tokio::test(start_paused = true)]
async fn topic_errors() {
    let (this, other) = (id(), id());
    let fake = Arc::new(Fake {
        status: Mutex::new(Some(in_group(this, other))),
        ..Fake::new(this)
    });
    let topics = topics_for(&fake);
    for name in ["nope", "folders/", "status/x"] {
        let error = topics.subscribe(name).err().expect("an unknown topic");
        assert_eq!(
            parse(&error),
            json!({
                "topic": name,
                "update": { "kind": "Error", "data": { "error": format!("no such UI topic: {name}") } },
            })
        );
    }
    assert_eq!(topics.running(), 0);
    // A read that fails: the same error text as the read route.
    let mut music = topics.subscribe("folders/music").unwrap();
    assert_eq!(
        parse(&music.next().await.unwrap()),
        json!({
            "topic": "folders/music",
            "update": { "kind": "Error", "data": { "error": "this node doesn't keep folder music" } },
        })
    );
    let mut folders = topics.subscribe("folders").unwrap();
    let list = parse(&folders.next().await.unwrap());
    assert_eq!(list["update"]["kind"], "Folders");
    assert_eq!(list["update"]["data"][0]["name"], "photos");
}

#[tokio::test(start_paused = true)]
async fn transfers_topic() {
    let (this, other, third) = (id(), id(), id());
    let fake = Arc::new(Fake::new(this));
    let topics = topics_for(&fake);
    let mut sub = topics.subscribe("transfers").unwrap();
    assert_eq!(
        parse(&sub.next().await.unwrap()),
        json!({
            "topic": "transfers",
            "update": { "kind": "Transfers", "data": { "swarms": [], "uploads": [] } },
        })
    );
    *fake.transfers.lock().unwrap() = transfers(other, third);
    let changed = parse(&sub.next().await.unwrap());
    assert_eq!(changed["update"]["data"]["swarms"][0]["have"], 4);
    assert_eq!(changed["update"]["data"]["uploads"][1]["optimistic"], true);
}

#[tokio::test(start_paused = true)]
async fn stop_ends_every_timer_and_subscription() {
    let fake = Arc::new(Fake::new(id()));
    let topics = topics_for(&fake);
    let mut sub = topics.subscribe("status").unwrap();
    sub.next().await.unwrap();
    let mut folders = topics.subscribe("folders").unwrap();
    folders.next().await.unwrap();
    topics.stop().await;
    assert_eq!(topics.running(), 0);
    assert!(sub.next().await.is_none());
    assert!(folders.next().await.is_none());
    let error = topics
        .subscribe("status")
        .err()
        .expect("refused after stop");
    assert_eq!(
        parse(&error)["update"]["data"]["error"],
        "the node is stopping"
    );
}

#[test]
fn client_messages() {
    assert_eq!(
        serde_json::from_str::<ClientMessage>(r#"{ "subscribe": "folders/photos" }"#).unwrap(),
        ClientMessage::Subscribe("folders/photos".to_string())
    );
    assert_eq!(
        serde_json::from_str::<ClientMessage>(r#"{ "unsubscribe": "status" }"#).unwrap(),
        ClientMessage::Unsubscribe("status".to_string())
    );
    assert!(serde_json::from_str::<ClientMessage>(r#"{ "publish": "status" }"#).is_err());
}

#[tokio::test]
async fn websocket_needs_the_ui_origin() {
    let dir = tempfile::tempdir().unwrap();
    let upgrade = |origin: Option<&str>| {
        let mut request = Request::get("/api/ws")
            .header(header::HOST, ADDR)
            .header(header::CONNECTION, "upgrade")
            .header(header::UPGRADE, "websocket")
            .header(header::SEC_WEBSOCKET_VERSION, "13")
            .header(header::SEC_WEBSOCKET_KEY, "dGhlIHNhbXBsZSBub25jZQ==");
        if let Some(origin) = origin {
            request = request.header(header::ORIGIN, origin);
        }
        request.body(Body::empty()).unwrap()
    };
    for origin in [
        None,
        Some("http://evil.example"),
        Some("http://127.0.0.1:8888"),
        Some("https://127.0.0.1:7777"),
    ] {
        let (code, body) = send_json(no_group(dir.path()), upgrade(origin)).await;
        assert_eq!(code, StatusCode::FORBIDDEN, "Origin {origin:?}");
        assert_eq!(body, json!({ "error": "unknown Origin" }));
    }
    // The UI's own Origin passes the check. `oneshot` has no connection to
    // upgrade, so the upgrade itself is refused after it.
    for origin in ["http://127.0.0.1:7777", "http://localhost:7777"] {
        let (code, _) = send(no_group(dir.path()), upgrade(Some(origin))).await;
        assert_ne!(code, StatusCode::FORBIDDEN, "Origin {origin}");
    }
}

// Actions.

fn post(path: &str, body: Value) -> Request<Body> {
    Request::post(path)
        .header(header::HOST, ADDR)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body.to_string()))
        .unwrap()
}

/// A fake in the group of `in_group` with member `other`.
fn grouped_with(this: EndpointId, other: EndpointId) -> Fake {
    Fake {
        status: Mutex::new(Some(in_group(this, other))),
        ..Fake::new(this)
    }
}

/// A fake in the group of `in_group`, keeping `photos`.
fn grouped(this: EndpointId) -> Fake {
    Fake {
        status: Mutex::new(Some(in_group(this, id()))),
        ..Fake::new(this)
    }
}

#[tokio::test]
async fn create_group() {
    let dir = tempfile::tempdir().unwrap();
    let (code, body) = send_json(no_group(dir.path()), post("/api/group", json!({}))).await;
    assert_eq!(code, StatusCode::OK);
    assert_eq!(body, json!({ "group": new_group().to_string() }));
    // With a name: checked, trimmed and passed on.
    let fake = Fake::new(id());
    let created = fake.created.clone();
    let (code, _) = send_json(
        app(fake, dir.path(), dir.path()),
        post("/api/group", json!({ "name": " home " })),
    )
    .await;
    assert_eq!(code, StatusCode::OK);
    assert_eq!(*created.lock().unwrap(), Some(Some("home".to_string())));
    let fake = Fake::new(id());
    let created = fake.created.clone();
    let (code, body) = send_json(
        app(fake, dir.path(), dir.path()),
        post("/api/group", json!({ "name": "" })),
    )
    .await;
    assert_eq!(code, StatusCode::BAD_REQUEST);
    assert_eq!(body, json!({ "error": "name is empty" }));
    assert_eq!(*created.lock().unwrap(), None);
    // In a group already.
    let fake = grouped(id());
    let (code, body) = send_json(
        app(fake, dir.path(), dir.path()),
        post("/api/group", json!({})),
    )
    .await;
    assert_eq!(code, StatusCode::CONFLICT);
    let group = NamespaceId::from(&[1u8; 32]);
    assert_eq!(
        body,
        json!({ "error": format!("already in group {group}") })
    );
}

#[tokio::test]
async fn join() {
    let dir = tempfile::tempdir().unwrap();
    let member = id();
    let (code, body) = send_json(
        no_group(dir.path()),
        post("/api/join", json!({ "ticket": format!(" {member} ") })),
    )
    .await;
    assert_eq!(code, StatusCode::OK);
    assert_eq!(
        body,
        json!({ "group": new_group().to_string(), "founder": member.to_string() })
    );
    let (code, body) = send_json(
        no_group(dir.path()),
        post("/api/join", json!({ "ticket": "not a ticket" })),
    )
    .await;
    assert_eq!(code, StatusCode::BAD_REQUEST);
    assert_eq!(
        body,
        json!({ "error": "not an invite ticket or a node ID" })
    );
    let fake = grouped(id());
    let (code, _) = send_json(
        app(fake, dir.path(), dir.path()),
        post("/api/join", json!({ "ticket": member.to_string() })),
    )
    .await;
    assert_eq!(code, StatusCode::CONFLICT);
}

#[tokio::test(start_paused = true)]
async fn join_has_a_time_limit() {
    let dir = tempfile::tempdir().unwrap();
    let fake = Fake {
        join_hangs: true,
        ..Fake::new(id())
    };
    let (code, body) = send_json(
        app(fake, dir.path(), dir.path()),
        post("/api/join", json!({ "ticket": id().to_string() })),
    )
    .await;
    assert_eq!(code, StatusCode::GATEWAY_TIMEOUT);
    assert!(
        body["error"]
            .as_str()
            .unwrap()
            .contains("didn't finish within 60s"),
        "{body}"
    );
}

#[tokio::test]
async fn invites() {
    let config = tempfile::tempdir().unwrap();
    let this = id();
    let (code, body) = send_json(
        app(grouped(this), config.path(), config.path()),
        post("/api/invites", json!({ "name": "pi" })),
    )
    .await;
    assert_eq!(code, StatusCode::OK);
    let ticket: InviteTicket = body["ticket"].as_str().unwrap().parse().unwrap();
    assert_eq!(ticket.inviter.id, this);
    assert_eq!(ticket.group, NamespaceId::from(&[1u8; 32]));
    let ttl = ticket.expires_at - sync_node::record::unix_now();
    assert!(
        (23 * 3600..=24 * 3600).contains(&ttl),
        "default 24 h, got {ttl}s"
    );
    let (code, body) = send_json(
        app(grouped(this), config.path(), config.path()),
        post("/api/invites", json!({ "hours": 2 })),
    )
    .await;
    assert_eq!(code, StatusCode::OK);
    let ticket: InviteTicket = body["ticket"].as_str().unwrap().parse().unwrap();
    assert!(ticket.expires_at - sync_node::record::unix_now() <= 2 * 3600);
    let invites = std::fs::read_dir(config.path().join(INVITES_DIR))
        .unwrap()
        .count();
    assert_eq!(invites, 2);
    // Refusals.
    let (code, body) = send_json(
        app(grouped(this), config.path(), config.path()),
        post("/api/invites", json!({ "name": "" })),
    )
    .await;
    assert_eq!(
        (code, body),
        (StatusCode::BAD_REQUEST, json!({ "error": "name is empty" }))
    );
    let (code, body) = send_json(no_group(config.path()), post("/api/invites", json!({}))).await;
    assert_eq!(code, StatusCode::CONFLICT);
    assert_eq!(
        body,
        json!({ "error": "not in a group: run `create` or `join` first" })
    );
}

#[tokio::test]
async fn members_are_queued() {
    let config = tempfile::tempdir().unwrap();
    let new = id();
    let (code, body) = send_json(
        app(grouped(id()), config.path(), config.path()),
        post(
            "/api/members",
            json!({ "id": new.to_string(), "name": "pi" }),
        ),
    )
    .await;
    assert_eq!(code, StatusCode::ACCEPTED);
    assert_eq!(body, json!({ "queued": true, "within_secs": 5 }));
    let queued = config.path().join(ADDS_DIR).join(new.to_string());
    assert_eq!(std::fs::read_to_string(queued).unwrap(), "pi");
    // Refusals.
    for (request, code, error) in [
        (
            json!({ "id": "nope" }),
            StatusCode::BAD_REQUEST,
            "not a node ID: nope".to_string(),
        ),
        (
            json!({ "id": new.to_string(), "name": "a\nb" }),
            StatusCode::BAD_REQUEST,
            "name contains control characters".to_string(),
        ),
        (
            json!({ "name": "pi" }),
            StatusCode::BAD_REQUEST,
            "".to_string(),
        ),
    ] {
        let (got, body) = send_json(
            app(grouped(id()), config.path(), config.path()),
            post("/api/members", request.clone()),
        )
        .await;
        assert_eq!(got, code, "{request}");
        if !error.is_empty() {
            assert_eq!(body, json!({ "error": error }), "{request}");
        } else {
            assert!(
                body["error"]
                    .as_str()
                    .unwrap()
                    .contains("missing field `id`"),
                "{body}"
            );
        }
    }
    let (code, _) = send_json(
        no_group(config.path()),
        post("/api/members", json!({ "id": new.to_string() })),
    )
    .await;
    assert_eq!(code, StatusCode::CONFLICT);
}

#[tokio::test]
async fn removals_are_queued() {
    let config = tempfile::tempdir().unwrap();
    let (this, other) = (id(), id());
    let (code, body) = send_json(
        app(grouped_with(this, other), config.path(), config.path()),
        post("/api/members/remove", json!({ "id": other.to_string() })),
    )
    .await;
    assert_eq!(code, StatusCode::ACCEPTED);
    assert_eq!(body, json!({ "queued": true, "within_secs": 5 }));
    assert!(config
        .path()
        .join(REMOVES_DIR)
        .join(other.to_string())
        .is_file());
    // Refusals. This node is the founder in `in_group`.
    let stranger = id();
    for (request, error) in [
        (json!({ "id": "nope" }), "not a node ID: nope".to_string()),
        (
            json!({ "id": this.to_string() }),
            "the founder can't be removed".to_string(),
        ),
        (
            json!({ "id": stranger.to_string() }),
            format!("{} is not a member", stranger.fmt_short()),
        ),
    ] {
        let (got, body) = send_json(
            app(grouped_with(this, other), config.path(), config.path()),
            post("/api/members/remove", request.clone()),
        )
        .await;
        assert_eq!(got, StatusCode::BAD_REQUEST, "{request}");
        assert_eq!(body, json!({ "error": error }), "{request}");
    }
    // As a member that isn't the founder: not itself.
    let mut status = in_group(other, this);
    status.node = this;
    status.founder = other;
    let fake = Fake {
        status: Mutex::new(Some(status)),
        ..Fake::new(this)
    };
    let (got, body) = send_json(
        app(fake, config.path(), config.path()),
        post("/api/members/remove", json!({ "id": this.to_string() })),
    )
    .await;
    assert_eq!(got, StatusCode::BAD_REQUEST);
    assert_eq!(body, json!({ "error": "a node can't remove itself" }));
    let (code, _) = send_json(
        no_group(config.path()),
        post("/api/members/remove", json!({ "id": other.to_string() })),
    )
    .await;
    assert_eq!(code, StatusCode::CONFLICT);
    assert_eq!(
        std::fs::read_dir(config.path().join(REMOVES_DIR))
            .unwrap()
            .count(),
        1
    );
}

#[tokio::test]
async fn group_names_are_queued() {
    let config = tempfile::tempdir().unwrap();
    let (code, body) = send_json(
        app(grouped(id()), config.path(), config.path()),
        post("/api/group/name", json!({ "name": "  family " })),
    )
    .await;
    assert_eq!(code, StatusCode::ACCEPTED);
    assert_eq!(body, json!({ "queued": true, "within_secs": 5 }));
    let queued = config.path().join(GROUP_NAME_FILE);
    assert_eq!(std::fs::read_to_string(&queued).unwrap(), "family");
    let (code, body) = send_json(
        app(grouped(id()), config.path(), config.path()),
        post("/api/group/name", json!({ "name": " " })),
    )
    .await;
    assert_eq!(code, StatusCode::BAD_REQUEST);
    assert_eq!(body, json!({ "error": "name is empty" }));
    let (code, _) = send_json(
        no_group(config.path()),
        post("/api/group/name", json!({ "name": "x" })),
    )
    .await;
    assert_eq!(code, StatusCode::CONFLICT);
    assert_eq!(std::fs::read_to_string(&queued).unwrap(), "family");
}

#[tokio::test]
async fn folders_are_disabled_and_enabled() {
    let config = tempfile::tempdir().unwrap();
    // The `folders` file `folder add` writes: this node keeps "photos".
    let kept = FolderConfig {
        id: NamespaceId::from(&[2u8; 32]),
        name: "photos".to_string(),
        path: "/home/g/photos".into(),
    };
    std::fs::write(
        config.path().join(folder::FOLDERS_FILE),
        postcard::to_stdvec(&vec![kept]).unwrap(),
    )
    .unwrap();
    let disabled = || std::fs::read_to_string(config.path().join(DISABLED_FILE)).unwrap();
    let (code, body) = send_json(
        app(grouped(id()), config.path(), config.path()),
        post("/api/folders/photos/enabled", json!({ "enabled": false })),
    )
    .await;
    assert_eq!(code, StatusCode::ACCEPTED);
    assert_eq!(body, json!({ "queued": true, "within_secs": 5 }));
    assert_eq!(disabled(), "photos\n");
    let (code, _) = send_json(
        app(grouped(id()), config.path(), config.path()),
        post("/api/folders/photos/enabled", json!({ "enabled": true })),
    )
    .await;
    assert_eq!(code, StatusCode::ACCEPTED);
    assert_eq!(disabled(), "");
    // A folder this node doesn't keep.
    let (code, body) = send_json(
        app(grouped(id()), config.path(), config.path()),
        post("/api/folders/music/enabled", json!({ "enabled": false })),
    )
    .await;
    assert_eq!(code, StatusCode::BAD_REQUEST);
    assert_eq!(
        body,
        json!({ "error": "this node doesn't keep folder music" })
    );
    let (code, _) = send_json(
        no_group(config.path()),
        post("/api/folders/photos/enabled", json!({ "enabled": false })),
    )
    .await;
    assert_eq!(code, StatusCode::CONFLICT);
}

#[tokio::test]
async fn folder_removes_are_queued() {
    let config = tempfile::tempdir().unwrap();
    let kept = FolderConfig {
        id: NamespaceId::from(&[2u8; 32]),
        name: "photos".to_string(),
        path: "/home/g/photos".into(),
    };
    std::fs::write(
        config.path().join(folder::FOLDERS_FILE),
        postcard::to_stdvec(&vec![kept]).unwrap(),
    )
    .unwrap();
    let (code, body) = send_json(
        app(grouped(id()), config.path(), config.path()),
        post("/api/folders/photos/remove", json!({})),
    )
    .await;
    assert_eq!(code, StatusCode::ACCEPTED);
    assert_eq!(body, json!({ "queued": true, "within_secs": 5 }));
    assert!(config
        .path()
        .join(FOLDER_REMOVES_DIR)
        .join("photos")
        .is_file());
    let (code, body) = send_json(
        app(grouped(id()), config.path(), config.path()),
        post("/api/folders/music/remove", json!({})),
    )
    .await;
    assert_eq!(code, StatusCode::BAD_REQUEST);
    assert_eq!(
        body,
        json!({ "error": "this node doesn't keep folder music" })
    );
    let (code, _) = send_json(
        no_group(config.path()),
        post("/api/folders/photos/remove", json!({})),
    )
    .await;
    assert_eq!(code, StatusCode::CONFLICT);
}

#[tokio::test]
async fn a_kept_folder_name_is_refused_at_once() {
    let config = tempfile::tempdir().unwrap();
    let root = tempfile::tempdir().unwrap();
    let kept = FolderConfig {
        id: NamespaceId::from(&[2u8; 32]),
        name: "photos".to_string(),
        path: "/home/g/photos".into(),
    };
    std::fs::write(
        config.path().join(folder::FOLDERS_FILE),
        postcard::to_stdvec(&vec![kept]).unwrap(),
    )
    .unwrap();
    let (code, body) = send_json(
        app(grouped(id()), config.path(), config.path()),
        post(
            "/api/folders",
            json!({ "name": " photos ", "path": root.path() }),
        ),
    )
    .await;
    assert_eq!(code, StatusCode::BAD_REQUEST);
    assert_eq!(
        body,
        json!({ "error": "this node already keeps folder photos at /home/g/photos" })
    );
    assert!(!config.path().join(FOLDER_ADDS_DIR).exists());
}

#[tokio::test]
async fn folders_are_queued() {
    let config = tempfile::tempdir().unwrap();
    let root = tempfile::tempdir().unwrap();
    let (code, body) = send_json(
        app(grouped(id()), config.path(), config.path()),
        post(
            "/api/folders",
            json!({ "name": "music", "path": format!("{}/./", root.path().display()) }),
        ),
    )
    .await;
    assert_eq!(code, StatusCode::ACCEPTED);
    assert_eq!(body, json!({ "queued": true, "within_secs": 5 }));
    let queued = config.path().join(FOLDER_ADDS_DIR).join("music");
    assert_eq!(
        PathBuf::from(std::fs::read_to_string(queued).unwrap()),
        root.path().canonicalize().unwrap()
    );
    // Refusals.
    let file = root.path().join("file");
    std::fs::write(&file, "x").unwrap();
    let missing = root.path().join("missing");
    for (request, error) in [
        (
            json!({ "name": "a/b", "path": root.path() }),
            "folder name contains '/'".to_string(),
        ),
        (
            json!({ "name": "music", "path": "relative/dir" }),
            "relative/dir is not an absolute path".to_string(),
        ),
        (
            json!({ "name": "music", "path": file }),
            format!("{} is not a directory", file.display()),
        ),
        (
            json!({ "name": "music", "path": missing }),
            format!(
                "resolving {}: No such file or directory (os error 2)",
                missing.display()
            ),
        ),
    ] {
        let (code, body) = send_json(
            app(grouped(id()), config.path(), config.path()),
            post("/api/folders", request.clone()),
        )
        .await;
        assert_eq!(code, StatusCode::BAD_REQUEST, "{request}");
        assert_eq!(body, json!({ "error": error }), "{request}");
    }
    let (code, _) = send_json(
        no_group(config.path()),
        post(
            "/api/folders",
            json!({ "name": "music", "path": root.path() }),
        ),
    )
    .await;
    assert_eq!(code, StatusCode::CONFLICT);
}

#[tokio::test]
async fn a_bad_body_is_a_json_400() {
    let dir = tempfile::tempdir().unwrap();
    let request = Request::post("/api/join")
        .header(header::HOST, ADDR)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from("{ not json"))
        .unwrap();
    let (code, body) = send_json(no_group(dir.path()), request).await;
    assert_eq!(code, StatusCode::BAD_REQUEST);
    assert!(body["error"].is_string(), "{body}");
}
