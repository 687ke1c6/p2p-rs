use std::net::SocketAddr;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::body::Body;
use axum::http::{header, Method, Request, StatusCode};
use axum::Router;
use iroh::{EndpointId, SecretKey};
use iroh_docs::NamespaceId;
use serde_json::{json, Value};
use sync_node::folder::{FolderConfig, FolderState};
use sync_node::index::IndexStats;
use sync_node::peers::{LinkKind, PeerSnapshot, Traffic};
use sync_node::presence::LinkState;
use sync_node::status::{Link, MemberStatus};
use sync_node::ui::{self, UiBackend, UiConfig};
use sync_node::Status;
use tower::ServiceExt;

const ADDR: &str = "127.0.0.1:7777";

/// A node as the handlers see it, with whatever state the test gives it.
struct Fake {
    id: EndpointId,
    status: Mutex<Option<Status>>,
}

impl UiBackend for Fake {
    fn id(&self) -> EndpointId {
        self.id
    }

    fn status(&self) -> Option<Status> {
        self.status.lock().unwrap().clone()
    }
}

fn id() -> EndpointId {
    SecretKey::generate().public()
}

fn app(fake: Fake, ui_dir: &Path, config_dir: &Path) -> Router {
    ui::router(
        Arc::new(fake),
        UiConfig {
            addr: ADDR.parse::<SocketAddr>().unwrap(),
            ui_dir: ui_dir.to_owned(),
            config_dir: config_dir.to_owned(),
        },
    )
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

/// A group of two: this node (the founder) and a member it is connected to
/// directly, keeping one folder.
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
            peers: 1,
        }],
    }
}

#[tokio::test]
async fn status_in_no_group() {
    let dir = tempfile::tempdir().unwrap();
    let this = id();
    let fake = Fake {
        id: this,
        status: Mutex::new(None),
    };
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
        id: this,
        status: Mutex::new(Some(status.clone())),
    };
    let (code, body) = send_json(app(fake, dir.path(), dir.path()), get("/api/status")).await;
    assert_eq!(code, StatusCode::OK);
    let (this, other) = (this.to_string(), other.to_string());
    assert_eq!(
        body,
        json!({
            "node": this,
            "group": {
                "id": status.group.to_string(),
                "founder": this,
                "generated_at": 1_700_000_000,
                "members": [
                    {
                        "id": this,
                        "name": "laptop",
                        "added_by": this,
                        "this_node": true,
                        "online": true,
                        "last_heard_secs": null,
                        "peer": null,
                    },
                    {
                        "id": other,
                        "name": null,
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
                "links": [{ "a": this, "b": other, "state": "Direct" }],
                "folders": [{
                    "id": NamespaceId::from(&[2u8; 32]).to_string(),
                    "name": "photos",
                    "path": "/home/g/photos",
                    "files": 5,
                    "bytes": 1234,
                    "paused": false,
                    "peers": 1,
                }],
            },
        })
    );
}

fn no_group(dir: &Path) -> Router {
    let fake = Fake {
        id: id(),
        status: Mutex::new(None),
    };
    app(fake, dir, dir)
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
