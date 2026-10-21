//! Folder indexes syncing between nodes, the merged view they agree on, and
//! the files it brings to each node.

use std::fs;
use std::future::Future;
use std::path::Path;
use std::time::{Duration, Instant};

use iroh::address_lookup::memory::MemoryLookup;
use iroh::{EndpointId, SecretKey};
use iroh_blobs::Hash;
use iroh_docs::NamespaceId;
use sync_node::view::{FolderView, NodeState, Outcome};
use sync_node::{Config, Network, Node};

const INTERVAL: Duration = Duration::from_millis(200);
const DEBOUNCE: Duration = Duration::from_millis(100);
const TIMEOUT: Duration = Duration::from_secs(30);
const HOUR: Duration = Duration::from_secs(3600);

/// Starts a node that publishes its address to `book`, the test's stand-in
/// for n0's DNS, and finds the others there.
async fn spawn(dir: &Path, key: SecretKey, book: &MemoryLookup) -> Node {
    let config = Config {
        network: Network::Local,
        presence_interval: INTERVAL,
        debounce: DEBOUNCE,
        address_book: Some(book.clone()),
        direct_only: false,
    };
    Node::spawn_with(dir, key, config).await.unwrap()
}

/// Waits until `check` holds, or panics with `what`.
async fn eventually<F, Fut>(what: &str, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let start = Instant::now();
    while !check().await {
        assert!(start.elapsed() < TIMEOUT, "timed out waiting for {what}");
        tokio::time::sleep(INTERVAL / 2).await;
    }
}

fn write(root: &Path, rel: &str, content: &str) {
    let path = root.join(rel);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, content).unwrap();
}

fn hash(content: &str) -> Hash {
    Hash::new(content)
}

async fn view(node: &Node, id: NamespaceId) -> FolderView {
    node.folder_view(id).await.unwrap()
}

/// Waits until every node keeping folder `id` has the same view, and it
/// passes `check`. Returns it.
async fn agreed_view(
    nodes: &[&Node],
    id: NamespaceId,
    check: impl Fn(&FolderView) -> bool,
) -> FolderView {
    let start = Instant::now();
    loop {
        let mut views = Vec::new();
        for node in nodes {
            views.push(view(node, id).await);
        }
        let same = views.windows(2).all(|w| w[0] == w[1]);
        if same && check(&views[0]) {
            return views.remove(0);
        }
        if start.elapsed() > TIMEOUT {
            for (node, view) in nodes.iter().zip(&views) {
                eprintln!(
                    "{}:\n{}",
                    node.id().fmt_short(),
                    view.render(&|id| id.fmt_short().to_string(), true)
                );
            }
            panic!("the nodes' views didn't agree (same: {same})");
        }
        tokio::time::sleep(INTERVAL / 2).await;
    }
}

fn state(view: &FolderView, path: &str, node: EndpointId) -> Option<NodeState> {
    view.path(path)?.nodes.get(&node).copied()
}

/// Whether every path is in sync on every node: content transfer done.
fn settled(view: &FolderView) -> bool {
    view.paths.iter().all(|p| p.in_sync())
}

fn read(root: &Path, rel: &str) -> Option<String> {
    fs::read_to_string(root.join(rel)).ok()
}

fn file_content(view: &FolderView, path: &str) -> Option<Hash> {
    match &view.path(path)?.outcome {
        Outcome::File(c) => Some(c.content),
        _ => None,
    }
}

/// A group of a node per directory, all able to reach each other.
async fn group(dirs: &[tempfile::TempDir], book: &MemoryLookup) -> Vec<Node> {
    let mut nodes = Vec::new();
    for dir in dirs {
        nodes.push(spawn(dir.path(), SecretKey::generate(), book).await);
    }
    nodes[0].create_group().await.unwrap();
    for node in &nodes[1..] {
        node.join(&nodes[0].create_invite(HOUR, None).unwrap())
            .await
            .unwrap();
    }
    nodes
}

/// Waits until `node` has `count` files indexed in its only folder.
async fn indexed(node: &Node, count: usize) {
    eventually("the folder to be indexed", || async {
        node.folders()
            .first()
            .is_some_and(|f| f.stats.files == count)
    })
    .await;
}

#[tokio::test]
async fn two_nodes_merge_overlapping_folders() {
    let dirs: Vec<_> = (0..2).map(|_| tempfile::tempdir().unwrap()).collect();
    let book = MemoryLookup::new();
    let nodes = group(&dirs, &book).await;
    let (a, b) = (&nodes[0], &nodes[1]);
    let (pa, pb) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (ra, rb) = (pa.path(), pb.path());
    for root in [ra, rb] {
        write(root, "same.jpg", "same");
        write(root, "both.txt", "both");
        write(root, "gone.jpg", "gone");
    }
    write(ra, "only-a.jpg", "a");
    write(rb, "only-b.jpg", "b");
    write(ra, "clash.jpg", "a's version");
    // Written later, so newer: b's version keeps the name.
    tokio::time::sleep(Duration::from_millis(20)).await;
    write(rb, "clash.jpg", "b's version");

    // a creates "pictures"; b, once it sees it listed, joins it.
    let id = a.add_folder("pictures", ra).await.unwrap();
    eventually("b to see the folder listed", || async {
        b.group_folders()
            .await
            .unwrap()
            .iter()
            .any(|(f, _)| *f == id)
    })
    .await;
    assert_eq!(b.add_folder("pictures", rb).await.unwrap(), id);
    indexed(a, 5).await;
    indexed(b, 5).await;

    // Seven paths once the conflict copy is made, all in sync.
    let first = agreed_view(&[a, b], id, |v| v.paths.len() == 7 && settled(v)).await;
    // On one side: the other fetched it.
    assert_eq!(read(rb, "only-a.jpg").as_deref(), Some("a"));
    assert_eq!(read(ra, "only-b.jpg").as_deref(), Some("b"));
    // Made separately, different: a conflict. b's newer version keeps the
    // name; a's is kept as a conflict copy named after a, on both.
    assert_eq!(file_content(&first, "clash.jpg"), Some(hash("b's version")));
    let copy = first
        .paths
        .iter()
        .map(|p| p.path.clone())
        .find(|p| p.starts_with("clash.sync-conflict-"))
        .expect("a conflict copy");
    assert!(copy.ends_with(".jpg"), "{copy}");
    for root in [ra, rb] {
        assert_eq!(read(root, "clash.jpg").as_deref(), Some("b's version"));
        assert_eq!(read(root, &copy).as_deref(), Some("a's version"));
    }

    // a edits same.jpg three times while b holds the old version: no conflict.
    for edit in ["edit 1", "edit 2", "edit 3"] {
        write(ra, "same.jpg", edit);
        eventually("a to index its edit", || async {
            a.folder_index(id).await.unwrap()["same.jpg"].content == Some(hash(edit))
        })
        .await;
    }
    // a deletes gone.jpg, which b still has.
    fs::remove_file(ra.join("gone.jpg")).unwrap();
    // a edits both.txt while b deletes it: the edit wins.
    write(ra, "both.txt", "edited by a");
    fs::remove_file(rb.join("both.txt")).unwrap();

    let v = agreed_view(&[a, b], id, |v| {
        settled(v)
            && file_content(v, "same.jpg") == Some(hash("edit 3"))
            && v.path("gone.jpg")
                .is_some_and(|p| p.outcome == Outcome::Deleted)
            && file_content(v, "both.txt") == Some(hash("edited by a"))
    })
    .await;
    assert_eq!(state(&v, "same.jpg", b.id()), Some(NodeState::Current));
    assert_eq!(read(rb, "same.jpg").as_deref(), Some("edit 3"));
    assert!(read(rb, "gone.jpg").is_none());
    assert_eq!(read(rb, "both.txt").as_deref(), Some("edited by a"));
    assert!(!v
        .paths
        .iter()
        .any(|p| p.path.starts_with("same.sync-conflict-")));

    for node in nodes {
        node.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn folders_created_apart_settle_on_one() {
    let dirs: Vec<_> = (0..2).map(|_| tempfile::tempdir().unwrap()).collect();
    let book = MemoryLookup::new();
    let nodes = group(&dirs, &book).await;
    let keys: Vec<SecretKey> = nodes
        .iter()
        .map(|n| n.endpoint().secret_key().clone())
        .collect();
    for node in nodes {
        node.shutdown().await.unwrap();
    }
    let (pa, pb) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    write(pa.path(), "from-a.jpg", "a");
    write(pb.path(), "from-b.jpg", "b");

    // Each creates "pictures" while the other is down: b first.
    let b = spawn(dirs[1].path(), keys[1].clone(), &book).await;
    let b_id = b.add_folder("pictures", pb.path()).await.unwrap();
    b.shutdown().await.unwrap();
    let a = spawn(dirs[0].path(), keys[0].clone(), &book).await;
    let a_id = a.add_folder("pictures", pa.path()).await.unwrap();
    assert_ne!(a_id, b_id);

    // Together again: both settle on b's, created first.
    let b = spawn(dirs[1].path(), keys[1].clone(), &book).await;
    eventually("a to switch to b's folder", || async {
        a.folders().first().is_some_and(|f| f.config.id == b_id)
    })
    .await;
    assert_eq!(b.folders()[0].config.id, b_id);
    agreed_view(&[&a, &b], b_id, |v| v.paths.len() == 2 && settled(v)).await;
    for root in [pa.path(), pb.path()] {
        assert_eq!(read(root, "from-a.jpg").as_deref(), Some("a"));
        assert_eq!(read(root, "from-b.jpg").as_deref(), Some("b"));
    }

    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}

#[tokio::test]
async fn three_nodes_agree() {
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let book = MemoryLookup::new();
    let nodes = group(&dirs, &book).await;
    let roots: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    for (i, root) in roots.iter().enumerate() {
        write(root.path(), "shared.txt", "everyone");
        write(root.path(), &format!("node-{i}.txt"), "mine");
    }
    let id = nodes[0].add_folder("docs", roots[0].path()).await.unwrap();
    for (node, root) in nodes[1..].iter().zip(&roots[1..]) {
        eventually("the folder to be listed", || async {
            node.group_folders()
                .await
                .unwrap()
                .iter()
                .any(|(f, _)| *f == id)
        })
        .await;
        assert_eq!(node.add_folder("docs", root.path()).await.unwrap(), id);
    }

    let all: Vec<&Node> = nodes.iter().collect();
    agreed_view(&all, id, |v| {
        v.paths.len() == 4 && v.nodes.len() == 3 && settled(v)
    })
    .await;
    // Every node has every node's file.
    for root in &roots {
        assert_eq!(read(root.path(), "shared.txt").as_deref(), Some("everyone"));
        for i in 0..3 {
            assert_eq!(
                read(root.path(), &format!("node-{i}.txt")).as_deref(),
                Some("mine")
            );
        }
    }

    for node in nodes {
        node.shutdown().await.unwrap();
    }
}
