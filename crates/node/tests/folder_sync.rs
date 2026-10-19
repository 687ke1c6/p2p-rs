//! Folder indexes syncing between nodes, and the merged view they agree on.

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

    let first = agreed_view(&[a, b], id, |v| v.paths.len() == 6).await;
    // On both, same content: in sync.
    assert!(first.path("same.jpg").unwrap().in_sync());
    // On one side: the other fetches it.
    assert_eq!(
        state(&first, "only-a.jpg", b.id()),
        Some(NodeState::Missing)
    );
    assert_eq!(
        state(&first, "only-b.jpg", a.id()),
        Some(NodeState::Missing)
    );
    // Made separately, different: a conflict, b's newer version keeps the name.
    let Outcome::Conflict { winner, others } = &first.path("clash.jpg").unwrap().outcome else {
        panic!("clash.jpg should be a conflict");
    };
    assert_eq!(winner.node, b.id());
    assert_eq!(others[0].node, a.id());
    let shown = first.render(
        &|id| if id == a.id() { "a".into() } else { "b".into() },
        false,
    );
    assert!(
        shown.contains("clash.sync-conflict-") && shown.contains("-a.jpg"),
        "{shown}"
    );

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
        file_content(v, "same.jpg") == Some(hash("edit 3"))
            && v.path("gone.jpg")
                .is_some_and(|p| p.outcome == Outcome::Deleted)
            && file_content(v, "both.txt") == Some(hash("edited by a"))
    })
    .await;
    assert_eq!(state(&v, "same.jpg", a.id()), Some(NodeState::Current));
    assert_eq!(state(&v, "same.jpg", b.id()), Some(NodeState::Outdated));
    assert_eq!(state(&v, "gone.jpg", b.id()), Some(NodeState::WillDelete));
    assert_eq!(state(&v, "both.txt", b.id()), Some(NodeState::Missing));

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
    let v = agreed_view(&[&a, &b], b_id, |v| v.paths.len() == 2).await;
    assert_eq!(state(&v, "from-a.jpg", a.id()), Some(NodeState::Current));
    assert_eq!(state(&v, "from-a.jpg", b.id()), Some(NodeState::Missing));
    assert_eq!(state(&v, "from-b.jpg", a.id()), Some(NodeState::Missing));

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
    let v = agreed_view(&all, id, |v| v.paths.len() == 4 && v.nodes.len() == 3).await;
    assert!(v.path("shared.txt").unwrap().in_sync());
    for (i, node) in nodes.iter().enumerate() {
        let path = format!("node-{i}.txt");
        for other in &nodes {
            let expected = if other.id() == node.id() {
                NodeState::Current
            } else {
                NodeState::Missing
            };
            assert_eq!(state(&v, &path, other.id()), Some(expected));
        }
    }

    for node in nodes {
        node.shutdown().await.unwrap();
    }
}
