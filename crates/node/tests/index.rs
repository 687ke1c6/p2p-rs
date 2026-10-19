use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs;
use std::future::Future;
use std::path::Path;
use std::time::{Duration, Instant};

use iroh::SecretKey;
use iroh_blobs::Hash;
use iroh_docs::engine::LiveEvent;
use iroh_docs::store::Query;
use iroh_docs::NamespaceId;
use n0_future::StreamExt;
use sync_node::folder;
use sync_node::index::FileMeta;
use sync_node::view::Version;
use sync_node::{Config, Network, Node};

const INTERVAL: Duration = Duration::from_millis(200);
const DEBOUNCE: Duration = Duration::from_millis(100);
const TIMEOUT: Duration = Duration::from_secs(20);

fn config() -> Config {
    Config {
        network: Network::Local,
        presence_interval: INTERVAL,
        debounce: DEBOUNCE,
        address_book: None,
    }
}

async fn spawn(dir: &Path, key: SecretKey) -> Node {
    Node::spawn_with(dir, key, config()).await.unwrap()
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
        tokio::time::sleep(DEBOUNCE / 2).await;
    }
}

/// A version only `node` has edited, `edits` times.
fn version(node: &Node, edits: u64) -> Version {
    Version([(node.id(), edits)].into())
}

fn hash(content: &str) -> Hash {
    Hash::new(content.as_bytes())
}

fn write(root: &Path, rel: &str, content: &str) {
    let path = root.join(rel);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, content).unwrap();
}

async fn index(node: &Node, id: NamespaceId) -> HashMap<String, FileMeta> {
    node.folder_index(id).await.unwrap()
}

/// Live paths and their content.
async fn live(node: &Node, id: NamespaceId) -> BTreeMap<String, Hash> {
    index(node, id)
        .await
        .into_iter()
        .filter_map(|(path, meta)| meta.content.map(|hash| (path, hash)))
        .collect()
}

async fn tombstones(node: &Node, id: NamespaceId) -> BTreeSet<String> {
    index(node, id)
        .await
        .into_iter()
        .filter(|(_, meta)| meta.is_deleted())
        .map(|(path, _)| path)
        .collect()
}

/// Waits until the live index is exactly `expected`.
async fn wait_live(node: &Node, id: NamespaceId, expected: &[(&str, &str)]) {
    let expected: BTreeMap<String, Hash> = expected
        .iter()
        .map(|(path, content)| (path.to_string(), hash(content)))
        .collect();
    let start = Instant::now();
    loop {
        let now = live(node, id).await;
        if now == expected {
            return;
        }
        if start.elapsed() > TIMEOUT {
            let keys = |m: &BTreeMap<String, Hash>| m.keys().cloned().collect::<Vec<_>>();
            panic!("index has {:?}, expected {:?}", keys(&now), keys(&expected));
        }
        tokio::time::sleep(DEBOUNCE / 2).await;
    }
}

/// The doc timestamp of each of this node's entries.
async fn entry_times(node: &Node, id: NamespaceId) -> HashMap<String, u64> {
    let doc = node.docs().open(id).await.unwrap().unwrap();
    let entries = doc
        .get_many(Query::all())
        .await
        .unwrap()
        .collect::<Vec<_>>()
        .await;
    entries
        .into_iter()
        .map(|entry| {
            let entry = entry.unwrap();
            (
                String::from_utf8(entry.key().to_vec()).unwrap(),
                entry.timestamp(),
            )
        })
        .collect()
}

#[tokio::test]
async fn index_tracks_edits_renames_deletes_and_restarts() {
    let config_dir = tempfile::tempdir().unwrap();
    let folder = tempfile::tempdir().unwrap();
    let root = folder.path();
    let ignore = "*.log\nbuild/\n";
    write(root, ".syncignore", ignore);
    write(root, "a.txt", "one");
    write(root, "sub/b.txt", "two");
    write(root, "x.log", "ignored");
    write(root, "build/out.bin", "ignored");
    fs::create_dir(root.join("empty")).unwrap();
    std::os::unix::fs::symlink(root.join("a.txt"), root.join("link")).unwrap();

    let key = SecretKey::generate();
    let node = spawn(config_dir.path(), key.clone()).await;
    node.create_group().await.unwrap();
    let id = node.add_folder("docs", root).await.unwrap();

    // The scan finds regular files only, minus ignored ones.
    wait_live(
        &node,
        id,
        &[
            (".syncignore", ignore),
            ("a.txt", "one"),
            ("sub/b.txt", "two"),
        ],
    )
    .await;

    // Edit: the new version replaces the old one.
    write(root, "a.txt", "one, edited");
    wait_live(
        &node,
        id,
        &[
            (".syncignore", ignore),
            ("a.txt", "one, edited"),
            ("sub/b.txt", "two"),
        ],
    )
    .await;
    // Created, then edited once.
    assert_eq!(index(&node, id).await["a.txt"].version, version(&node, 2));

    // Same size, rewritten at once: still noticed.
    write(root, "a.txt", "ONE, EDITED");
    eventually("a same-size rewrite to be indexed", || async {
        live(&node, id).await.get("a.txt") == Some(&hash("ONE, EDITED"))
    })
    .await;

    // Rename a file: old path tombstoned, new path live.
    fs::rename(root.join("sub/b.txt"), root.join("sub/c.txt")).unwrap();
    // Delete a file.
    fs::remove_file(root.join("a.txt")).unwrap();
    // A new directory with a file, then the directory renamed.
    write(root, "new/deep/d.txt", "four");
    wait_live(
        &node,
        id,
        &[
            (".syncignore", ignore),
            ("sub/c.txt", "two"),
            ("new/deep/d.txt", "four"),
        ],
    )
    .await;
    fs::rename(root.join("new"), root.join("moved")).unwrap();
    wait_live(
        &node,
        id,
        &[
            (".syncignore", ignore),
            ("sub/c.txt", "two"),
            ("moved/deep/d.txt", "four"),
        ],
    )
    .await;
    let index_now = index(&node, id).await;
    // Created, then deleted by the rename.
    assert_eq!(index_now["sub/b.txt"].version, version(&node, 2));
    // Created, edited twice, deleted.
    assert_eq!(index_now["a.txt"].version, version(&node, 4));
    assert_eq!(
        tombstones(&node, id).await,
        ["a.txt", "new/deep/d.txt", "sub/b.txt"]
            .map(String::from)
            .into()
    );

    // Ignoring a file doesn't tombstone it; un-ignoring indexes it.
    let ignore = "build/\nsub/\n";
    write(root, ".syncignore", ignore);
    wait_live(
        &node,
        id,
        &[
            (".syncignore", ignore),
            ("sub/c.txt", "two"),
            ("moved/deep/d.txt", "four"),
            ("x.log", "ignored"),
        ],
    )
    .await;

    // Changes while the node is down are picked up on restart, and an
    // unchanged file isn't rewritten.
    let before = entry_times(&node, id).await;
    node.shutdown().await.unwrap();
    write(root, "x.log", "changed while down");
    fs::remove_file(root.join("moved/deep/d.txt")).unwrap();
    write(root, "e.txt", "five");
    let node = spawn(config_dir.path(), key).await;
    assert_eq!(node.folders().len(), 1);
    wait_live(
        &node,
        id,
        &[
            (".syncignore", ignore),
            ("sub/c.txt", "two"),
            ("x.log", "changed while down"),
            ("e.txt", "five"),
        ],
    )
    .await;
    let after = entry_times(&node, id).await;
    assert_eq!(before[".syncignore"], after[".syncignore"]);
    assert_eq!(before["sub/c.txt"], after["sub/c.txt"]);

    let status = node.status().unwrap();
    assert_eq!(status.folders[0].stats.files, 4);
    assert!(status.render().contains("docs"), "{}", status.render());

    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn burst_of_changes_yields_one_update_per_file() {
    let config_dir = tempfile::tempdir().unwrap();
    let folder = tempfile::tempdir().unwrap();
    let root = folder.path();
    let node = spawn(config_dir.path(), SecretKey::generate()).await;
    node.create_group().await.unwrap();
    let id = node.add_folder("burst", root).await.unwrap();
    // Let the first (empty) scan finish.
    tokio::time::sleep(DEBOUNCE * 3).await;

    let doc = node.docs().open(id).await.unwrap().unwrap();
    let mut events = doc.subscribe().await.unwrap();
    for round in 0..20 {
        for file in 0..10 {
            write(root, &format!("f{file}.txt"), &format!("round {round}"));
        }
    }
    let expected: Vec<(String, String)> = (0..10)
        .map(|file| (format!("f{file}.txt"), "round 19".to_string()))
        .collect();
    let expected: Vec<(&str, &str)> = expected
        .iter()
        .map(|(p, c)| (p.as_str(), c.as_str()))
        .collect();
    wait_live(&node, id, &expected).await;
    // Anything still pending would show up within a few intervals.
    tokio::time::sleep(DEBOUNCE * 5).await;

    let mut writes: HashMap<String, usize> = HashMap::new();
    while let Ok(Some(event)) = tokio::time::timeout(Duration::from_millis(50), events.next()).await
    {
        if let LiveEvent::InsertLocal { entry } = event.unwrap() {
            *writes
                .entry(String::from_utf8(entry.key().to_vec()).unwrap())
                .or_default() += 1;
        }
    }
    assert_eq!(writes.len(), 10, "{writes:?}");
    assert!(writes.values().all(|n| *n == 1), "{writes:?}");

    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn queued_folder_add_is_applied_once_per_name() {
    let config_dir = tempfile::tempdir().unwrap();
    let one = tempfile::tempdir().unwrap();
    let two = tempfile::tempdir().unwrap();
    write(one.path(), "hello.txt", "hi");
    let node = spawn(config_dir.path(), SecretKey::generate()).await;
    node.create_group().await.unwrap();

    folder::queue_add(config_dir.path(), "shared", one.path()).unwrap();
    eventually("the queued folder to be added", || async {
        node.folders().len() == 1
    })
    .await;
    let id = node.folders()[0].config.id;
    wait_live(&node, id, &[("hello.txt", "hi")]).await;

    // The name is taken: the second request is dropped.
    folder::queue_add(config_dir.path(), "shared", two.path()).unwrap();
    let adds = config_dir.path().join(folder::FOLDER_ADDS_DIR);
    eventually("the queue to empty", || async {
        fs::read_dir(&adds).map_or(true, |mut entries| entries.next().is_none())
    })
    .await;
    assert_eq!(node.folders().len(), 1);
    assert_eq!(
        node.folders()[0].config.path,
        one.path().canonicalize().unwrap()
    );

    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn missing_root_pauses_the_folder_instead_of_deleting() {
    let config_dir = tempfile::tempdir().unwrap();
    let parent = tempfile::tempdir().unwrap();
    let root = parent.path().join("photos");
    let away = parent.path().join("photos-moved");
    write(&root, "a.jpg", "a");
    write(&root, "2026/b.jpg", "b");
    let node = spawn(config_dir.path(), SecretKey::generate()).await;
    node.create_group().await.unwrap();
    let id = node.add_folder("photos", &root).await.unwrap();
    wait_live(&node, id, &[("a.jpg", "a"), ("2026/b.jpg", "b")]).await;
    assert!(root.join(sync_node::index::MARKER).is_file());

    // The root goes away, as when a drive is unmounted: paused, nothing
    // tombstoned.
    fs::rename(&root, &away).unwrap();
    eventually("the folder to pause", || async { node.folders()[0].paused }).await;
    tokio::time::sleep(INTERVAL * 3).await;
    assert!(tombstones(&node, id).await.is_empty());
    assert!(node.status().unwrap().render().contains("paused"));

    // An empty directory in its place isn't the folder either.
    fs::create_dir(&root).unwrap();
    tokio::time::sleep(INTERVAL * 3).await;
    assert!(node.folders()[0].paused);
    assert!(tombstones(&node, id).await.is_empty());

    // Back again: resumed, and changes made meanwhile are picked up.
    fs::remove_dir(&root).unwrap();
    write(&away, "c.jpg", "c");
    fs::rename(&away, &root).unwrap();
    eventually("the folder to resume", || async {
        !node.folders()[0].paused
    })
    .await;
    wait_live(
        &node,
        id,
        &[("a.jpg", "a"), ("2026/b.jpg", "b"), ("c.jpg", "c")],
    )
    .await;
    assert!(tombstones(&node, id).await.is_empty());

    node.shutdown().await.unwrap();
}
