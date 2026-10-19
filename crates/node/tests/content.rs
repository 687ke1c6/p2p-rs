//! Content transfer: nodes running with it converge on the same files.

use std::fs;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use bao_tree::{ChunkNum, ChunkRanges};
use iroh::address_lookup::memory::MemoryLookup;
use iroh::SecretKey;
use iroh_blobs::api::proto::BlobStatus;
use iroh_blobs::Hash;
use iroh_docs::NamespaceId;
use sync_node::index::FileMeta;
use sync_node::view::Version;
use sync_node::{Config, Network, Node};

const INTERVAL: Duration = Duration::from_millis(200);
const DEBOUNCE: Duration = Duration::from_millis(100);
const TIMEOUT: Duration = Duration::from_secs(30);
const HOUR: Duration = Duration::from_secs(3600);

/// Starts a node with content transfer on.
async fn spawn(dir: &Path, key: SecretKey, book: &MemoryLookup) -> Node {
    let config = Config {
        network: Network::Local,
        presence_interval: INTERVAL,
        debounce: DEBOUNCE,
        address_book: Some(book.clone()),
        content: true,
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

fn write(root: &Path, rel: &str, content: &[u8]) {
    let path = root.join(rel);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, content).unwrap();
}

fn read(root: &Path, rel: &str) -> Option<Vec<u8>> {
    fs::read(root.join(rel)).ok()
}

/// Every regular file under `root` but the marker: path → contents.
fn tree(root: &Path) -> Vec<(String, Vec<u8>)> {
    fn walk(root: &Path, dir: &Path, out: &mut Vec<(String, Vec<u8>)>) {
        for entry in fs::read_dir(dir).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            let kind = entry.file_type().unwrap();
            if kind.is_dir() {
                walk(root, &path, out);
            } else if kind.is_file() {
                let rel = path
                    .strip_prefix(root)
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .to_string();
                if rel != ".syncfolder" {
                    out.push((rel, fs::read(&path).unwrap()));
                }
            }
        }
    }
    let mut out = Vec::new();
    walk(root, root, &mut out);
    out.sort();
    out
}

/// A group of a node per directory.
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

/// `node` keeps folder `name` at `root`, joining it once another node has
/// listed it if `created` is given.
async fn add_folder(
    node: &Node,
    name: &str,
    root: &Path,
    created: Option<NamespaceId>,
) -> NamespaceId {
    if let Some(id) = created {
        eventually("the folder to be listed", || async {
            node.group_folders()
                .await
                .unwrap()
                .iter()
                .any(|(f, _)| *f == id)
        })
        .await;
    }
    node.add_folder(name, root).await.unwrap()
}

#[tokio::test]
async fn two_nodes_converge() {
    let dirs: Vec<_> = (0..2).map(|_| tempfile::tempdir().unwrap()).collect();
    let book = MemoryLookup::new();
    let nodes = group(&dirs, &book).await;
    let (a, b) = (&nodes[0], &nodes[1]);
    let (pa, pb) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (ra, rb) = (pa.path(), pb.path());
    for root in [ra, rb] {
        write(root, "same.txt", b"same");
        write(root, "gone.txt", b"gone");
    }
    write(ra, "only-a.txt", b"a");
    write(ra, "dir/sub/nested.txt", b"nested");
    write(rb, "only-b.txt", b"b");
    write(ra, "clash.txt", b"a's version");
    write(rb, "clash.txt", b"b's version");

    let id = add_folder(a, "docs", ra, None).await;
    add_folder(b, "docs", rb, Some(id)).await;

    // Each fetches what it was missing, with the same modification time.
    eventually("both to hold every file", || async {
        read(rb, "only-a.txt").as_deref() == Some(b"a")
            && read(rb, "dir/sub/nested.txt").as_deref() == Some(b"nested")
            && read(ra, "only-b.txt").as_deref() == Some(b"b")
    })
    .await;
    let mtime = |path: PathBuf| fs::metadata(path).unwrap().modified().unwrap();
    assert_eq!(mtime(ra.join("only-a.txt")), mtime(rb.join("only-a.txt")));
    // A conflict is left alone until step 7.
    assert_eq!(read(ra, "clash.txt").unwrap(), b"a's version");
    assert_eq!(read(rb, "clash.txt").unwrap(), b"b's version");

    // Changes keep flowing: a delete, an edit, and a delete that empties
    // its directories.
    fs::remove_file(ra.join("gone.txt")).unwrap();
    write(rb, "only-a.txt", b"edited by b");
    fs::remove_file(ra.join("dir/sub/nested.txt")).unwrap();
    eventually("the changes to reach the other node", || async {
        read(rb, "gone.txt").is_none()
            && read(ra, "only-a.txt").as_deref() == Some(b"edited by b")
            && !rb.join("dir").exists()
    })
    .await;

    let without_clash = |root| {
        let mut t = tree(root);
        t.retain(|(p, _)| p != "clash.txt");
        t
    };
    assert_eq!(without_clash(ra), without_clash(rb));
    // Every path but the conflict is in sync in the merged view.
    eventually("the view to settle", || async {
        let v = a.folder_view(id).await.unwrap();
        v.paths.iter().all(|p| p.in_sync() || p.path == "clash.txt")
    })
    .await;

    for node in nodes {
        node.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn unsafe_paths_are_refused() {
    let dirs: Vec<_> = (0..2).map(|_| tempfile::tempdir().unwrap()).collect();
    let book = MemoryLookup::new();
    let nodes = group(&dirs, &book).await;
    let (a, b) = (&nodes[0], &nodes[1]);
    let (pa, pb) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    // b's root is one level down, so an escape lands in a directory the
    // test owns.
    let (ra, rb) = (pa.path(), pb.path().join("root"));
    fs::create_dir(&rb).unwrap();
    let outside = pb.path().join("outside");
    fs::create_dir(&outside).unwrap();
    // On b, `link` points out of the folder; on a, it is a real directory.
    std::os::unix::fs::symlink(&outside, rb.join("link")).unwrap();
    write(ra, "link/through.txt", b"through a symlink");
    write(ra, "ok.txt", b"ok");

    let id = add_folder(a, "docs", ra, None).await;
    // a writes entries no honest node would: paths leaving the folder, and
    // the marker.
    let doc = a.docs().open(id).await.unwrap().unwrap();
    let author = a.docs().author_default().await.unwrap();
    let evil = a.blobs().add_bytes(&b"evil"[..]).temp_tag().await.unwrap();
    for path in [
        "../escape.txt",
        "x/../../escape.txt",
        "/abs.txt",
        ".syncfolder",
    ] {
        let meta = FileMeta {
            content: Some(evil.hash()),
            size: 4,
            mtime_ns: 1,
            node: a.id(),
            version: Version::default().bumped(a.id()),
        };
        doc.set_bytes(
            author,
            path.as_bytes().to_vec(),
            postcard::to_stdvec(&meta).unwrap(),
        )
        .await
        .unwrap();
    }
    add_folder(b, "docs", &rb, Some(id)).await;

    eventually("b to fetch the honest file", || async {
        read(&rb, "ok.txt").as_deref() == Some(b"ok")
    })
    .await;
    // Give the refused ones a few passes too.
    tokio::time::sleep(INTERVAL * 5).await;
    assert!(!pb.path().join("escape.txt").exists());
    assert!(!Path::new("/abs.txt").exists());
    assert_eq!(read(&rb, ".syncfolder").unwrap(), b"");
    assert!(!outside.join("through.txt").exists());

    for node in nodes {
        node.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn a_partial_transfer_resumes() {
    let dirs: Vec<_> = (0..2).map(|_| tempfile::tempdir().unwrap()).collect();
    let book = MemoryLookup::new();
    let nodes = group(&dirs, &book).await;
    let mut nodes = nodes.into_iter();
    let (a, b) = (nodes.next().unwrap(), nodes.next().unwrap());
    let (pa, pb) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (ra, rb) = (pa.path(), pb.path());
    const SIZE: usize = 8 << 20;
    let data: Vec<u8> = (0..SIZE).map(|i| (i * 7 + i / 4093) as u8).collect();
    let hash = Hash::new(&data);
    write(ra, "big.bin", &data);
    let id = add_folder(&a, "docs", ra, None).await;
    eventually("a to add big.bin to its store", || async {
        a.blobs().has(hash).await.unwrap()
    })
    .await;

    // b got the first half before the transfer stopped (as a download cut
    // off midway leaves it), then restarted.
    let half = ChunkRanges::from(..ChunkNum((SIZE / 2 / 1024) as u64));
    let bao = a
        .blobs()
        .export_bao(hash, half.clone())
        .bao_to_vec()
        .await
        .unwrap();
    b.blobs().import_bao_bytes(hash, half, bao).await.unwrap();
    let key = b.endpoint().secret_key().clone();
    b.shutdown().await.unwrap();
    let b = spawn(dirs[1].path(), key, &book).await;
    assert!(matches!(
        b.blobs().status(hash).await.unwrap(),
        BlobStatus::Partial { .. }
    ));

    add_folder(&b, "docs", rb, Some(id)).await;
    eventually("b to write big.bin", || async {
        read(rb, "big.bin").is_some_and(|got| got == data)
    })
    .await;
    // Only the missing half crossed the wire, plus overhead.
    let received = b
        .status()
        .unwrap()
        .member(a.id())
        .and_then(|m| m.peer.as_ref())
        .map(|p| p.traffic.direct_recv + p.traffic.relay_recv)
        .unwrap();
    assert!(
        received < (SIZE as u64) * 3 / 4,
        "received {received} bytes for {SIZE}"
    );

    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}
