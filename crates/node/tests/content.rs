//! Content transfer: nodes running with it converge on the same files.

use std::fs;
use std::future::Future;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use iroh::address_lookup::memory::MemoryLookup;
use iroh::SecretKey;
use iroh_blobs::Hash;
use iroh_docs::NamespaceId;
use sync_node::content::{RENAME_GRACE, STALE_STAGING};
use sync_node::index::FileMeta;
use sync_node::pieces::{staging_path, Hasher, MIN_PIECE_LENGTH};
use sync_node::view::Version;
use sync_node::{Config, Network, Node};

const INTERVAL: Duration = Duration::from_millis(200);
const DEBOUNCE: Duration = Duration::from_millis(100);
const TIMEOUT: Duration = Duration::from_secs(30);
const HOUR: Duration = Duration::from_secs(3600);

/// Starts a node with content transfer on.
async fn spawn(dir: &Path, key: SecretKey, book: &MemoryLookup) -> Node {
    spawn_direct_only(dir, key, book, false).await
}

/// Starts a node, with `--direct-only` if `direct_only`.
async fn spawn_direct_only(
    dir: &Path,
    key: SecretKey,
    book: &MemoryLookup,
    direct_only: bool,
) -> Node {
    let config = Config {
        network: Network::Local,
        presence_interval: INTERVAL,
        debounce: DEBOUNCE,
        address_book: Some(book.clone()),
        direct_only,
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

/// Every regular file under `root` but the marker and the node's temporary
/// files: path → contents. Files and directories that vanish while it reads
/// (the node is still working) are skipped.
fn tree(root: &Path) -> Vec<(String, Vec<u8>)> {
    fn walk(root: &Path, dir: &Path, out: &mut Vec<(String, Vec<u8>)>) {
        let Ok(entries) = fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let Ok(kind) = entry.file_type() else {
                continue;
            };
            let rel = path
                .strip_prefix(root)
                .unwrap()
                .to_str()
                .unwrap()
                .to_string();
            let name = entry.file_name();
            let name = name.to_str().unwrap();
            if kind.is_dir() {
                walk(root, &path, out);
            } else if kind.is_file() && rel != ".syncfolder" && !name.starts_with(".sync-node-tmp-")
            {
                if let Ok(bytes) = fs::read(&path) {
                    out.push((rel, bytes));
                }
            }
        }
    }
    let mut out = Vec::new();
    walk(root, root, &mut out);
    out.sort();
    out
}

/// The conflict copies in `tree`.
fn conflict_copies(tree: &[(String, Vec<u8>)]) -> Vec<(String, Vec<u8>)> {
    tree.iter()
        .filter(|(p, _)| p.contains(".sync-conflict-"))
        .cloned()
        .collect()
}

/// The file data `node` has fetched from `peer`: verified pieces, each
/// counted once.
fn fetched(node: &Node, peer: &Node) -> u64 {
    node.fetched().get(&peer.id()).copied().unwrap_or(0)
}

/// All the bytes `node` has received from `peer`, protocol overhead and
/// other traffic included.
fn received(node: &Node, peer: &Node) -> u64 {
    node.peers()
        .iter()
        .find(|p| p.id == peer.id())
        .map(|p| p.traffic.direct_recv + p.traffic.relay_recv)
        .unwrap_or(0)
}

/// A group of a node per directory.
async fn group(dirs: &[tempfile::TempDir], book: &MemoryLookup) -> Vec<Node> {
    group_direct_only(dirs, book, false).await
}

/// A group of a node per directory, all with `--direct-only` if
/// `direct_only`.
async fn group_direct_only(
    dirs: &[tempfile::TempDir],
    book: &MemoryLookup,
    direct_only: bool,
) -> Vec<Node> {
    let mut nodes = Vec::new();
    for dir in dirs {
        nodes.push(spawn_direct_only(dir.path(), SecretKey::generate(), book, direct_only).await);
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
    // The conflict: the newer version keeps the name, and the other is
    // kept beside it on both nodes.
    eventually("the conflict to be resolved", || async {
        let (ta, tb) = (tree(ra), tree(rb));
        ta == tb && conflict_copies(&ta).len() == 1
    })
    .await;
    let t = tree(ra);
    let mut versions = vec![
        read(ra, "clash.txt").unwrap(),
        conflict_copies(&t)[0].1.clone(),
    ];
    versions.sort();
    assert_eq!(versions, [b"a's version".to_vec(), b"b's version".to_vec()]);

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

    assert_eq!(tree(ra), tree(rb));
    // Every path is in sync in the merged view.
    eventually("the view to settle", || async {
        let v = a.folder_view(id).await.unwrap();
        v.paths.iter().all(|p| p.in_sync())
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
    // b already has the bad entries' content, so it can copy it locally
    // rather than fetch it (which a, serving only its own files, refuses):
    // only the path checks stand between it and the escape.
    write(&rb, "decoy.txt", b"evil");

    let id = add_folder(a, "docs", ra, None).await;
    // a writes entries no honest node would: paths leaving the folder, and
    // the marker.
    let doc = a.docs().open(id).await.unwrap().unwrap();
    let author = a.docs().author_default().await.unwrap();
    for path in [
        "../escape.txt",
        "x/../../escape.txt",
        "/abs.txt",
        ".syncfolder",
    ] {
        let mut hasher = Hasher::new(4);
        hasher.update(b"evil");
        let meta = FileMeta::file(
            hasher.finish(),
            4,
            1,
            a.id(),
            Version::default().bumped(a.id()),
        );
        doc.set_bytes(
            author,
            path.as_bytes().to_vec(),
            postcard::to_stdvec(&meta).unwrap(),
        )
        .await
        .unwrap();
    }
    // A safe path whose piece hashes don't describe its content.
    let mut hasher = Hasher::new(4);
    hasher.update(b"evil");
    let mut liar = FileMeta::file(
        hasher.finish(),
        4,
        1,
        a.id(),
        Version::default().bumped(a.id()),
    );
    liar.pieces = vec![Hash::new(b"something else")];
    doc.set_bytes(
        author,
        b"liar.txt".to_vec(),
        postcard::to_stdvec(&liar).unwrap(),
    )
    .await
    .unwrap();
    add_folder(b, "docs", &rb, Some(id)).await;

    eventually("b to fetch the honest file", || async {
        read(&rb, "ok.txt").as_deref() == Some(b"ok")
    })
    .await;
    // Give the refused ones a few passes too, after the wait for content
    // this node holds elsewhere.
    tokio::time::sleep(RENAME_GRACE + INTERVAL * 5).await;
    assert!(!pb.path().join("escape.txt").exists());
    assert!(!Path::new("/abs.txt").exists());
    assert_eq!(read(&rb, ".syncfolder").unwrap(), b"");
    assert!(!outside.join("through.txt").exists());
    assert!(!rb.join("liar.txt").exists());

    for node in nodes {
        node.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn a_partial_transfer_resumes() {
    let dirs: Vec<_> = (0..2).map(|_| tempfile::tempdir().unwrap()).collect();
    let book = MemoryLookup::new();
    let nodes = group(&dirs, &book).await;
    let (a, b) = (&nodes[0], &nodes[1]);
    let (pa, pb) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (ra, rb) = (pa.path(), pb.path().canonicalize().unwrap());
    const SIZE: usize = 8 << 20;
    let data: Vec<u8> = (0..SIZE).map(|i| (i * 7 + i / 4093) as u8).collect();
    let hash = Hash::new(&data);
    write(ra, "big.bin", &data);
    let id = add_folder(a, "docs", ra, None).await;

    // What a fetch stopped half way leaves behind: the staging file at full
    // length, its first half written.
    let mut partial = data[..SIZE / 2].to_vec();
    partial.resize(SIZE, 0);
    fs::write(staging_path(&rb, "big.bin", hash), &partial).unwrap();

    add_folder(b, "docs", &rb, Some(id)).await;
    eventually("b to write big.bin", || async {
        read(&rb, "big.bin").is_some_and(|got| got == data)
    })
    .await;
    // Only the missing half was fetched.
    assert_eq!(fetched(b, a), SIZE as u64 / 2);

    for node in nodes {
        node.shutdown().await.unwrap();
    }
}

/// Two nodes, a and b, holding `data` at `big.bin` in a folder at `roots`.
async fn synced(
    dirs: &[tempfile::TempDir],
    roots: [&Path; 2],
    book: &MemoryLookup,
    data: &[u8],
) -> Vec<Node> {
    let nodes = group(dirs, book).await;
    let (a, b) = (&nodes[0], &nodes[1]);
    write(roots[0], "big.bin", data);
    let id = add_folder(a, "docs", roots[0], None).await;
    add_folder(b, "docs", roots[1], Some(id)).await;
    eventually("b to fetch big.bin", || async {
        read(roots[1], "big.bin").is_some_and(|got| got == data)
    })
    .await;
    nodes
}

const BIG: usize = 10 << 20;

fn big_data() -> Vec<u8> {
    (0..BIG).map(|i| (i * 7 + i / 4093) as u8).collect()
}

#[tokio::test]
async fn an_append_fetches_only_the_new_pieces() {
    let dirs: Vec<_> = (0..2).map(|_| tempfile::tempdir().unwrap()).collect();
    let book = MemoryLookup::new();
    let (pa, pb) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (ra, rb) = (pa.path(), pb.path());
    let mut data = big_data();
    let nodes = synced(&dirs, [ra, rb], &book, &data).await;
    let (a, b) = (&nodes[0], &nodes[1]);
    let before = fetched(b, a);

    const TAIL: usize = 1 << 20;
    data.extend((0..TAIL).map(|i| (i * 11 + 5) as u8));
    write(ra, "big.bin", &data);
    eventually("b to take the append", || async {
        read(rb, "big.bin").is_some_and(|got| got == data)
    })
    .await;
    // The appended pieces only.
    assert_eq!(fetched(b, a) - before, TAIL as u64);

    for node in nodes {
        node.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn an_edit_in_the_middle_fetches_one_piece() {
    let dirs: Vec<_> = (0..2).map(|_| tempfile::tempdir().unwrap()).collect();
    let book = MemoryLookup::new();
    let (pa, pb) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (ra, rb) = (pa.path(), pb.path());
    let mut data = big_data();
    let nodes = synced(&dirs, [ra, rb], &book, &data).await;
    let (a, b) = (&nodes[0], &nodes[1]);
    let before = fetched(b, a);

    data[BIG / 2..BIG / 2 + 100].fill(0xAA);
    write(ra, "big.bin", &data);
    eventually("b to take the edit", || async {
        read(rb, "big.bin").is_some_and(|got| got == data)
    })
    .await;
    assert_eq!(fetched(b, a) - before, u64::from(MIN_PIECE_LENGTH));

    for node in nodes {
        node.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn pieces_are_copied_from_other_files() {
    let dirs: Vec<_> = (0..2).map(|_| tempfile::tempdir().unwrap()).collect();
    let book = MemoryLookup::new();
    let (pa, pb) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (ra, rb) = (pa.path(), pb.path());
    let data = big_data();
    let nodes = synced(&dirs, [ra, rb], &book, &data).await;
    let (a, b) = (&nodes[0], &nodes[1]);
    let before = fetched(b, a);

    // A new file: the first half of big.bin, then new bytes.
    const TAIL: usize = 1 << 20;
    let mut other = data[..BIG / 2].to_vec();
    other.extend((0..TAIL).map(|i| (i * 11 + 5) as u8));
    write(ra, "other.bin", &other);
    eventually("b to fetch other.bin", || async {
        read(rb, "other.bin").is_some_and(|got| got == other)
    })
    .await;
    assert_eq!(read(rb, "big.bin").unwrap(), data);
    // The new bytes only: the first half was copied from big.bin.
    assert_eq!(fetched(b, a) - before, TAIL as u64);

    for node in nodes {
        node.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn stale_staging_files_are_removed() {
    let dirs: Vec<_> = (0..2).map(|_| tempfile::tempdir().unwrap()).collect();
    let book = MemoryLookup::new();
    let nodes = group(&dirs, &book).await;
    let (a, b) = (&nodes[0], &nodes[1]);
    let (pa, pb) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (ra, rb) = (pa.path(), pb.path().canonicalize().unwrap());
    write(ra, "doc.txt", b"current");
    // Left by fetches of versions nobody wants any more, untouched for
    // longer than STALE_STAGING; one as old that only looks like one; and
    // one as unwanted but written recently, which may be about to resume.
    let stale = staging_path(&rb, "doc.txt", Hash::new(b"an old version"));
    let gone = staging_path(&rb, "deleted.txt", Hash::new(b"deleted since"));
    let other = rb.join(".sync-node-tmp-not-a-staging-file");
    let recent = staging_path(&rb, "doc.txt", Hash::new(b"another version"));
    let long_ago = std::time::SystemTime::now() - STALE_STAGING * 2;
    for path in [&stale, &gone, &other, &recent] {
        fs::write(path, b"part of it").unwrap();
    }
    for path in [&stale, &gone, &other] {
        let file = fs::File::options().write(true).open(path).unwrap();
        file.set_modified(long_ago).unwrap();
    }

    let id = add_folder(a, "docs", ra, None).await;
    add_folder(b, "docs", &rb, Some(id)).await;
    eventually("b to fetch doc.txt and clean up", || async {
        read(&rb, "doc.txt").is_some_and(|got| got == b"current")
            && !stale.exists()
            && !gone.exists()
    })
    .await;
    assert!(other.exists() && recent.exists());

    for node in nodes {
        node.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn a_rename_moves_the_file() {
    let dirs: Vec<_> = (0..2).map(|_| tempfile::tempdir().unwrap()).collect();
    let book = MemoryLookup::new();
    let nodes = group(&dirs, &book).await;
    let (a, b) = (&nodes[0], &nodes[1]);
    let (pa, pb) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (ra, rb) = (pa.path(), pb.path());
    const SIZE: usize = 2 << 20;
    let data: Vec<u8> = (0..SIZE).map(|i| (i * 13 + i / 1021) as u8).collect();
    write(ra, "old/photo.bin", &data);
    let id = add_folder(a, "docs", ra, None).await;
    add_folder(b, "docs", rb, Some(id)).await;
    eventually("b to fetch the file", || async {
        read(rb, "old/photo.bin").is_some_and(|got| got == data)
    })
    .await;
    let inode = fs::metadata(rb.join("old/photo.bin")).unwrap().ino();
    let before = fetched(b, a);

    fs::create_dir(ra.join("new")).unwrap();
    fs::rename(ra.join("old/photo.bin"), ra.join("new/photo.bin")).unwrap();
    eventually("b to move the file", || async {
        read(rb, "new/photo.bin").is_some_and(|got| got == data) && !rb.join("old").exists()
    })
    .await;
    // The same file, moved: not fetched again, nor copied.
    assert_eq!(fs::metadata(rb.join("new/photo.bin")).unwrap().ino(), inode);
    assert_eq!(fetched(b, a), before);

    for node in nodes {
        node.shutdown().await.unwrap();
    }
}

/// The plan's "done when" for step 7: three nodes converge through an edit
/// conflict, a delete and a rename, all made while one of them is offline.
#[tokio::test]
async fn three_nodes_converge_with_one_offline() {
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let book = MemoryLookup::new();
    let nodes = group(&dirs, &book).await;
    let mut nodes = nodes.into_iter();
    let (a, b, c) = (
        nodes.next().unwrap(),
        nodes.next().unwrap(),
        nodes.next().unwrap(),
    );
    let roots: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let (ra, rb, rc) = (roots[0].path(), roots[1].path(), roots[2].path());
    write(ra, "edited.txt", b"original");
    write(ra, "deleted.txt", b"to delete");
    write(ra, "renamed.txt", b"to rename");
    write(ra, "kept.txt", b"kept");
    let id = add_folder(&a, "docs", ra, None).await;
    add_folder(&b, "docs", rb, Some(id)).await;
    add_folder(&c, "docs", rc, Some(id)).await;
    eventually("all three to hold the folder", || async {
        let t = tree(ra);
        t.len() == 4 && tree(rb) == t && tree(rc) == t
    })
    .await;

    // c goes offline, and everyone changes things.
    let key = c.endpoint().secret_key().clone();
    c.shutdown().await.unwrap();
    write(rc, "edited.txt", b"c's edit");
    // Written later, so newer: a's edit keeps the name.
    tokio::time::sleep(Duration::from_millis(20)).await;
    write(ra, "edited.txt", b"a's edit");
    fs::remove_file(rb.join("deleted.txt")).unwrap();
    fs::rename(ra.join("renamed.txt"), ra.join("moved.txt")).unwrap();
    eventually("a and b to agree without c", || async {
        let t = tree(ra);
        tree(rb) == t && read(ra, "moved.txt").is_some() && read(ra, "deleted.txt").is_none()
    })
    .await;

    // c comes back.
    let c = spawn(dirs[2].path(), key, &book).await;
    eventually("all three to converge", || async {
        let t = tree(ra);
        tree(rb) == t && tree(rc) == t && conflict_copies(&t).len() == 1
    })
    .await;
    let t = tree(ra);
    let names: Vec<&str> = t.iter().map(|(p, _)| p.as_str()).collect();
    assert_eq!(read(ra, "edited.txt").unwrap(), b"a's edit");
    let copy = &conflict_copies(&t)[0];
    assert_eq!(copy.1, b"c's edit");
    assert!(
        copy.0.starts_with("edited.sync-conflict-")
            && copy.0.ends_with(&format!("-{}.txt", c.id().fmt_short())),
        "{}",
        copy.0
    );
    // The delete stayed deleted and the rename moved, despite c's old copies.
    assert_eq!(
        names,
        [copy.0.as_str(), "edited.txt", "kept.txt", "moved.txt"]
    );
    eventually("the view to settle", || async {
        let v = c.folder_view(id).await.unwrap();
        v.paths.iter().all(|p| p.in_sync())
    })
    .await;

    for node in [a, b, c] {
        node.shutdown().await.unwrap();
    }
}

/// Total size of the regular files under `dir`.
fn dir_size(dir: &Path) -> u64 {
    fs::read_dir(dir)
        .map(|entries| {
            entries
                .flatten()
                .map(|entry| match entry.file_type() {
                    Ok(kind) if kind.is_dir() => dir_size(&entry.path()),
                    Ok(_) => entry.metadata().map(|m| m.len()).unwrap_or(0),
                    Err(_) => 0,
                })
                .sum()
        })
        .unwrap_or(0)
}

/// A file renamed on one node can still be fetched from it by a node that
/// never had it (R4), and a received file is stored once (R1).
#[tokio::test]
async fn a_renamed_file_reaches_a_new_node() {
    let dirs: Vec<_> = (0..2).map(|_| tempfile::tempdir().unwrap()).collect();
    let book = MemoryLookup::new();
    let nodes = group(&dirs, &book).await;
    let (a, c) = (&nodes[0], &nodes[1]);
    let (pa, pc) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (ra, rc) = (pa.path(), pc.path());
    const SIZE: usize = 4 << 20;
    let data: Vec<u8> = (0..SIZE).map(|i| (i * 11 + i / 2039) as u8).collect();
    write(ra, "zzz/original.bin", &data);
    let id = add_folder(a, "docs", ra, None).await;
    eventually("a to index the file", || async {
        a.folder_index(id)
            .await
            .unwrap()
            .contains_key("zzz/original.bin")
    })
    .await;
    // Renamed to a path that sorts first: the case iroh-blobs' store got
    // wrong.
    fs::create_dir(ra.join("aaa")).unwrap();
    fs::rename(ra.join("zzz/original.bin"), ra.join("aaa/renamed.bin")).unwrap();
    eventually("a to index the rename", || async {
        a.folder_index(id)
            .await
            .unwrap()
            .contains_key("aaa/renamed.bin")
    })
    .await;

    let blobs = dirs[1].path().join("blobs");
    let before = dir_size(&blobs);
    add_folder(c, "docs", rc, Some(id)).await;
    eventually("c to fetch the renamed file", || async {
        read(rc, "aaa/renamed.bin").is_some_and(|got| got == data)
    })
    .await;
    assert!(read(rc, "zzz/original.bin").is_none());
    // Stored once: in the folder, not again in the blob store.
    let grown = dir_size(&blobs).saturating_sub(before);
    assert!(
        grown < SIZE as u64 / 8,
        "the blob store grew by {grown} bytes"
    );

    for node in nodes {
        node.shutdown().await.unwrap();
    }
}

/// A peer that can't send hands the rest of the file over to the next.
#[tokio::test]
async fn a_failing_peer_hands_over() {
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let book = MemoryLookup::new();
    let nodes = group(&dirs, &book).await;
    let roots: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    const SIZE: usize = 4 << 20;
    let data: Vec<u8> = (0..SIZE).map(|i| (i * 5 + i / 3001) as u8).collect();
    let (a, b, c) = (&nodes[0], &nodes[1], &nodes[2]);
    write(roots[0].path(), "shared.bin", &data);
    write(roots[1].path(), "shared.bin", &data);
    let id = add_folder(a, "docs", roots[0].path(), None).await;
    add_folder(b, "docs", roots[1].path(), Some(id)).await;
    eventually("a and b to agree", || async {
        let v = a.folder_view(id).await.unwrap();
        v.path("shared.bin").is_some_and(|p| p.in_sync())
    })
    .await;
    // Peers are tried in node ID order: break the first one, which can
    // then no longer read its copy (its index doesn't notice: the size and
    // time are unchanged).
    let (first, second) = if a.id() < b.id() { (0, 1) } else { (1, 0) };
    let broken = roots[first].path().join("shared.bin");
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(&broken, fs::Permissions::from_mode(0o000)).unwrap();

    add_folder(c, "docs", roots[2].path(), Some(id)).await;
    eventually("c to fetch the file", || async {
        read(roots[2].path(), "shared.bin").is_some_and(|got| got == data)
    })
    .await;
    assert_eq!(fetched(c, &nodes[first]), 0);
    assert_eq!(fetched(c, &nodes[second]), SIZE as u64);

    fs::set_permissions(&broken, fs::Permissions::from_mode(0o644)).unwrap();
    for node in nodes {
        node.shutdown().await.unwrap();
    }
}

/// Bytes that don't match the piece hashes are never written, and the node
/// that sent them is dropped rather than asked for the whole file.
#[tokio::test]
async fn peers_share_the_load() {
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let book = MemoryLookup::new();
    let nodes = group(&dirs, &book).await;
    let roots: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    // Several files: a fetch uses the peers c knows when it starts, and
    // c may learn of b's copy only during the first one.
    const FILES: usize = 4;
    const SIZE: usize = 4 << 20;
    let files: Vec<(String, Vec<u8>)> = (0..FILES)
        .map(|f| {
            let data = (0..SIZE).map(|i| (i * 5 + i / 3001 + f) as u8).collect();
            (format!("shared-{f}.bin"), data)
        })
        .collect();
    let (a, b, c) = (&nodes[0], &nodes[1], &nodes[2]);
    for (name, data) in &files {
        write(roots[0].path(), name, data);
        write(roots[1].path(), name, data);
    }
    let id = add_folder(a, "docs", roots[0].path(), None).await;
    add_folder(b, "docs", roots[1].path(), Some(id)).await;
    eventually("a and b to agree", || async {
        let v = a.folder_view(id).await.unwrap();
        files
            .iter()
            .all(|(name, _)| v.path(name).is_some_and(|p| p.in_sync()))
    })
    .await;

    add_folder(c, "docs", roots[2].path(), Some(id)).await;
    eventually("c to fetch the files", || async {
        files
            .iter()
            .all(|(name, data)| read(roots[2].path(), name).is_some_and(|got| got == *data))
    })
    .await;
    // Both sent a part (under load, c may learn of one peer only after
    // most of the data has arrived), and together each piece once.
    let total = (FILES * SIZE) as u64;
    let (from_a, from_b) = (fetched(c, a), fetched(c, b));
    assert!(
        from_a > total / 16 && from_b > total / 16,
        "from a {from_a} bytes, from b {from_b}"
    );
    assert_eq!(from_a + from_b, total);

    for node in nodes {
        node.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn a_new_peer_joins_a_running_fetch() {
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let book = MemoryLookup::new();
    let nodes = group(&dirs, &book).await;
    let roots: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    const SIZE: usize = 32 << 20;
    let data: Vec<u8> = (0..SIZE).map(|i| (i * 5 + i / 3001) as u8).collect();
    let (a, b, c) = (&nodes[0], &nodes[1], &nodes[2]);
    // b holds the file too, but keeps the folder only once c is fetching.
    write(roots[0].path(), "big.bin", &data);
    write(roots[1].path(), "big.bin", &data);
    let id = add_folder(a, "docs", roots[0].path(), None).await;
    eventually("b to see the folder listed", || async {
        b.group_folders()
            .await
            .unwrap()
            .iter()
            .any(|(f, _)| *f == id)
    })
    .await;
    add_folder(c, "docs", roots[2].path(), Some(id)).await;
    eventually("c to be fetching from a", || async {
        fetched(c, a) > 2 << 20
    })
    .await;

    b.add_folder("docs", roots[1].path()).await.unwrap();
    eventually("c to fetch the file", || async {
        read(roots[2].path(), "big.bin").is_some_and(|got| got == data)
    })
    .await;
    let (from_a, from_b) = (fetched(c, a), fetched(c, b));
    assert!(
        from_b > SIZE as u64 / 32,
        "from a {from_a} bytes, from b {from_b}: b didn't join, or c finished first"
    );
    assert_eq!(from_a + from_b, SIZE as u64);

    for node in nodes {
        node.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn pieces_that_dont_match_are_refused() {
    let dirs: Vec<_> = (0..2).map(|_| tempfile::tempdir().unwrap()).collect();
    let book = MemoryLookup::new();
    let nodes = group(&dirs, &book).await;
    let (a, b) = (&nodes[0], &nodes[1]);
    let (pa, pb) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (ra, rb) = (pa.path(), pb.path().canonicalize().unwrap());
    const SIZE: usize = 8 << 20;
    let data: Vec<u8> = (0..SIZE).map(|i| (i * 3 + i / 7919) as u8).collect();
    write(ra, "real.bin", &data);
    let id = add_folder(a, "docs", ra, None).await;
    eventually("a to index the file", || async {
        a.folder_index(id).await.unwrap().contains_key("real.bin")
    })
    .await;
    // a's doc now tells other nodes piece hashes that don't describe the
    // file; a's own index (what it serves by) still has the true one.
    let mut lie = a
        .folder_index(id)
        .await
        .unwrap()
        .remove("real.bin")
        .unwrap();
    for piece in lie.pieces.iter_mut() {
        *piece = Hash::new(piece.as_bytes());
    }
    let doc = a.docs().open(id).await.unwrap().unwrap();
    let author = a.docs().author_default().await.unwrap();
    doc.set_bytes(
        author,
        b"real.bin".to_vec(),
        postcard::to_stdvec(&lie).unwrap(),
    )
    .await
    .unwrap();

    add_folder(b, "docs", &rb, Some(id)).await;
    let staged = staging_path(&rb, "real.bin", Hash::new(&data));
    eventually("b to try", || async { staged.exists() }).await;
    tokio::time::sleep(INTERVAL * 10).await;
    assert!(read(&rb, "real.bin").is_none());
    // Nothing that failed the check reached the staging file.
    assert!(fs::read(&staged).unwrap().iter().all(|byte| *byte == 0));
    // Nothing counted as fetched, and one window of pieces crossed the
    // wire, not the whole file.
    assert_eq!(fetched(b, a), 0);
    let received = received(b, a);
    assert!(received < SIZE as u64 / 4, "received {received} bytes");

    for node in nodes {
        node.shutdown().await.unwrap();
    }
}

/// The staging files under `root`.
fn staging_files(root: &Path) -> Vec<PathBuf> {
    fs::read_dir(root)
        .unwrap()
        .flatten()
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .is_some_and(|n| n.to_string_lossy().starts_with(".sync-node-tmp-"))
        })
        .collect()
}

/// A newer version that wins during a download stops it: its staging file
/// is removed, and the newer version is fetched instead.
#[tokio::test]
async fn a_newer_version_stops_a_running_fetch() {
    let dirs: Vec<_> = (0..2).map(|_| tempfile::tempdir().unwrap()).collect();
    let book = MemoryLookup::new();
    let nodes = group(&dirs, &book).await;
    let (a, b) = (&nodes[0], &nodes[1]);
    let (pa, pb) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (ra, rb) = (pa.path(), pb.path().canonicalize().unwrap());
    const SIZE: usize = 32 << 20;
    let old: Vec<u8> = (0..SIZE).map(|i| (i * 5 + i / 3001) as u8).collect();
    // Shares no piece with the old one, so nothing is copied locally.
    let new: Vec<u8> = (0..SIZE).map(|i| (i * 7 + i / 2003 + 1) as u8).collect();
    write(ra, "big.bin", &old);
    let id = add_folder(a, "docs", ra, None).await;
    add_folder(b, "docs", &rb, Some(id)).await;
    eventually("b to be fetching the old version", || async {
        fetched(b, a) > 1 << 20
    })
    .await;

    write(ra, "big.bin", &new);
    eventually("b to have the new version", || async {
        read(&rb, "big.bin").is_some_and(|got| got == new)
    })
    .await;
    // The old version's download stopped part way, and left nothing.
    assert!(
        fetched(b, a) < 2 * SIZE as u64,
        "fetched {} bytes: the old version finished",
        fetched(b, a)
    );
    assert!(!staging_path(&rb, "big.bin", Hash::new(&old)).exists());
    eventually("no staging file to be left", || async {
        staging_files(&rb).is_empty()
    })
    .await;

    for node in nodes {
        node.shutdown().await.unwrap();
    }
}

/// With `--direct-only`, files still move over direct paths (on one
/// machine, every path is direct).
#[tokio::test]
async fn direct_only_nodes_converge() {
    let dirs: Vec<_> = (0..2).map(|_| tempfile::tempdir().unwrap()).collect();
    let book = MemoryLookup::new();
    let nodes = group_direct_only(&dirs, &book, true).await;
    let (a, b) = (&nodes[0], &nodes[1]);
    let roots: Vec<_> = (0..2).map(|_| tempfile::tempdir().unwrap()).collect();
    let data: Vec<u8> = (0..1usize << 20).map(|i| (i * 3 + i / 977) as u8).collect();
    write(roots[0].path(), "data.bin", &data);
    let id = add_folder(a, "docs", roots[0].path(), None).await;
    add_folder(b, "docs", roots[1].path(), Some(id)).await;
    eventually("b to fetch the file", || async {
        read(roots[1].path(), "data.bin").is_some_and(|got| got == data)
    })
    .await;
    assert_eq!(fetched(b, a), data.len() as u64);

    for node in nodes {
        node.shutdown().await.unwrap();
    }
}

/// A new file reaches two nodes at once, the leechers serving each other the
/// pieces they have verified. On one machine the author's upload has no
/// limit, so the leechers' deep request queues (`choker::queue_length`)
/// take more from it than on a real link: here it uploads up to about 1.5 ×
/// the file. The real ratio is measured on the Pi (BitTorrent plan step 8).
#[tokio::test]
async fn leechers_share_a_new_file() {
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let book = MemoryLookup::new();
    let nodes = group(&dirs, &book).await;
    let roots: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    const SIZE: usize = 32 << 20;
    let data: Vec<u8> = (0..SIZE).map(|i| (i * 11 + i / 4099) as u8).collect();
    let (a, b, c) = (&nodes[0], &nodes[1], &nodes[2]);
    write(roots[0].path(), "new.bin", &data);
    let id = add_folder(a, "docs", roots[0].path(), None).await;
    tokio::join!(
        add_folder(b, "docs", roots[1].path(), Some(id)),
        add_folder(c, "docs", roots[2].path(), Some(id)),
    );
    eventually("b and c to have the file", || async {
        [&roots[1], &roots[2]]
            .iter()
            .all(|root| read(root.path(), "new.bin").is_some_and(|got| got == data))
    })
    .await;
    let from_a = fetched(b, a) + fetched(c, a);
    let (b_from_c, c_from_b) = (fetched(b, c), fetched(c, b));
    assert!(
        from_a < SIZE as u64 * 7 / 4,
        "a uploaded {from_a} bytes; b got {b_from_c} from c, c {c_from_b} from b"
    );
    assert!(
        b_from_c > SIZE as u64 / 10 && c_from_b > SIZE as u64 / 10,
        "b got {b_from_c} bytes from c, c {c_from_b} from b"
    );
    assert_eq!(from_a + b_from_c + c_from_b, 2 * SIZE as u64);

    for node in nodes {
        node.shutdown().await.unwrap();
    }
}
