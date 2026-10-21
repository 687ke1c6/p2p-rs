//! Serving pieces: a node serves exactly the pieces its index describes, to
//! members that keep the folder, and refuses everything else.

use std::fs;
use std::future::Future;
use std::path::Path;
use std::time::{Duration, Instant};

use iroh::address_lookup::memory::MemoryLookup;
use iroh::endpoint::Connection;
use iroh::SecretKey;
use iroh_blobs::Hash;
use iroh_docs::NamespaceId;
use sync_node::index::FileMeta;
use sync_node::peer_wire::{self, request_piece, PieceRequest, Refusal};
use sync_node::pieces::MIN_PIECE_LENGTH;
use sync_node::{Config, Network, Node};

const INTERVAL: Duration = Duration::from_millis(200);
const TIMEOUT: Duration = Duration::from_secs(20);
const HOUR: Duration = Duration::from_secs(3600);

/// A node with content transfer off: only the peer-wire server runs.
async fn spawn(dir: &Path, book: &MemoryLookup) -> Node {
    let config = Config {
        network: Network::Local,
        presence_interval: INTERVAL,
        debounce: Duration::from_millis(100),
        address_book: Some(book.clone()),
    };
    Node::spawn_with(dir, SecretKey::generate(), config)
        .await
        .unwrap()
}

async fn eventually<F, Fut>(what: &str, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let start = Instant::now();
    while !check().await {
        assert!(start.elapsed() < TIMEOUT, "timed out waiting for {what}");
        tokio::time::sleep(INTERVAL / 4).await;
    }
}

fn data(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i * 17 + i / 509) as u8).collect()
}

async fn entry(node: &Node, id: NamespaceId, path: &str) -> Option<FileMeta> {
    node.folder_index(id).await.unwrap().remove(path)
}

fn request(id: NamespaceId, path: &str, content: Hash, index: u32) -> PieceRequest {
    PieceRequest {
        folder: id,
        path: path.to_string(),
        content,
        index,
    }
}

async fn ask(conn: &Connection, request: &PieceRequest) -> Result<Vec<u8>, Refusal> {
    request_piece(conn, request).await.unwrap()
}

#[tokio::test]
async fn a_node_serves_what_its_index_describes() {
    let book = MemoryLookup::new();
    let dirs: Vec<_> = (0..2).map(|_| tempfile::tempdir().unwrap()).collect();
    let (a, b) = (
        spawn(dirs[0].path(), &book).await,
        spawn(dirs[1].path(), &book).await,
    );
    a.create_group().await.unwrap();
    b.join(&a.create_invite(HOUR, None).unwrap()).await.unwrap();
    let (pa, pb) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (ra, rb) = (pa.path(), pb.path());
    // Two full pieces and a short one.
    let content = data(2 * MIN_PIECE_LENGTH as usize + 1000);
    fs::write(ra.join("data.bin"), &content).unwrap();
    let id = a.add_folder("docs", ra).await.unwrap();
    eventually("a to index data.bin", || async {
        entry(&a, id, "data.bin").await.is_some()
    })
    .await;
    let meta = entry(&a, id, "data.bin").await.unwrap();
    let hash = meta.content.unwrap();
    let conn = b.endpoint().connect(a.id(), peer_wire::ALPN).await.unwrap();

    // b is a member, but doesn't keep the folder yet.
    let wanted = request(id, "data.bin", hash, 0);
    assert_eq!(ask(&conn, &wanted).await, Err(Refusal::NotKept));
    eventually("b to see the folder", || async {
        b.group_folders()
            .await
            .unwrap()
            .iter()
            .any(|(f, _)| *f == id)
    })
    .await;
    b.add_folder("docs", rb).await.unwrap();
    eventually("a to serve b", || async {
        ask(&conn, &wanted).await != Err(Refusal::NotKept)
    })
    .await;

    // Every piece, the short last one too, as the piece hashes say.
    let piece = MIN_PIECE_LENGTH as usize;
    for (i, expected) in content.chunks(piece).enumerate() {
        let got = ask(&conn, &request(id, "data.bin", hash, i as u32))
            .await
            .unwrap();
        assert_eq!(got, expected);
        assert_eq!(Hash::new(&got), meta.pieces[i]);
    }

    // Nothing the index doesn't describe.
    let refused = [
        request(id, "data.bin", hash, 3),
        request(id, "data.bin", Hash::new(b"another version"), 0),
        request(id, "nothing.bin", hash, 0),
        request(id, "../data.bin", hash, 0),
        request(id, "/etc/passwd", hash, 0),
    ];
    for wrong in &refused {
        assert_eq!(
            ask(&conn, wrong).await,
            Err(Refusal::NoSuchFile),
            "{wrong:?}"
        );
    }
    let unknown = NamespaceId::from(&[7u8; 32]);
    assert_eq!(
        ask(&conn, &request(unknown, "data.bin", hash, 0)).await,
        Err(Refusal::NotKept)
    );

    // Garbage on one stream doesn't harm the next.
    let (mut send, mut recv) = conn.open_bi().await.unwrap();
    send.write_all(&(1u32 << 20).to_le_bytes()).await.unwrap();
    send.write_all(b"not a request").await.unwrap();
    let _ = send.finish();
    assert!(recv.read_to_end(1 << 20).await.is_err());
    assert_eq!(ask(&conn, &wanted).await.unwrap(), content[..piece]);

    // Different bytes with the same size and modification time: the
    // watcher sees nothing to re-read, but the server notices, refuses, and
    // has the index catch up.
    let edited = data(content.len())
        .iter()
        .map(|b| b ^ 0xff)
        .collect::<Vec<_>>();
    let staged = pb.path().join("staged.bin");
    fs::write(&staged, &edited).unwrap();
    let mtime = fs::metadata(ra.join("data.bin"))
        .unwrap()
        .modified()
        .unwrap();
    fs::File::options()
        .write(true)
        .open(&staged)
        .unwrap()
        .set_modified(mtime)
        .unwrap();
    fs::rename(&staged, ra.join("data.bin")).unwrap();
    assert_eq!(ask(&conn, &wanted).await, Err(Refusal::Changed));
    eventually("a to re-index data.bin", || async {
        entry(&a, id, "data.bin").await.unwrap().content == Some(Hash::new(&edited))
    })
    .await;
    let new = request(id, "data.bin", Hash::new(&edited), 0);
    assert_eq!(ask(&conn, &new).await.unwrap(), edited[..piece]);

    // A file replaced by a symlink is no longer served, even with the same
    // entry.
    // Same size and time as the entry, so only the symlink gives it away.
    let outside = pb.path().join("outside.bin");
    fs::write(&outside, &edited).unwrap();
    let mtime = fs::metadata(ra.join("data.bin"))
        .unwrap()
        .modified()
        .unwrap();
    fs::File::options()
        .write(true)
        .open(&outside)
        .unwrap()
        .set_modified(mtime)
        .unwrap();
    fs::remove_file(ra.join("data.bin")).unwrap();
    std::os::unix::fs::symlink(&outside, ra.join("data.bin")).unwrap();
    assert!(ask(&conn, &new).await.is_err());

    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_non_member_cant_connect() {
    let book = MemoryLookup::new();
    let dirs: Vec<_> = (0..2).map(|_| tempfile::tempdir().unwrap()).collect();
    let (a, outsider) = (
        spawn(dirs[0].path(), &book).await,
        spawn(dirs[1].path(), &book).await,
    );
    a.create_group().await.unwrap();
    let connected = outsider.endpoint().connect(a.id(), peer_wire::ALPN).await;
    if let Ok(conn) = connected {
        // Accepted at first by QUIC, then closed by the gate.
        let request = request(NamespaceId::from(&[0u8; 32]), "x", Hash::new(b""), 0);
        assert!(request_piece(&conn, &request).await.is_err());
    }
    a.shutdown().await.unwrap();
    outsider.shutdown().await.unwrap();
}
