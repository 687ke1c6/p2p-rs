//! The peer wire, seeder side: a node serves exactly the blocks of the pieces
//! its index describes, to members that keep the folder, and rejects
//! everything else.

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
use sync_node::peer_wire::{
    self, holds, Block, Handshake, Message, Refusal, SwarmPeerState, Why, Wire, BLOCK_LENGTH,
    MAX_BLOCK_LENGTH,
};
use sync_node::picker::Holds;
use sync_node::pieces::{staging_path, MIN_PIECE_LENGTH};
use sync_node::{Config, Network, Node};

const INTERVAL: Duration = Duration::from_millis(200);
const TIMEOUT: Duration = Duration::from_secs(20);
const HOUR: Duration = Duration::from_secs(3600);

async fn spawn(dir: &Path, book: &MemoryLookup) -> Node {
    let config = Config {
        network: Network::Local,
        presence_interval: INTERVAL,
        debounce: Duration::from_millis(100),
        address_book: Some(book.clone()),
        direct_only: false,
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

fn handshake(id: NamespaceId, path: &str, content: Hash) -> Handshake {
    Handshake {
        folder: id,
        path: path.to_string(),
        content,
    }
}

/// Joins the swarm `handshake` names: the other end's answer.
async fn answer(conn: &Connection, handshake: &Handshake) -> Message {
    Wire::open(conn, handshake).await.unwrap().1
}

/// Joins the swarm as a leecher with nothing, and is unchoked.
async fn unchoked(conn: &Connection, handshake: &Handshake) -> Wire {
    let (mut wire, answer) = Wire::open(conn, handshake).await.unwrap();
    assert_eq!(answer, Message::HaveAll);
    wire.send(&Message::HaveNone).await.unwrap();
    wire.send(&Message::Interested).await.unwrap();
    assert_eq!(next(&mut wire).await.0, Message::Unchoke);
    wire
}

/// Joins the swarm as a leecher with nothing, whatever the other end has,
/// and is unchoked.
async fn unchoked_by(conn: &Connection, handshake: &Handshake) -> Wire {
    let (mut wire, answer) = Wire::open(conn, handshake).await.unwrap();
    assert!(!matches!(answer, Message::Refused(_)), "{answer:?}");
    wire.send(&Message::HaveNone).await.unwrap();
    wire.send(&Message::Interested).await.unwrap();
    loop {
        match next(&mut wire).await.0 {
            Message::Unchoke => return wire,
            Message::Have(_) => continue,
            other => panic!("not unchoked: {other:?}"),
        }
    }
}

async fn next(wire: &mut Wire) -> (Message, Vec<u8>) {
    tokio::time::timeout(TIMEOUT, wire.recv())
        .await
        .expect("no answer")
        .unwrap()
        .expect("stream finished")
}

/// Requests `block`: its bytes, or why not. A leecher's `Have`s meanwhile
/// are skipped.
async fn ask(wire: &mut Wire, block: Block) -> Result<Vec<u8>, Why> {
    wire.send(&Message::Request(block)).await.unwrap();
    let mut answer = next(wire).await;
    while let (Message::Have(_), _) = answer {
        answer = next(wire).await;
    }
    match answer {
        (Message::Piece(got), data) => {
            assert_eq!(got, block);
            Ok(data)
        }
        (Message::Reject(got, why), _) => {
            assert_eq!(got, block);
            Err(why)
        }
        other => panic!("not an answer to {block:?}: {other:?}"),
    }
}

/// Piece `piece` of a file whose piece length and size are `meta`'s, asked
/// for in blocks of `BLOCK_LENGTH`.
async fn ask_piece(wire: &mut Wire, meta: &FileMeta, piece: u32) -> Vec<u8> {
    let start = u64::from(piece) * u64::from(meta.piece_length);
    let len = u64::from(meta.piece_length).min(meta.size - start) as u32;
    let mut out = Vec::new();
    for begin in (0..len).step_by(BLOCK_LENGTH as usize) {
        let length = BLOCK_LENGTH.min(len - begin);
        let block = Block {
            piece,
            begin,
            length,
        };
        out.extend(ask(wire, block).await.unwrap());
    }
    out
}

/// Gives `path` under `root` the contents `bytes`, keeping its size and
/// modification time: the watcher sees nothing to re-read.
fn swap_keeping_time(root: &Path, path: &str, bytes: &[u8], scratch: &Path) {
    let staged = scratch.join("staged.bin");
    fs::write(&staged, bytes).unwrap();
    let mtime = fs::metadata(root.join(path)).unwrap().modified().unwrap();
    fs::File::options()
        .write(true)
        .open(&staged)
        .unwrap()
        .set_modified(mtime)
        .unwrap();
    fs::rename(&staged, root.join(path)).unwrap();
}

#[tokio::test]
async fn a_seeder_serves_what_its_index_describes() {
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
    // Two full pieces and a short one, which ends in a short block.
    let content = data(2 * MIN_PIECE_LENGTH as usize + 1000);
    fs::write(ra.join("data.bin"), &content).unwrap();
    let id = a.add_folder("docs", ra).await.unwrap();
    eventually("a to index data.bin", || async {
        entry(&a, id, "data.bin").await.is_some()
    })
    .await;
    let meta = entry(&a, id, "data.bin").await.unwrap();
    let hash = meta.content.unwrap();
    let swarm = handshake(id, "data.bin", hash);
    let conn = b.endpoint().connect(a.id(), peer_wire::ALPN).await.unwrap();

    // b is a member, but doesn't keep the folder yet.
    assert_eq!(
        answer(&conn, &swarm).await,
        Message::Refused(Refusal::NotKept)
    );
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
        answer(&conn, &swarm).await == Message::HaveAll
    })
    .await;

    // Requests before `Interested` are choked; then every block of every
    // piece, the short last ones too, makes up pieces matching their hashes.
    let (mut wire, _) = Wire::open(&conn, &swarm).await.unwrap();
    wire.send(&Message::HaveNone).await.unwrap();
    let first = Block {
        piece: 0,
        begin: 0,
        length: BLOCK_LENGTH,
    };
    assert_eq!(ask(&mut wire, first).await, Err(Why::Choked));
    wire.send(&Message::Interested).await.unwrap();
    assert_eq!(next(&mut wire).await.0, Message::Unchoke);
    let piece = MIN_PIECE_LENGTH as usize;
    for (i, expected) in content.chunks(piece).enumerate() {
        let got = ask_piece(&mut wire, &meta, i as u32).await;
        assert_eq!(got, expected);
        assert_eq!(Hash::new(&got), meta.pieces[i]);
    }
    // Any block within a piece, up to the longest served.
    let long = Block {
        piece: 1,
        begin: 1000,
        length: MAX_BLOCK_LENGTH - 1000,
    };
    assert_eq!(
        ask(&mut wire, long).await.unwrap(),
        content[piece + 1000..2 * piece]
    );

    // Not blocks of this file: rejected, and the stream goes on.
    let last = (content.len() - 2 * piece) as u32;
    let invalid = [
        Block {
            piece: 3,
            begin: 0,
            length: 1,
        },
        Block {
            piece: 2,
            begin: last - 10,
            length: 11,
        },
        Block {
            piece: 0,
            begin: MIN_PIECE_LENGTH,
            length: 1,
        },
        Block {
            piece: 0,
            begin: 0,
            length: 0,
        },
        Block {
            piece: 0,
            begin: 0,
            length: MAX_BLOCK_LENGTH + 1,
        },
    ];
    for block in invalid {
        assert_eq!(ask(&mut wire, block).await, Err(Why::Invalid), "{block:?}");
    }
    assert_eq!(
        ask(&mut wire, first).await.unwrap(),
        content[..BLOCK_LENGTH as usize]
    );

    // Swarms the index doesn't describe.
    let refused = [
        handshake(id, "data.bin", Hash::new(b"another version")),
        handshake(id, "nothing.bin", hash),
        handshake(id, "../data.bin", hash),
        handshake(id, "/etc/passwd", hash),
    ];
    for wrong in &refused {
        assert_eq!(
            answer(&conn, wrong).await,
            Message::Refused(Refusal::NoSuchFile),
            "{wrong:?}"
        );
    }
    let unknown = NamespaceId::from(&[7u8; 32]);
    assert_eq!(
        answer(&conn, &handshake(unknown, "data.bin", hash)).await,
        Message::Refused(Refusal::NotKept)
    );

    // Different bytes with the same size and modification time: the
    // watcher sees nothing to re-read, but the server notices, rejects, and
    // has the index catch up. A piece already verified on the stream is
    // served from memory, so ask a fresh stream.
    let edited = data(content.len())
        .iter()
        .map(|b| b ^ 0xff)
        .collect::<Vec<_>>();
    swap_keeping_time(ra, "data.bin", &edited, rb);
    let mut fresh = unchoked(&conn, &swarm).await;
    assert_eq!(ask(&mut fresh, first).await, Err(Why::Changed));
    eventually("a to re-index data.bin", || async {
        entry(&a, id, "data.bin").await.unwrap().content == Some(Hash::new(&edited))
    })
    .await;
    // The old swarm's requests now fail at once: a no longer has its
    // version (neither in its index nor downloading it).
    assert_eq!(ask(&mut wire, first).await, Err(Why::NotHave));
    let new = handshake(id, "data.bin", Hash::new(&edited));
    let mut wire = unchoked(&conn, &new).await;
    assert_eq!(
        ask(&mut wire, first).await.unwrap(),
        edited[..BLOCK_LENGTH as usize]
    );

    // A file replaced by a symlink is no longer served, even with the same
    // entry: same size and time, so only the symlink gives it away.
    let outside = rb.join("outside.bin");
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
    let second = Block { piece: 1, ..first };
    assert_eq!(ask(&mut wire, second).await, Err(Why::Changed));

    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}

#[tokio::test]
async fn garbage_closes_the_stream_not_the_connection() {
    let book = MemoryLookup::new();
    let dirs: Vec<_> = (0..2).map(|_| tempfile::tempdir().unwrap()).collect();
    let (a, b) = (
        spawn(dirs[0].path(), &book).await,
        spawn(dirs[1].path(), &book).await,
    );
    a.create_group().await.unwrap();
    b.join(&a.create_invite(HOUR, None).unwrap()).await.unwrap();
    let (pa, pb) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    fs::write(pa.path().join("data.bin"), data(1000)).unwrap();
    let id = a.add_folder("docs", pa.path()).await.unwrap();
    eventually("b to see the folder", || async {
        b.group_folders()
            .await
            .unwrap()
            .iter()
            .any(|(f, _)| *f == id)
    })
    .await;
    b.add_folder("docs", pb.path()).await.unwrap();
    eventually("a to index data.bin", || async {
        entry(&a, id, "data.bin").await.is_some()
    })
    .await;
    let swarm = handshake(id, "data.bin", Hash::new(data(1000)));
    let conn = b.endpoint().connect(a.id(), peer_wire::ALPN).await.unwrap();
    eventually("a to serve b", || async {
        answer(&conn, &swarm).await == Message::HaveAll
    })
    .await;

    // An oversized handshake, then bytes that aren't one.
    for garbage in [
        [&(1u32 << 20).to_le_bytes()[..], b"not a handshake"].concat(),
        [&5u32.to_le_bytes()[..], b"\xff\xff\xff\xff\xff"].concat(),
    ] {
        let (mut send, mut recv) = conn.open_bi().await.unwrap();
        send.write_all(&garbage).await.unwrap();
        let _ = send.finish();
        assert!(recv.read_to_end(1 << 20).await.is_err());
    }
    // A message that isn't one, after a good handshake.
    let (mut send, mut recv) = conn.open_bi().await.unwrap();
    let hello = postcard::to_stdvec(&swarm).unwrap();
    send.write_all(&(hello.len() as u32).to_le_bytes())
        .await
        .unwrap();
    send.write_all(&hello).await.unwrap();
    send.write_all(&3u32.to_le_bytes()).await.unwrap();
    send.write_all(b"\xff\xff\xff").await.unwrap();
    assert!(recv.read_to_end(1 << 20).await.is_err());

    // The connection still serves.
    let mut wire = unchoked(&conn, &swarm).await;
    let block = Block {
        piece: 0,
        begin: 0,
        length: 1000,
    };
    assert_eq!(ask(&mut wire, block).await.unwrap(), data(1000));
    assert!(conn.close_reason().is_none());

    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}

/// A leecher serves the pieces its download has verified, from its staging
/// file; one that no longer matches there is rejected, and no longer in its
/// bitfield.
#[tokio::test]
async fn a_leecher_serves_from_its_staging_file() {
    let book = MemoryLookup::new();
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let (a, b, c) = (
        spawn(dirs[0].path(), &book).await,
        spawn(dirs[1].path(), &book).await,
        spawn(dirs[2].path(), &book).await,
    );
    a.create_group().await.unwrap();
    for node in [&b, &c] {
        node.join(&a.create_invite(HOUR, None).unwrap())
            .await
            .unwrap();
    }
    let roots: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let rb = roots[1].path().canonicalize().unwrap();
    const SIZE: usize = 64 << 20;
    let content = data(SIZE);
    fs::write(roots[0].path().join("big.bin"), &content).unwrap();
    let id = a.add_folder("docs", roots[0].path()).await.unwrap();
    eventually("a to index big.bin", || async {
        entry(&a, id, "big.bin").await.is_some()
    })
    .await;
    for (node, root) in [(&b, rb.as_path()), (&c, roots[2].path())] {
        eventually("the folder to be listed", || async {
            node.group_folders()
                .await
                .unwrap()
                .iter()
                .any(|(f, _)| *f == id)
        })
        .await;
        node.add_folder("docs", root).await.unwrap();
    }
    let meta = entry(&a, id, "big.bin").await.unwrap();
    let hash = meta.content.unwrap();
    // b has part of the file when its only seeder goes.
    eventually("b to be downloading", || async {
        b.fetched().get(&a.id()).copied().unwrap_or(0) > 2 << 20
    })
    .await;
    // The seeder's choker uploads to b, as its next published rows show.
    eventually("a's rows to show b unchoked", || async {
        a.transfers()
            .uploads
            .iter()
            .any(|u| u.id == b.id() && u.unchoked && u.interested)
    })
    .await;
    let a_id = a.id();
    a.shutdown().await.unwrap();

    let swarm = handshake(id, "big.bin", hash);
    let conn = c.endpoint().connect(b.id(), peer_wire::ALPN).await.unwrap();
    // b answers as a leecher once it lists c as keeping the folder.
    let start = Instant::now();
    let answer_ = loop {
        let got = answer(&conn, &swarm).await;
        if matches!(got, Message::Bitfield(_)) {
            break got;
        }
        assert!(
            start.elapsed() < TIMEOUT,
            "b never answered as a leecher: {got:?}"
        );
        tokio::time::sleep(INTERVAL / 4).await;
    };
    let count = meta.pieces.len() as u32;
    let Some(Holds::These(have)) = holds(&answer_, count) else {
        panic!("not a bitfield: {answer_:?}");
    };
    let held: Vec<u32> = (0..count).filter(|i| have[*i as usize]).collect();
    assert!(!held.is_empty() && held.len() < count as usize);
    let (good, bad) = (held[0], held[held.len() - 1]);
    assert_ne!(good, bad);

    // A verified piece is served, and matches.
    let mut wire = unchoked_by(&conn, &swarm).await;
    let got = ask_piece(&mut wire, &meta, good).await;
    assert_eq!(Hash::new(&got), meta.pieces[good as usize]);

    // A piece that no longer matches in the staging file is rejected.
    let staging = staging_path(&rb, "big.bin", hash);
    let file = fs::File::options().write(true).open(&staging).unwrap();
    let at = u64::from(bad) * u64::from(meta.piece_length);
    std::os::unix::fs::FileExt::write_all_at(&file, b"not the piece", at).unwrap();
    let first = Block {
        piece: bad,
        begin: 0,
        length: BLOCK_LENGTH,
    };
    assert_eq!(ask(&mut wire, first).await, Err(Why::NotHave));
    let again = answer(&conn, &swarm).await;
    let Some(Holds::These(have)) = holds(&again, count) else {
        panic!("not a bitfield: {again:?}");
    };
    assert!(!have[bad as usize] && have[good as usize]);

    // b's download goes on without a, with the pieces it has.
    eventually("b's snapshot to show a out of the swarm", || async {
        b.transfers().swarms.iter().any(|s| {
            s.folder == id
                && s.path == "big.bin"
                && s.content == hash
                && s.pieces == meta.pieces.len() as u32
                && s.have > 0
                && s.have < s.pieces
                && s.peers
                    .iter()
                    .any(|p| p.id == a_id && !matches!(p.state, SwarmPeerState::In(_)))
        })
    })
    .await;

    b.shutdown().await.unwrap();
    c.shutdown().await.unwrap();
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
        let swarm = handshake(NamespaceId::from(&[0u8; 32]), "x", Hash::new(b""));
        assert!(Wire::open(&conn, &swarm).await.is_err());
    }
    a.shutdown().await.unwrap();
    outsider.shutdown().await.unwrap();
}
