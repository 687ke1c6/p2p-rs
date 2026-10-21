//! Nodes starved of CPU, as on a busy Pi: the runtime thread is blocked
//! most of the time, so downloads and requests time out. Whatever fails is
//! tried again later, instead of being left undone. Slow by design: run with
//! `just stress`.
//!
//! * Every membership record is read: one whose content didn't arrive with
//!   it is fetched again on a later presence interval.
//! * Every file is fetched.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use iroh::address_lookup::memory::MemoryLookup;
use iroh::SecretKey;
use iroh_docs::engine::LiveEvent;
use iroh_docs::NamespaceId;
use n0_future::StreamExt;
use sync_node::{Config, Network, Node};

const HOUR: Duration = Duration::from_secs(3600);
const GROUPS: usize = 8;
const TIMEOUT: Duration = Duration::from_secs(45);

/// How long the runtime thread is blocked at a time, with short runs in
/// between: content downloads time out, as on a busy Pi.
const STALL: Duration = Duration::from_millis(300);

async fn spawn(dir: &std::path::Path, book: &MemoryLookup) -> Node {
    let config = Config {
        network: Network::Local,
        presence_interval: Duration::from_millis(200),
        debounce: Duration::from_millis(100),
        address_book: Some(book.clone()),
    };
    Node::spawn_with(dir, SecretKey::generate(), config)
        .await
        .unwrap()
}

/// a founds a group, b joins, a adds a folder: whether b lists it in time.
async fn b_lists_the_folder() -> bool {
    let book = MemoryLookup::new();
    let dirs: Vec<_> = (0..2).map(|_| tempfile::tempdir().unwrap()).collect();
    let a = spawn(dirs[0].path(), &book).await;
    let b = spawn(dirs[1].path(), &book).await;
    a.create_group().await.unwrap();
    b.join(&a.create_invite(HOUR, None).unwrap()).await.unwrap();
    let root = tempfile::tempdir().unwrap();
    let id = a.add_folder("docs", root.path()).await.unwrap();
    let start = Instant::now();
    let mut listed = false;
    while !listed && start.elapsed() < TIMEOUT {
        tokio::time::sleep(Duration::from_millis(50)).await;
        listed = b
            .group_folders()
            .await
            .unwrap()
            .iter()
            .any(|(f, _)| *f == id);
    }
    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
    listed
}

// Slow by design (40–60 s): run with `just stress`.
#[tokio::test]
#[ignore = "load test: run with `just stress`"]
async fn a_starved_node_reads_every_record() {
    tokio::spawn(async {
        loop {
            std::thread::sleep(STALL);
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    });
    let mut groups = tokio::task::JoinSet::new();
    for _ in 0..GROUPS {
        groups.spawn(b_lists_the_folder());
    }
    let missed = groups.join_all().await.iter().filter(|l| !**l).count();
    assert_eq!(
        missed, 0,
        "{missed} of {GROUPS} nodes never listed the folder"
    );
}

/// How far b got with fetching a's file, when it didn't finish.
#[derive(Debug)]
#[allow(dead_code, reason = "read through Debug, in the assertion message")]
struct Stuck {
    listed: bool,
    /// b's merged view has the path: a's entry and its metadata arrived.
    in_view: bool,
    staged: bool,
    fetched: u64,
    /// When b's doc got a's entry for the file, after b kept the folder.
    entry_after: Option<Duration>,
    /// When the file was first in b's merged view, after b kept the folder.
    view_after: Option<Duration>,
    /// Both docs' sync events, in order (see [`watch_doc`]).
    timeline: Vec<String>,
}

/// What happened to the file's entry and the doc syncs, with times from
/// `start`.
#[derive(Clone, Default)]
struct Probe(Arc<Mutex<(Option<Duration>, Vec<String>)>>);

/// Records in `probe`, once, when `node` (named `who`) first syncs doc `id`
/// with another node: `Folders::sync_peers` called `start_sync`.
fn note_sync_start(
    who: &str,
    node: &Node,
    id: NamespaceId,
    start: Instant,
    probe: &Probe,
    noted: &mut bool,
) {
    let peers = node
        .folders()
        .into_iter()
        .find(|f| f.config.id == id)
        .map(|f| f.peers);
    if !*noted && peers.is_some_and(|n| n > 0) {
        *noted = true;
        let at = start.elapsed().as_millis();
        let line = format!("{at:>6} ms {who}: started doc sync with the other node");
        probe.0.lock().unwrap().1.push(line);
    }
}

/// Records `node`'s sync events for doc `id` in `probe`, named `who`, until
/// the doc closes: every sync finished (with whom, why, and its result),
/// gossip neighbours, and the file's entry arriving.
async fn watch_doc(who: &'static str, node: &Node, id: NamespaceId, start: Instant, probe: Probe) {
    let Ok(Some(doc)) = node.docs().open(id).await else {
        return;
    };
    let Ok(mut events) = doc.subscribe().await else {
        return;
    };
    tokio::spawn(async move {
        // The content hash of the file's entry (its metadata blob), once seen.
        let mut meta = None;
        while let Some(Ok(event)) = events.next().await {
            let at = start.elapsed();
            let line = match event {
                LiveEvent::SyncFinished(sync) => format!(
                    "sync with {} ({:?}): {:?}",
                    sync.peer.fmt_short(),
                    sync.origin,
                    sync.result
                ),
                LiveEvent::NeighborUp(peer) => format!("neighbour up {}", peer.fmt_short()),
                LiveEvent::NeighborDown(peer) => format!("neighbour down {}", peer.fmt_short()),
                LiveEvent::InsertRemote {
                    entry,
                    content_status,
                    ..
                } if entry.key() == b"big.bin" => {
                    probe.0.lock().unwrap().0.get_or_insert(at);
                    meta = Some(entry.content_hash());
                    format!("entry arrived, content {content_status:?}")
                }
                LiveEvent::ContentReady { hash } if Some(hash) == meta => {
                    "entry's metadata arrived".to_string()
                }
                _ => continue,
            };
            let line = format!("{:>6} ms {who}: {line}", at.as_millis());
            probe.0.lock().unwrap().1.push(line);
        }
    });
}

/// a founds a group with a 4 MiB file in a folder, b joins and keeps the
/// folder: `None` if b has the file in time, else how far it got.
async fn b_fetches_the_file(seed: usize) -> Option<Stuck> {
    let book = MemoryLookup::new();
    let dirs: Vec<_> = (0..2).map(|_| tempfile::tempdir().unwrap()).collect();
    let a = spawn(dirs[0].path(), &book).await;
    let b = spawn(dirs[1].path(), &book).await;
    a.create_group().await.unwrap();
    b.join(&a.create_invite(HOUR, None).unwrap()).await.unwrap();
    let (ra, rb) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let data: Vec<u8> = (0..4usize << 20)
        .map(|i| (i * 7 + i / 4093 + seed) as u8)
        .collect();
    std::fs::write(ra.path().join("big.bin"), &data).unwrap();
    let id = a.add_folder("docs", ra.path()).await.unwrap();
    let start = Instant::now();
    let mut listed = false;
    while !listed && start.elapsed() < FETCH_TIMEOUT {
        tokio::time::sleep(Duration::from_millis(50)).await;
        listed = b
            .group_folders()
            .await
            .unwrap()
            .iter()
            .any(|(f, _)| *f == id);
    }
    let mut done = false;
    let probe = Probe::default();
    let mut kept_at = start;
    let mut noted = (false, false);
    let mut view_after = None;
    if listed {
        watch_doc("a", &a, id, start, probe.clone()).await;
        let at = start.elapsed().as_millis();
        probe
            .0
            .lock()
            .unwrap()
            .1
            .push(format!("{at:>6} ms b: keeps the folder"));
        b.add_folder("docs", rb.path()).await.unwrap();
        kept_at = Instant::now();
        watch_doc("b", &b, id, start, probe.clone()).await;
        while !done && start.elapsed() < FETCH_TIMEOUT {
            tokio::time::sleep(Duration::from_millis(100)).await;
            note_sync_start("a", &a, id, start, &probe, &mut noted.0);
            note_sync_start("b", &b, id, start, &probe, &mut noted.1);
            if view_after.is_none()
                && b.folder_view(id)
                    .await
                    .is_ok_and(|view| view.path("big.bin").is_some())
            {
                view_after = Some(kept_at.elapsed());
                let at = start.elapsed().as_millis();
                let line = format!("{at:>6} ms b: the file is in the view");
                probe.0.lock().unwrap().1.push(line);
            }
            done = std::fs::read(rb.path().join("big.bin")).is_ok_and(|got| got == data);
        }
    }
    let (arrived, timeline) = probe.0.lock().unwrap().clone();
    let entry_after = arrived.map(|at| (start + at).saturating_duration_since(kept_at));
    println!(
        "group {seed}: done {done}, entry after {entry_after:?}, in view after {view_after:?}"
    );
    if view_after.is_some_and(|t| t > Duration::from_secs(30)) {
        println!("group {seed} timeline: {timeline:#?}");
    }
    let mut stuck = None;
    if !done {
        let in_view = match b.folder_view(id).await {
            Ok(view) => view.path("big.bin").is_some(),
            Err(_) => false,
        };
        let staged = std::fs::read_dir(rb.path()).is_ok_and(|dir| {
            dir.flatten().any(|e| {
                e.file_name()
                    .to_string_lossy()
                    .starts_with(".sync-node-tmp-")
            })
        });
        stuck = Some(Stuck {
            listed,
            in_view,
            staged,
            fetched: b.fetched().values().sum(),
            entry_after,
            view_after,
            timeline,
        });
    }
    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
    stuck
}

const FETCH_TIMEOUT: Duration = Duration::from_secs(90);

#[tokio::test]
#[ignore = "load test: run with `just stress`"]
async fn a_starved_node_fetches_every_file() {
    tokio::spawn(async {
        loop {
            std::thread::sleep(STALL);
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    });
    let mut groups = tokio::task::JoinSet::new();
    for seed in 0..GROUPS {
        groups.spawn(b_fetches_the_file(seed));
    }
    let stuck: Vec<Stuck> = groups.join_all().await.into_iter().flatten().collect();
    assert!(
        stuck.is_empty(),
        "{} of {GROUPS} didn't get the file: {stuck:#?}",
        stuck.len()
    );
}
