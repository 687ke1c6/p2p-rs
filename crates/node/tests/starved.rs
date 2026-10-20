//! Nodes starved of CPU, as on a busy Pi: the runtime thread is blocked
//! most of the time, so downloads and requests time out. Whatever fails is
//! tried again later, instead of being left undone. Slow by design: run with
//! `just stress`.
//!
//! * Every membership record is read: one whose content didn't arrive with
//!   it is fetched again on a later presence interval.
//! * Every file is fetched.

use std::time::{Duration, Instant};

use iroh::address_lookup::memory::MemoryLookup;
use iroh::SecretKey;
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
    if listed {
        b.add_folder("docs", rb.path()).await.unwrap();
        while !done && start.elapsed() < FETCH_TIMEOUT {
            tokio::time::sleep(Duration::from_millis(100)).await;
            done = std::fs::read(rb.path().join("big.bin")).is_ok_and(|got| got == data);
        }
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
