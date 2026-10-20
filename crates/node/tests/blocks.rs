//! Block lists: the block size rule, hashing, and the index carrying them.

use std::fs;
use std::sync::Arc;
use std::time::{Duration, Instant};

use iroh::SecretKey;
use iroh_blobs::Hash;
use sync_node::blocks::{
    block_size, hash_reader, staging_path, Hasher, Schedule, Staged, MAX_BLOCK_SIZE,
    MIN_BLOCK_SIZE, OVERDUE, WINDOW,
};
use sync_node::index::FileMeta;
use sync_node::view::Version;
use sync_node::{Config, Network, Node};

const KIB: u64 = 1 << 10;
const MIB: u64 = 1 << 20;
const GIB: u64 = 1 << 30;

#[test]
fn block_size_grows_with_the_file() {
    assert_eq!(block_size(0), 0);
    assert_eq!(block_size(1), MIN_BLOCK_SIZE);
    assert_eq!(block_size(128 * KIB), MIN_BLOCK_SIZE);
    // 2,048 blocks of 128 KiB, then the next size up.
    assert_eq!(block_size(256 * MIB), MIN_BLOCK_SIZE);
    assert_eq!(block_size(256 * MIB + 1), 256 << 10);
    assert_eq!(block_size(GIB), 512 << 10);
    assert_eq!(block_size(32 * GIB), MAX_BLOCK_SIZE);
    // Past the largest size: longer lists rather than bigger blocks.
    assert_eq!(block_size(32 * GIB + 1), MAX_BLOCK_SIZE);
    assert_eq!(block_size(100 * GIB), MAX_BLOCK_SIZE);
}

/// Content that isn't a repeating pattern of any block size.
fn data(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i * 31 + i / 997) as u8).collect()
}

#[test]
fn hashing_matches_hashes_of_each_block() {
    // Two full blocks and a short one.
    let content = data(2 * 128 * KIB as usize + 1000);
    let (hashed, read) = hash_reader(content.len() as u64, &content[..]).unwrap();
    assert_eq!(read, content.len() as u64);
    assert_eq!(hashed.content, Hash::new(&content));
    assert_eq!(hashed.block_size, MIN_BLOCK_SIZE);
    let expected: Vec<Hash> = content
        .chunks(MIN_BLOCK_SIZE as usize)
        .map(Hash::new)
        .collect();
    assert_eq!(expected.len(), 3);
    assert_eq!(hashed.blocks, expected);

    // Fed in odd pieces, the same result.
    let mut hasher = Hasher::new(content.len() as u64);
    for piece in content.chunks(7919) {
        hasher.update(piece);
    }
    assert_eq!(hasher.finish(), hashed);
}

#[test]
fn an_empty_file_has_no_blocks() {
    let (hashed, read) = hash_reader(0, &[][..]).unwrap();
    assert_eq!(read, 0);
    assert_eq!(hashed.content, Hash::new(b""));
    assert_eq!(hashed.block_size, 0);
    assert!(hashed.blocks.is_empty());
}

#[test]
fn an_exact_multiple_has_no_short_block() {
    let content = data(2 * 128 * KIB as usize);
    let (hashed, _) = hash_reader(content.len() as u64, &content[..]).unwrap();
    assert_eq!(hashed.blocks.len(), 2);
}

#[tokio::test]
async fn the_index_carries_block_lists() {
    let config_dir = tempfile::tempdir().unwrap();
    let folder = tempfile::tempdir().unwrap();
    let root = folder.path();
    let big = data(300 * KIB as usize);
    let write = |rel: &str, content: &[u8]| {
        let path = root.join(rel);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, content).unwrap();
    };
    write("big.bin", &big);
    write("copy/big.bin", &big);
    write("empty.txt", b"");

    let config = Config {
        network: Network::Local,
        presence_interval: Duration::from_millis(200),
        debounce: Duration::from_millis(100),
        address_book: None,
    };
    let node = Node::spawn_with(config_dir.path(), SecretKey::generate(), config)
        .await
        .unwrap();
    node.create_group().await.unwrap();
    let id = node.add_folder("docs", root).await.unwrap();
    let start = Instant::now();
    let index = loop {
        let index = node.folder_index(id).await.unwrap();
        if index.len() == 3 {
            break index;
        }
        assert!(start.elapsed() < Duration::from_secs(20), "not indexed");
        tokio::time::sleep(Duration::from_millis(50)).await;
    };

    let expected: Vec<Hash> = big.chunks(MIN_BLOCK_SIZE as usize).map(Hash::new).collect();
    let entry = &index["big.bin"];
    assert_eq!(entry.content, Some(Hash::new(&big)));
    assert_eq!(entry.block_size, MIN_BLOCK_SIZE);
    assert_eq!(entry.blocks, expected);
    // The same content elsewhere: the same list.
    assert_eq!(index["copy/big.bin"].blocks, expected);
    let empty = &index["empty.txt"];
    assert_eq!((empty.block_size, empty.blocks.len()), (0, 0));

    // A tombstone has none either.
    fs::remove_file(root.join("big.bin")).unwrap();
    let start = Instant::now();
    loop {
        let index = node.folder_index(id).await.unwrap();
        if index["big.bin"].is_deleted() {
            assert!(index["big.bin"].blocks.is_empty());
            break;
        }
        assert!(start.elapsed() < Duration::from_secs(20), "not tombstoned");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    node.shutdown().await.unwrap();
}

/// An entry for `content`, as this node's index would write it.
fn entry(content: &[u8]) -> FileMeta {
    let (hashed, _) = hash_reader(content.len() as u64, content).unwrap();
    let node = SecretKey::generate().public();
    FileMeta::file(hashed, content.len() as u64, 0, node, Version::default())
}

#[test]
fn staging_copies_only_blocks_that_match() {
    let root = tempfile::tempdir().unwrap();
    let root = root.path();
    let block = MIN_BLOCK_SIZE as usize;
    // Four blocks; this node holds the first two in one file and the third
    // in another, whose bytes on disk no longer match its entry.
    let wanted = data(4 * block);
    let old = &wanted[..2 * block];
    fs::write(root.join("old.bin"), old).unwrap();
    let mut edited = wanted[..3 * block].to_vec();
    let edited_entry = entry(&edited);
    edited[2 * block] ^= 1;
    fs::write(root.join("edited.bin"), &edited).unwrap();
    let sources = [
        ("old.bin".to_string(), Arc::new(entry(old))),
        ("edited.bin".to_string(), Arc::new(edited_entry)),
    ];

    let meta = entry(&wanted);
    let temp = staging_path(root, "new.bin", meta.content.unwrap());
    let staged = Staged::open(&temp, &meta, root, &sources).unwrap();
    assert_eq!(staged.copied, 2 * block as u64);
    assert_eq!(staged.to_fetch, 2 * block as u64);
    let staged_bytes = fs::read(&temp).unwrap();
    assert_eq!(staged_bytes.len(), wanted.len());
    assert_eq!(&staged_bytes[..2 * block], old);
    assert!(staged_bytes[2 * block..].iter().all(|b| *b == 0));

    // Once the missing blocks are there, opening again finds them all.
    drop(staged);
    fs::write(&temp, &wanted).unwrap();
    let staged = Staged::open(&temp, &meta, root, &[]).unwrap();
    assert!(staged.is_complete());
    assert_eq!((staged.copied, staged.to_fetch), (0, 0));
}

/// Every block `p` is given at `now` until it has no room or nothing is
/// left.
fn take_all(schedule: &mut Schedule, p: usize, now: Instant) -> Vec<u32> {
    std::iter::from_fn(|| schedule.next(p, now)).collect()
}

const MS: Duration = Duration::from_millis(1);

#[test]
fn providers_draw_from_one_queue_up_to_the_window() {
    let t = Instant::now();
    let window = WINDOW as u32;
    let mut schedule = Schedule::new(0..2 * window + 3, 2);
    assert_eq!(
        take_all(&mut schedule, 0, t),
        (0..window).collect::<Vec<_>>()
    );
    let second = take_all(&mut schedule, 1, t);
    assert_eq!(second, (window..2 * window).collect::<Vec<_>>());
    // A provider that delivers gets the next block.
    assert!(schedule.delivered(0, 0, t + MS).is_empty());
    assert_eq!(schedule.next(0, t + MS), Some(2 * window));
}

#[test]
fn the_endgame_asks_again_only_for_overdue_blocks() {
    let t = Instant::now();
    let mut schedule = Schedule::new(0..4, 3);
    assert_eq!(take_all(&mut schedule, 0, t), [0, 1, 2, 3]);
    // Requests typically take 10 ms.
    assert!(schedule.delivered(0, 0, t + 10 * MS).is_empty());
    // Nothing left in the queue, and nothing overdue yet.
    assert_eq!(schedule.next(1, t + 15 * MS), None);
    let due = t + 10 * MS * OVERDUE;
    assert_eq!(schedule.next_overdue(t + 15 * MS), Some(due));
    // Overdue: provider 1 is asked too, and no block goes to a third.
    let late = due + MS;
    assert_eq!(take_all(&mut schedule, 1, late), [1, 2, 3]);
    assert_eq!(schedule.next(2, late), None);
    assert_eq!(schedule.next_overdue(late), None);
    // The first to send a block wins; the other's request can stop.
    assert_eq!(schedule.delivered(1, 1, late + MS), [0]);
    assert!(!schedule.is_asked(0, 1));
    assert_eq!(schedule.delivered(0, 2, late + MS), [1]);
    assert!(!schedule.is_complete());
    assert_eq!(schedule.delivered(1, 3, late + MS), [0]);
    assert!(schedule.is_complete());
}

#[test]
fn without_a_delivery_nothing_is_overdue() {
    let t = Instant::now();
    let mut schedule = Schedule::new(0..2, 2);
    take_all(&mut schedule, 0, t);
    let later = t + Duration::from_secs(3600);
    assert_eq!(schedule.next(1, later), None);
    assert_eq!(schedule.next_overdue(later), None);
}

#[test]
fn a_dropped_provider_s_blocks_go_to_the_others() {
    let t = Instant::now();
    let mut schedule = Schedule::new(0..4, 2);
    assert_eq!(take_all(&mut schedule, 0, t), [0, 1, 2, 3]);
    assert!(schedule.delivered(0, 0, t + MS).is_empty());
    assert_eq!(schedule.next(1, t + 10 * MS), Some(1));
    // Block 1 stays with 1; 2 and 3 go back to the queue, in order.
    assert_eq!(schedule.drop_provider(0), [1, 2, 3]);
    assert!(schedule.is_asked(1, 1));
    assert_eq!(take_all(&mut schedule, 1, t + 10 * MS), [2, 3]);
    for index in 1..4 {
        assert!(schedule.delivered(1, index, t + 20 * MS).is_empty());
    }
    assert!(schedule.is_complete());
}

#[test]
fn a_returned_block_is_asked_for_again_first() {
    let t = Instant::now();
    let mut schedule = Schedule::new(0..WINDOW as u32 + 1, 1);
    take_all(&mut schedule, 0, t);
    schedule.returned(0, 3);
    assert!(!schedule.is_asked(0, 3));
    assert_eq!(schedule.next(0, t), Some(3));
    // Giving back a block that isn't asked for changes nothing.
    schedule.returned(0, 3);
    schedule.returned(0, 3);
    assert_eq!(schedule.next(0, t), Some(3));
}

#[test]
fn a_provider_added_late_takes_what_is_left() {
    let t = Instant::now();
    let mut schedule = Schedule::new(0..WINDOW as u32 + 2, 0);
    assert_eq!(schedule.add_provider(), 0);
    take_all(&mut schedule, 0, t);
    assert_eq!(schedule.add_provider(), 1);
    assert_eq!(
        take_all(&mut schedule, 1, t),
        [WINDOW as u32, WINDOW as u32 + 1]
    );
}
