//! The choker, upload and download rates, and request queues.

use std::collections::HashSet;
use std::time::{Duration, Instant};

use sync_node::choker::{queue_length, Choker, Rate, MAX_QUEUE, MIN_QUEUE, RATE_WINDOW, SLOTS};
use sync_node::peer_wire::BLOCK_LENGTH;

/// Peers 0..n, the rate this node uploads to each its number + 1.
fn peers(n: u32) -> Vec<(u32, f64)> {
    (0..n).map(|k| (k, f64::from(k + 1) * 1000.0)).collect()
}

#[test]
fn the_fastest_interested_peers_get_the_slots() {
    let mut choker = Choker::default();
    let unchoked = choker.round(&peers(8), false);
    assert_eq!(unchoked.len(), SLOTS + 1);
    // The four this node uploads to fastest, and one optimistic unchoke.
    for fast in 4..8 {
        assert!(unchoked.contains(&fast), "{unchoked:?}");
    }
    assert_eq!(unchoked.iter().filter(|k| **k < 4).count(), 1);
}

#[test]
fn with_few_peers_every_interested_peer_is_unchoked() {
    for n in 1..=SLOTS as u32 + 1 {
        let mut choker = Choker::default();
        let unchoked = choker.round(&peers(n), false);
        assert_eq!(unchoked, (0..n).collect::<HashSet<_>>());
    }
    // A peer that isn't interested isn't unchoked.
    let mut choker = Choker::<u32>::default();
    assert!(choker.round(&[], false).is_empty());
}

#[test]
fn the_optimistic_unchoke_rotates_through_every_choked_peer() {
    let mut choker = Choker::default();
    let all = peers(8);
    let first = choker.round(&all, false);
    let optimistic = |set: &HashSet<u32>| *set.iter().find(|k| **k < 4).unwrap();
    let start = optimistic(&first);
    // Without a rotation it stays.
    assert_eq!(optimistic(&choker.round(&all, false)), start);
    // Each rotation moves it to the slow peer that had it least recently:
    // every one of them in turn.
    let mut seen = HashSet::from([start]);
    for _ in 0..3 {
        let next = optimistic(&choker.round(&all, true));
        assert!(seen.insert(next), "{next} came back before the others");
    }
    assert_eq!(seen, HashSet::from([0, 1, 2, 3]));
    // Then the first again.
    assert_eq!(optimistic(&choker.round(&all, true)), start);
}

#[test]
fn only_what_this_node_uploads_counts() {
    // The choker is told only what this node uploads to each peer, so a
    // peer that sends nothing back (it only downloads: a new laptop, a
    // backup) gets a slot if this node uploads to it fast.
    let mut choker = Choker::default();
    let mut interested = peers(8);
    interested.push((99, 1e9));
    let unchoked = choker.round(&interested, false);
    assert!(unchoked.contains(&99));
    // A peer that is no longer interested loses its slot.
    interested.pop();
    let unchoked = choker.round(&interested, false);
    assert!(!unchoked.contains(&99));
    assert_eq!(unchoked.len(), SLOTS + 1);
}

#[test]
fn a_rate_is_bytes_per_second_over_its_window() {
    let t = Instant::now();
    let mut rate = Rate::default();
    assert_eq!(rate.per_second(t), 0.0);
    // A first burst counts over at least a second.
    rate.add(1000, t);
    assert_eq!(rate.per_second(t), 1000.0);
    rate.add(3000, t + Duration::from_secs(1));
    assert_eq!(rate.per_second(t + Duration::from_secs(2)), 2000.0);
    // Samples older than the window drop out.
    let later = t + RATE_WINDOW + Duration::from_secs(2);
    assert_eq!(rate.per_second(later), 0.0);
}

#[test]
fn the_request_queue_follows_the_rate() {
    let block = f64::from(BLOCK_LENGTH);
    assert_eq!(queue_length(0.0, false), MIN_QUEUE);
    // Two seconds' worth of blocks.
    assert_eq!(queue_length(10.0 * block, false), 20);
    assert_eq!(queue_length(1e12, false), MAX_QUEUE);
    // A snubbed peer gets one request at a time, however fast it was.
    assert_eq!(queue_length(1e12, true), 1);
}
