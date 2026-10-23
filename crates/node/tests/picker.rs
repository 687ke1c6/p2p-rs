//! The piece picker: one peer per piece, partial pieces first, rarest
//! first, the endgame, and peers that leave or send bad pieces.

use std::collections::HashSet;
use std::time::{Duration, Instant};

use sync_node::peer_wire::{bitfield_message, holds, Block, Message, BLOCK_LENGTH};
use sync_node::picker::{Holds, Picker};

const PIECE: u32 = 128 << 10;
/// Blocks per full piece.
const BLOCKS: u32 = PIECE / BLOCK_LENGTH;
const MS: Duration = Duration::from_millis(1);

fn bytes(block: Block) -> Vec<u8> {
    vec![block.piece as u8; block.length as usize]
}

/// Every block `p` is given at `now` until it has `limit` open or nothing
/// is left.
fn take(picker: &mut Picker, p: usize, limit: usize, now: Instant) -> Vec<Block> {
    let mut out = Vec::new();
    while picker.open(p) < limit {
        let Some(block) = picker.next(p, now) else {
            break;
        };
        out.push(block);
    }
    out
}

#[test]
fn one_seeder_gets_every_block_once() {
    // Two full pieces and a short one of 1000 bytes: one short block.
    let size = 2 * u64::from(PIECE) + 1000;
    let mut picker = Picker::new(PIECE, size, 0..3);
    let p = picker.add_peer(Holds::All);
    let t = Instant::now();
    let blocks = take(&mut picker, p, usize::MAX, t);
    assert_eq!(blocks.len(), 2 * BLOCKS as usize + 1);
    let unique: HashSet<Block> = blocks.iter().copied().collect();
    assert_eq!(unique.len(), blocks.len());
    // A piece is started only once the one before is fully asked for.
    let order: Vec<u32> = blocks.iter().map(|b| b.piece).collect();
    let mut runs = order.clone();
    runs.dedup();
    assert_eq!(runs.len(), 3, "{order:?}");
    let last = blocks.iter().find(|b| b.piece == 2).unwrap();
    assert_eq!((last.begin, last.length), (0, 1000));

    let mut done = Vec::new();
    for block in blocks {
        let got = picker.received(p, block, &bytes(block), t + MS);
        assert!(got.cancel.is_empty());
        if let Some(complete) = got.complete {
            assert_eq!(
                complete.data,
                vec![complete.piece as u8; complete.data.len()]
            );
            let sent: u32 = complete
                .senders
                .iter()
                .map(|(q, n)| {
                    assert_eq!(*q, p);
                    n
                })
                .sum();
            assert_eq!(sent as usize, complete.data.len());
            done.push(complete.piece);
            picker.verified(complete.piece);
        }
    }
    done.sort();
    assert_eq!(done, [0, 1, 2]);
    assert!(picker.is_complete());
    assert_eq!(picker.open(p), 0);
}

#[test]
fn pieces_present_are_not_asked_for() {
    let mut picker = Picker::new(PIECE, 4 * u64::from(PIECE), [1, 3]);
    assert_eq!(picker.have(), [true, false, true, false]);
    let p = picker.add_peer(Holds::All);
    let pieces: HashSet<u32> = take(&mut picker, p, usize::MAX, Instant::now())
        .iter()
        .map(|b| b.piece)
        .collect();
    assert_eq!(pieces, HashSet::from([1, 3]));
    // A peer with only what this node has offers nothing.
    assert!(!picker.wants(&Holds::These(vec![true, false, true, false])));
    assert!(picker.wants(&Holds::These(vec![false, false, false, true])));
}

#[test]
fn one_peer_per_piece() {
    let mut picker = Picker::new(PIECE, 4 * u64::from(PIECE), 0..4);
    let (a, b) = (picker.add_peer(Holds::All), picker.add_peer(Holds::All));
    let t = Instant::now();
    let from_a: HashSet<u32> = take(&mut picker, a, 3, t).iter().map(|b| b.piece).collect();
    let from_b: HashSet<u32> = take(&mut picker, b, 3, t).iter().map(|b| b.piece).collect();
    assert_eq!(from_a.len(), 1);
    assert_eq!(from_b.len(), 1);
    assert!(from_a.is_disjoint(&from_b));
}

#[test]
fn the_rarest_piece_comes_first() {
    let mut picker = Picker::new(PIECE, 3 * u64::from(PIECE), 0..3);
    // Piece 2 is held by both; pieces 0 and 1 by a only.
    let a = picker.add_peer(Holds::All);
    let b = picker.add_peer(Holds::These(vec![false, false, true]));
    let t = Instant::now();
    for _ in 0..20 {
        let mut fresh = Picker::new(PIECE, 3 * u64::from(PIECE), 0..3);
        let a = fresh.add_peer(Holds::All);
        fresh.add_peer(Holds::These(vec![false, false, true]));
        assert_ne!(fresh.next(a, t).unwrap().piece, 2);
    }
    assert_eq!(picker.next(b, t).unwrap().piece, 2);
    // Ties at random: over many tries, a starts either rare piece.
    let firsts: HashSet<u32> = (0..64)
        .map(|_| {
            let mut fresh = Picker::new(PIECE, 3 * u64::from(PIECE), 0..3);
            fresh.add_peer(Holds::All);
            fresh.next(a, t).unwrap().piece
        })
        .collect();
    assert_eq!(firsts, HashSet::from([0, 1, 2]));
    // A peer that has none of what is missing is asked for nothing.
    let c = picker.add_peer(Holds::These(vec![false, false, false]));
    assert_eq!(picker.next(c, t), None);
    // Until it has a piece.
    picker.has(c, 1);
    assert!(picker.next(c, t).is_some());
}

#[test]
fn a_dropped_peer_s_partial_piece_is_finished_first() {
    let mut picker = Picker::new(PIECE, 3 * u64::from(PIECE), 0..3);
    let a = picker.add_peer(Holds::All);
    let t = Instant::now();
    let blocks = take(&mut picker, a, 3, t);
    let piece = blocks[0].piece;
    // Two blocks arrive; then a leaves with the third open.
    for block in &blocks[..2] {
        assert!(picker
            .received(a, *block, &bytes(*block), t + MS)
            .complete
            .is_none());
    }
    picker.drop_peer(a);
    assert_eq!(picker.next(a, t), None);
    assert_eq!(picker.open(a), 0);

    // b finishes that piece before starting another, and isn't asked for
    // the blocks that arrived.
    let b = picker.add_peer(Holds::All);
    let rest = take(&mut picker, b, usize::MAX, t);
    let of_piece: Vec<&Block> = rest.iter().filter(|x| x.piece == piece).collect();
    assert_eq!(of_piece.len(), BLOCKS as usize - 2);
    assert!(rest[..of_piece.len()].iter().all(|x| x.piece == piece));
    assert_eq!(of_piece[0].begin, 2 * BLOCK_LENGTH);
    let mut complete = None;
    for block in of_piece {
        complete = picker.received(b, *block, &bytes(*block), t + MS).complete;
    }
    let complete = complete.expect("the piece is complete");
    let mut by: Vec<(usize, u32)> = complete.senders.clone();
    by.dedup_by_key(|(q, _)| *q);
    assert_eq!(by.iter().map(|(q, _)| *q).collect::<Vec<_>>(), [a, b]);
}

#[test]
fn a_rejected_block_is_asked_for_again() {
    let mut picker = Picker::new(PIECE, u64::from(PIECE), [0]);
    let a = picker.add_peer(Holds::All);
    let t = Instant::now();
    let first = picker.next(a, t).unwrap();
    picker.rejected(a, first);
    assert_eq!(picker.open(a), 0);
    assert_eq!(picker.next(a, t), Some(first));
    // Rejecting a block that isn't asked for changes nothing.
    let other = Block {
        begin: BLOCK_LENGTH,
        ..first
    };
    picker.rejected(a, other);
    assert_eq!(picker.open(a), 1);
}

#[test]
fn a_bad_piece_is_fetched_again_from_scratch() {
    let mut picker = Picker::new(PIECE, u64::from(PIECE), [0]);
    let a = picker.add_peer(Holds::All);
    let t = Instant::now();
    let mut complete = None;
    for block in take(&mut picker, a, usize::MAX, t) {
        complete = picker.received(a, block, &bytes(block), t + MS).complete;
    }
    let complete = complete.unwrap();
    assert!(complete.senders.iter().all(|(q, _)| *q == a));
    // While it is checked, nothing more is asked.
    assert_eq!(picker.next(a, t), None);
    picker.failed(complete.piece);
    picker.drop_peer(a);
    let b = picker.add_peer(Holds::All);
    assert_eq!(take(&mut picker, b, usize::MAX, t).len(), BLOCKS as usize);
    assert!(!picker.is_complete());
}

#[test]
fn the_endgame_asks_again_only_for_overdue_blocks() {
    let mut picker = Picker::new(PIECE, u64::from(PIECE), [0]);
    let (a, b) = (picker.add_peer(Holds::All), picker.add_peer(Holds::All));
    let t = Instant::now();
    let blocks = take(&mut picker, a, usize::MAX, t);
    // Nothing is overdue before any block has arrived.
    assert_eq!(picker.next(b, t + Duration::from_secs(60)), None);
    assert_eq!(picker.next_overdue(t), None);
    // Blocks typically take 10 ms; the last stays open.
    for block in &blocks[..blocks.len() - 1] {
        picker.received(a, *block, &bytes(*block), t + 10 * MS);
    }
    let last = *blocks.last().unwrap();
    assert_eq!(picker.next(b, t + 15 * MS), None);
    let due = picker.next_overdue(t + 15 * MS).unwrap();
    assert!(due <= t + 21 * MS, "{:?}", due - t);
    // Overdue: b is asked too, and nobody a third time.
    assert_eq!(picker.next(b, t + 30 * MS), Some(last));
    assert_eq!(picker.next(b, t + 30 * MS), None);
    let c = picker.add_peer(Holds::All);
    assert_eq!(picker.next(c, t + 60 * MS), None);
    // b answers first and wins: a's request can be cancelled.
    let got = picker.received(b, last, &bytes(last), t + 31 * MS);
    assert_eq!(got.cancel, [a]);
    assert!(got.complete.is_some());
    assert_eq!((picker.open(a), picker.open(b)), (0, 0));
    // a's late answer is ignored.
    let late = picker.received(a, last, &bytes(last), t + 40 * MS);
    assert!(late.complete.is_none() && late.cancel.is_empty());
}

#[test]
fn bitfields_round_trip() {
    let have = vec![true, false, true, true, false, false, false, false, true];
    let msg = bitfield_message(&have);
    assert_eq!(msg, Message::Bitfield(vec![0b1011_0000, 0b1000_0000]));
    assert_eq!(holds(&msg, 9), Some(Holds::These(have)));
    assert_eq!(bitfield_message(&[true; 3]), Message::HaveAll);
    assert_eq!(bitfield_message(&[false; 3]), Message::HaveNone);
    assert_eq!(holds(&Message::HaveAll, 3), Some(Holds::All));
    assert_eq!(
        holds(&Message::HaveNone, 3),
        Some(Holds::These(vec![false; 3]))
    );
    // Wrong length, spare bits set, or not a bitfield at all.
    assert_eq!(holds(&Message::Bitfield(vec![0xff]), 9), None);
    assert_eq!(holds(&Message::Bitfield(vec![0, 0b0100_0000]), 9), None);
    assert_eq!(holds(&Message::Interested, 9), None);
}

#[test]
fn a_piece_a_peer_lacks_or_this_node_lost_is_asked_for_elsewhere() {
    let mut picker = Picker::new(PIECE, 2 * u64::from(PIECE), [0]);
    let a = picker.add_peer(Holds::All);
    let t = Instant::now();
    let first = picker.next(a, t).unwrap();
    // a rejects with `NotHave`: not asked for piece 0 again, so nothing.
    picker.rejected(a, first);
    picker.lacks(a, 0);
    assert_eq!(picker.next(a, t), None);
    assert!(!picker.wants(&Holds::These(vec![false, true])));
    // This node's piece 1 no longer matches on disk: fetched again.
    picker.lost(1);
    assert_eq!(picker.have(), [false, false]);
    assert_eq!(picker.next(a, t).unwrap().piece, 1);
}

#[test]
fn progress_counts() {
    let mut picker = Picker::new(PIECE, 4 * u64::from(PIECE), [1, 2, 3]);
    let seeder = picker.add_peer(Holds::All);
    let partial = picker.add_peer(Holds::These(vec![true, false, true, false]));
    assert_eq!((picker.held(seeder), picker.held(partial)), (4, 2));
    assert_eq!(picker.have_count(), 1);
    picker.has(partial, 3);
    assert_eq!(picker.held(partial), 3);
    assert_eq!(picker.active(), 0);
    let t = Instant::now();
    // Two pieces started; the first in full, so it waits for its check.
    let blocks = take(&mut picker, seeder, BLOCKS as usize + 1, t);
    assert_eq!(picker.active(), 2);
    for block in blocks.iter().filter(|b| b.piece == blocks[0].piece) {
        picker.received(seeder, *block, &bytes(*block), t + MS);
    }
    assert_eq!(picker.active(), 2);
    picker.verified(blocks[0].piece);
    assert_eq!(picker.active(), 1);
    assert_eq!(picker.have_count(), 2);
}
