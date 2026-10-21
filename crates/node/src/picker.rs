//! The piece picker (see `plans/bittorrent/plan.md`): which block to ask
//! which peer for during one swarm's download.
//!
//! * One peer per piece: a piece started with a peer is finished with it,
//!   so a piece that fails its hash has one sender, who is dropped. A
//!   dropped peer's pieces are left partial, for another peer to finish.
//! * Partial pieces first, then rarest first among the pieces the peer has,
//!   ties at random.
//! * The endgame: once every missing block is asked for, a block whose only
//!   request is overdue (open more than [`OVERDUE`] times as long as a block
//!   typically takes) is asked of another peer too. The first answer wins;
//!   the others are told to cancel.
//!
//! A piece's blocks are kept here until all have arrived, so nothing that
//! hasn't been checked against the piece hashes is written to disk.
//! Bookkeeping only: the download loop makes the requests and passes the
//! time in. Peers are numbered from 0, in the order they are added.

use std::time::{Duration, Instant};

use rand::RngExt;

use crate::peer_wire::{Block, BLOCK_LENGTH};

/// How many times the typical block time a request must be open before its
/// block is asked of a second peer.
pub const OVERDUE: u32 = 2;

/// Which pieces a peer has.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Holds {
    /// A seeder.
    All,
    /// One flag per piece.
    These(Vec<bool>),
}

/// A piece that is complete in memory, for the caller to check against its
/// hash and then [`verified`](Picker::verified) or
/// [`failed`](Picker::failed).
#[derive(Debug)]
pub struct Complete {
    pub piece: u32,
    pub data: Vec<u8>,
    /// Who sent which bytes: (peer, bytes), one entry per block.
    pub senders: Vec<(usize, u32)>,
}

/// What a block's arrival changed.
#[derive(Debug, Default)]
pub struct Received {
    /// Other peers asked for the same block (in the endgame), whose requests
    /// can be cancelled.
    pub cancel: Vec<usize>,
    pub complete: Option<Complete>,
}

#[derive(Debug)]
enum Piece {
    Missing,
    Active(Active),
    /// Every block is in; waiting for the caller's hash check.
    Checking,
    Have,
}

/// A started piece.
#[derive(Debug)]
struct Active {
    /// The peer finishing it; `None` once that peer is dropped.
    owner: Option<usize>,
    slots: Vec<Slot>,
    data: Vec<u8>,
}

#[derive(Debug, Clone)]
enum Slot {
    Missing,
    /// The peers asked for it (one, or more in the endgame), and when.
    Asked(Vec<(usize, Instant)>),
    /// Arrived, from this peer.
    Got(usize),
}

#[derive(Debug)]
struct Peer {
    holds: Holds,
    open: usize,
    dropped: bool,
}

impl Peer {
    fn has(&self, piece: u32) -> bool {
        match &self.holds {
            Holds::All => true,
            Holds::These(have) => have.get(piece as usize).copied().unwrap_or(false),
        }
    }
}

#[derive(Debug)]
pub struct Picker {
    piece_length: u32,
    size: u64,
    pieces: Vec<Piece>,
    peers: Vec<Peer>,
    /// How long a delivered block typically took (a moving average), once
    /// one has been.
    typical: Option<Duration>,
}

impl Picker {
    /// A picker for a file of `size` bytes in pieces of `piece_length`, of
    /// which this node lacks `missing`.
    pub fn new(piece_length: u32, size: u64, missing: impl IntoIterator<Item = u32>) -> Self {
        let count = if piece_length == 0 {
            0
        } else {
            size.div_ceil(u64::from(piece_length)) as usize
        };
        let mut pieces: Vec<Piece> = (0..count).map(|_| Piece::Have).collect();
        for piece in missing {
            if let Some(slot) = pieces.get_mut(piece as usize) {
                *slot = Piece::Missing;
            }
        }
        Self {
            piece_length,
            size,
            pieces,
            peers: Vec::new(),
            typical: None,
        }
    }

    pub fn piece_count(&self) -> u32 {
        self.pieces.len() as u32
    }

    /// Which pieces this node has, verified.
    pub fn have(&self) -> Vec<bool> {
        self.pieces
            .iter()
            .map(|p| matches!(p, Piece::Have))
            .collect()
    }

    /// Whether `holds` has a piece this node lacks.
    pub fn wants(&self, holds: &Holds) -> bool {
        let peer = Peer {
            holds: holds.clone(),
            open: 0,
            dropped: false,
        };
        (0..self.piece_count())
            .any(|i| !matches!(self.pieces[i as usize], Piece::Have) && peer.has(i))
    }

    /// Adds a peer, numbered next.
    pub fn add_peer(&mut self, holds: Holds) -> usize {
        self.peers.push(Peer {
            holds,
            open: 0,
            dropped: false,
        });
        self.peers.len() - 1
    }

    /// Peer `p` has verified `piece`.
    pub fn has(&mut self, p: usize, piece: u32) {
        let count = self.pieces.len();
        if let Holds::These(have) = &mut self.peers[p].holds {
            if let Some(flag) = have.get_mut(piece as usize) {
                *flag = true;
            }
            if have.len() == count && have.iter().all(|h| *h) {
                self.peers[p].holds = Holds::All;
            }
        }
    }

    /// Peer `p` doesn't have `piece` (it rejected a request with
    /// `NotHave`): it isn't asked for it again.
    pub fn lacks(&mut self, p: usize, piece: u32) {
        let count = self.pieces.len();
        let peer = &mut self.peers[p];
        if peer.holds == Holds::All {
            peer.holds = Holds::These(vec![true; count]);
        }
        if let Holds::These(have) = &mut peer.holds {
            if let Some(flag) = have.get_mut(piece as usize) {
                *flag = false;
            }
        }
        // Another peer finishes it.
        if let Some(Piece::Active(a)) = self.pieces.get_mut(piece as usize) {
            if a.owner == Some(p) {
                a.owner = None;
            }
        }
    }

    /// A piece this node had no longer matches its hash on disk: it is
    /// fetched again.
    pub fn lost(&mut self, piece: u32) {
        if let Some(slot @ Piece::Have) = self.pieces.get_mut(piece as usize) {
            *slot = Piece::Missing;
        }
    }

    /// Requests open to peer `p`.
    pub fn open(&self, p: usize) -> usize {
        self.peers[p].open
    }

    pub fn is_complete(&self) -> bool {
        self.pieces.iter().all(|p| matches!(p, Piece::Have))
    }

    /// The length of `piece`: the last one may be short.
    fn piece_len(&self, piece: u32) -> u32 {
        let start = u64::from(piece) * u64::from(self.piece_length);
        u64::from(self.piece_length).min(self.size - start) as u32
    }

    fn block(&self, piece: u32, slot: usize) -> Block {
        let begin = slot as u32 * BLOCK_LENGTH;
        Block {
            piece,
            begin,
            length: BLOCK_LENGTH.min(self.piece_len(piece) - begin),
        }
    }

    /// The next block to ask peer `p` for at `now`, if there is one: a
    /// block of a piece it is finishing, else of a partial piece nobody is
    /// finishing, else of the rarest piece it has that nobody has started,
    /// else, in the endgame, an overdue block.
    pub fn next(&mut self, p: usize, now: Instant) -> Option<Block> {
        if self.peers[p].dropped {
            return None;
        }
        let piece = self
            .own_piece(p)
            .or_else(|| self.orphan(p))
            .or_else(|| self.start(p));
        if let Some(piece) = piece {
            let Piece::Active(active) = &mut self.pieces[piece as usize] else {
                unreachable!("picked pieces are active");
            };
            let slot = active
                .slots
                .iter()
                .position(|s| matches!(s, Slot::Missing))
                .expect("picked pieces have a missing block");
            active.slots[slot] = Slot::Asked(vec![(p, now)]);
            self.peers[p].open += 1;
            return Some(self.block(piece, slot));
        }
        self.endgame(p, now)
    }

    /// A piece `p` is finishing that has a block not asked for yet.
    fn own_piece(&self, p: usize) -> Option<u32> {
        self.pieces
            .iter()
            .enumerate()
            .find_map(|(i, piece)| match piece {
                Piece::Active(a)
                    if a.owner == Some(p) && a.slots.iter().any(|s| matches!(s, Slot::Missing)) =>
                {
                    Some(i as u32)
                }
                _ => None,
            })
    }

    /// A partial piece whose peer was dropped, which `p` has: `p` finishes
    /// it.
    fn orphan(&mut self, p: usize) -> Option<u32> {
        let found = self
            .pieces
            .iter()
            .enumerate()
            .find_map(|(i, piece)| match piece {
                Piece::Active(a)
                    if a.owner.is_none()
                        && self.peers[p].has(i as u32)
                        && a.slots.iter().any(|s| matches!(s, Slot::Missing)) =>
                {
                    Some(i as u32)
                }
                _ => None,
            })?;
        if let Piece::Active(a) = &mut self.pieces[found as usize] {
            a.owner = Some(p);
        }
        Some(found)
    }

    /// Starts the rarest missing piece `p` has, ties at random.
    fn start(&mut self, p: usize) -> Option<u32> {
        let mut rarest = Vec::new();
        let mut fewest = usize::MAX;
        for i in 0..self.piece_count() {
            if !matches!(self.pieces[i as usize], Piece::Missing) || !self.peers[p].has(i) {
                continue;
            }
            let holders = self.peers.iter().filter(|q| !q.dropped && q.has(i)).count();
            if holders < fewest {
                fewest = holders;
                rarest.clear();
            }
            if holders == fewest {
                rarest.push(i);
            }
        }
        if rarest.is_empty() {
            return None;
        }
        let piece = rarest[rand::rng().random_range(0..rarest.len())];
        let len = self.piece_len(piece);
        self.pieces[piece as usize] = Piece::Active(Active {
            owner: Some(p),
            slots: vec![Slot::Missing; len.div_ceil(BLOCK_LENGTH) as usize],
            data: vec![0; len as usize],
        });
        Some(piece)
    }

    /// Whether every block still missing is asked for.
    fn in_endgame(&self) -> bool {
        self.pieces.iter().all(|piece| match piece {
            Piece::Missing => false,
            Piece::Active(a) => a.slots.iter().all(|s| !matches!(s, Slot::Missing)),
            Piece::Checking | Piece::Have => true,
        })
    }

    fn overdue_after(&self) -> Option<Duration> {
        self.typical.map(|t| t * OVERDUE)
    }

    /// An overdue block, asked of one other peer only, that `p` has.
    fn endgame(&mut self, p: usize, now: Instant) -> Option<Block> {
        if !self.in_endgame() {
            return None;
        }
        let overdue = self.overdue_after()?;
        let mut found = None;
        'pieces: for (i, piece) in self.pieces.iter().enumerate() {
            let Piece::Active(a) = piece else { continue };
            if !self.peers[p].has(i as u32) {
                continue;
            }
            for (s, slot) in a.slots.iter().enumerate() {
                if let Slot::Asked(by) = slot {
                    if by.len() == 1 && by[0].0 != p && now - by[0].1 > overdue {
                        found = Some((i, s));
                        break 'pieces;
                    }
                }
            }
        }
        let (i, s) = found?;
        if let Piece::Active(a) = &mut self.pieces[i] {
            if let Slot::Asked(by) = &mut a.slots[s] {
                by.push((p, now));
            }
        }
        self.peers[p].open += 1;
        Some(self.block(i as u32, s))
    }

    /// When, after `now`, the next request becomes overdue in the endgame,
    /// if one will: the time to look again for endgame requests.
    pub fn next_overdue(&self, now: Instant) -> Option<Instant> {
        if !self.in_endgame() {
            return None;
        }
        let overdue = self.overdue_after()?;
        self.pieces
            .iter()
            .filter_map(|piece| match piece {
                Piece::Active(a) => Some(a.slots.iter()),
                _ => None,
            })
            .flatten()
            .filter_map(|slot| match slot {
                Slot::Asked(by) if by.len() == 1 => Some(by[0].1 + overdue),
                _ => None,
            })
            .filter(|at| *at > now)
            .min()
    }

    /// The slot `block` is, if it is one of `piece`'s.
    fn slot_of(&self, block: Block) -> Option<usize> {
        let piece = block.piece;
        if piece >= self.piece_count() || !block.begin.is_multiple_of(BLOCK_LENGTH) {
            return None;
        }
        let slot = (block.begin / BLOCK_LENGTH) as usize;
        let expected = self.block(piece, slot);
        (block.begin < self.piece_len(piece) && expected == block).then_some(slot)
    }

    /// Peer `p` sent `block`, `data`, at `now`. A block it wasn't asked for
    /// (cancelled, or already sent by another) is ignored.
    pub fn received(&mut self, p: usize, block: Block, data: &[u8], now: Instant) -> Received {
        let mut out = Received::default();
        let Some(s) = self.slot_of(block) else {
            return out;
        };
        let Piece::Active(a) = &mut self.pieces[block.piece as usize] else {
            return out;
        };
        let Slot::Asked(by) = &a.slots[s] else {
            return out;
        };
        let Some(at) = by.iter().find(|(q, _)| *q == p).map(|(_, at)| *at) else {
            return out;
        };
        for (q, _) in by {
            self.peers[*q].open -= 1;
            if *q != p {
                out.cancel.push(*q);
            }
        }
        let start = block.begin as usize;
        a.data[start..start + data.len()].copy_from_slice(data);
        a.slots[s] = Slot::Got(p);
        let took = now - at;
        self.typical = Some(match self.typical {
            Some(typical) => (typical * 7 + took) / 8,
            None => took,
        });
        if a.slots.iter().all(|s| matches!(s, Slot::Got(_))) {
            let Piece::Active(a) =
                std::mem::replace(&mut self.pieces[block.piece as usize], Piece::Checking)
            else {
                unreachable!("matched above");
            };
            let senders = a
                .slots
                .iter()
                .enumerate()
                .map(|(s, slot)| match slot {
                    Slot::Got(q) => (*q, self.block(block.piece, s).length),
                    _ => unreachable!("all arrived"),
                })
                .collect();
            out.complete = Some(Complete {
                piece: block.piece,
                data: a.data,
                senders,
            });
        }
        out
    }

    /// Peer `p` didn't send `block` (rejected): it is asked for again,
    /// unless another peer is still asked for it.
    pub fn rejected(&mut self, p: usize, block: Block) {
        let Some(s) = self.slot_of(block) else {
            return;
        };
        let Piece::Active(a) = &mut self.pieces[block.piece as usize] else {
            return;
        };
        if let Slot::Asked(by) = &mut a.slots[s] {
            if let Some(at) = by.iter().position(|(q, _)| *q == p) {
                by.remove(at);
                self.peers[p].open -= 1;
                if by.is_empty() {
                    a.slots[s] = Slot::Missing;
                }
            }
        }
    }

    /// A complete piece matched its hash.
    pub fn verified(&mut self, piece: u32) {
        self.pieces[piece as usize] = Piece::Have;
    }

    /// A complete piece didn't match its hash: it is fetched again, from
    /// scratch.
    pub fn failed(&mut self, piece: u32) {
        self.pieces[piece as usize] = Piece::Missing;
    }

    /// Drops peer `p`: it is asked for nothing more, its open requests are
    /// given back, and the pieces it was finishing are left partial, for
    /// others.
    pub fn drop_peer(&mut self, p: usize) {
        self.peers[p].dropped = true;
        self.peers[p].open = 0;
        for piece in &mut self.pieces {
            let Piece::Active(a) = piece else { continue };
            if a.owner == Some(p) {
                a.owner = None;
            }
            for slot in &mut a.slots {
                if let Slot::Asked(by) = slot {
                    by.retain(|(q, _)| *q != p);
                    if by.is_empty() {
                        *slot = Slot::Missing;
                    }
                }
            }
        }
    }
}
