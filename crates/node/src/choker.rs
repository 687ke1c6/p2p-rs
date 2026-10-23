//! Choking and request queues (see `plans/bittorrent/plan.md`, step 6):
//! the bookkeeping, with the time passed in.
//!
//! * [`Choker`]: which interested peers this node uploads to. The
//!   [`SLOTS`] it uploads to fastest, plus one optimistic unchoke that moves
//!   on each rotation to the choked interested peer that had it least
//!   recently, so every peer is sometimes unchoked and its speed measured.
//!   No tit-for-tat: what a peer uploads to this node doesn't count, so a
//!   node that only downloads is never choked for that.
//! * [`Rate`]: bytes per second over the last [`RATE_WINDOW`], for upload
//!   rates and download rates alike.
//! * [`queue_length`]: how many requests to keep open to a peer, from the
//!   speed measured from it; [`SNUB_AFTER`] without a block, just one.

use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::Hash;
use std::time::{Duration, Instant};

use crate::peer_wire::BLOCK_LENGTH;

/// Upload slots, besides the optimistic unchoke.
pub const SLOTS: usize = 4;

/// How often the choker looks again at who uploads fastest.
pub const ROUND: Duration = Duration::from_secs(10);

/// Every how many rounds the optimistic unchoke moves on (30 s).
pub const ROTATE_EVERY: u32 = 3;

/// The span a [`Rate`] averages over.
pub const RATE_WINDOW: Duration = Duration::from_secs(5);

/// How long a peer with open requests may send no block before it is
/// snubbed: one request at a time until it sends.
pub const SNUB_AFTER: Duration = Duration::from_secs(60);

/// The fewest and most requests kept open to one peer (libtorrent's rule).
pub const MIN_QUEUE: usize = 4;
pub const MAX_QUEUE: usize = 250;

/// How long the requests open to a peer should take it to send.
pub const QUEUE_TIME: Duration = Duration::from_secs(2);

/// Which interested peers are unchoked, peers named by `K`.
#[derive(Debug)]
pub struct Choker<K> {
    optimistic: Option<K>,
    /// When each peer last had the optimistic unchoke, by pick number.
    last_optimistic: HashMap<K, u64>,
    picks: u64,
}

impl<K> Default for Choker<K> {
    fn default() -> Self {
        Self {
            optimistic: None,
            last_optimistic: HashMap::new(),
            picks: 0,
        }
    }
}

impl<K: Copy + Eq + Hash + Ord> Choker<K> {
    /// The peers to unchoke now, of the `interested` ones with the rates
    /// this node uploads to them at: the [`SLOTS`] fastest, plus the
    /// optimistic unchoke, moved on if `rotate` (or if it is no longer a
    /// choked interested peer).
    pub fn round(&mut self, interested: &[(K, f64)], rotate: bool) -> HashSet<K> {
        let mut ranked = interested.to_vec();
        ranked.sort_by(|(a, ra), (b, rb)| rb.total_cmp(ra).then(a.cmp(b)));
        let mut unchoked: HashSet<K> = ranked.iter().take(SLOTS).map(|(k, _)| *k).collect();
        let candidates: Vec<K> = ranked
            .iter()
            .map(|(k, _)| *k)
            .filter(|k| !unchoked.contains(k))
            .collect();
        let keep = self
            .optimistic
            .filter(|o| !rotate && candidates.contains(o));
        self.optimistic = keep.or_else(|| {
            let next = candidates
                .iter()
                .min_by_key(|k| (self.last_optimistic.get(k).copied(), **k))
                .copied()?;
            self.picks += 1;
            self.last_optimistic.insert(next, self.picks);
            Some(next)
        });
        unchoked.extend(self.optimistic);
        unchoked
    }

    /// The optimistic unchoke of the last round, if there was a choked
    /// interested peer for it.
    pub fn optimistic(&self) -> Option<K> {
        self.optimistic
    }
}

/// Bytes per second, over the last [`RATE_WINDOW`].
#[derive(Debug, Default)]
pub struct Rate {
    samples: VecDeque<(Instant, u64)>,
}

impl Rate {
    pub fn add(&mut self, bytes: u64, now: Instant) {
        self.samples.push_back((now, bytes));
        self.trim(now);
    }

    fn trim(&mut self, now: Instant) {
        while self
            .samples
            .front()
            .is_some_and(|(at, _)| now.duration_since(*at) > RATE_WINDOW)
        {
            self.samples.pop_front();
        }
    }

    /// The rate at `now`: the bytes in the window over its length (at least
    /// one second, so a first burst doesn't count as a huge rate).
    pub fn per_second(&mut self, now: Instant) -> f64 {
        self.trim(now);
        let Some((first, _)) = self.samples.front() else {
            return 0.0;
        };
        let span = now
            .duration_since(*first)
            .max(Duration::from_secs(1))
            .as_secs_f64();
        let bytes: u64 = self.samples.iter().map(|(_, b)| b).sum();
        bytes as f64 / span
    }
}

/// How many requests to keep open to a peer sending `rate` bytes per
/// second: enough for [`QUEUE_TIME`], from [`MIN_QUEUE`] to [`MAX_QUEUE`].
/// One if it is `snubbed`.
pub fn queue_length(rate: f64, snubbed: bool) -> usize {
    if snubbed {
        return 1;
    }
    let blocks = rate * QUEUE_TIME.as_secs_f64() / f64::from(BLOCK_LENGTH);
    (blocks as usize).clamp(MIN_QUEUE, MAX_QUEUE)
}
