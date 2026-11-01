//! The last [`LOG_CAPACITY`] transfers of file data over the peer wire, for
//! the web UI's Transfers view: one entry per download attempt of a version
//! of a file, and one per upload of a version of a file to one peer. Kept in
//! memory only. A running transfer updates its entry as pieces and blocks
//! go; once it ends, its entry no longer changes.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use iroh::EndpointId;
use iroh_blobs::Hash;
use iroh_docs::NamespaceId;

use crate::choker::Rate;

/// How many transfers the log keeps. A running one is never dropped, so the
/// log can hold more while that many run.
pub const LOG_CAPACITY: usize = 500;

/// A piece in [`TransferRecord::sources`]: no source yet.
pub const SOURCE_MISSING: u8 = u8::MAX;
/// A piece in [`TransferRecord::sources`]: in the staging file before this
/// download started (an earlier attempt fetched it).
pub const SOURCE_EARLIER: u8 = u8::MAX - 1;
/// A piece in [`TransferRecord::sources`]: its blocks came from more than one
/// peer, or from a peer past the last one numbered.
pub const SOURCE_MIXED: u8 = u8::MAX - 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Download,
    Upload,
}

/// How a transfer ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransferOutcome {
    /// A download: the file is complete. An upload: the peer ended the
    /// stream (its download ended).
    Done,
    /// A download: no peer in the swarm for a while; it is tried again
    /// later, from the pieces it has.
    GaveUp,
    Failed(String),
    /// Stopped by this node: a newer version, the folder disabled or
    /// removed, or the node stopping.
    Stopped,
}

/// What one peer sent (a download) or got (an upload).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerShare {
    pub id: EndpointId,
    /// Bytes of verified pieces (a download), or of blocks sent (an upload).
    pub bytes: u64,
    /// Pieces it sent all of (a download only).
    pub pieces: u32,
}

/// One transfer, as the log has it.
#[derive(Debug, Clone)]
pub struct TransferRecord {
    pub id: u64,
    pub direction: Direction,
    pub folder: NamespaceId,
    pub path: String,
    pub content: Hash,
    pub size: u64,
    pub piece_length: u32,
    pub pieces: u32,
    pub started: SystemTime,
    pub ended: Option<SystemTime>,
    /// `None` while it runs.
    pub outcome: Option<TransferOutcome>,
    /// A download: each peer that sent pieces, in the order they first did.
    /// An upload: the one peer.
    pub peers: Vec<PeerShare>,
    /// A download: per piece, the index in `peers` of the peer that sent it,
    /// or one of the `SOURCE_` values. Empty for an upload.
    pub sources: Vec<u8>,
    /// A download: pieces that didn't match their hash.
    pub bad_pieces: u32,
    /// An upload: how many pieces the peer has, as it last said.
    pub remote_have: Option<u32>,
    /// Bytes per second now, while it runs: an upload's own. A download's
    /// is its swarm peers' (see `peer_wire::InSwarm::rate`), so 0 here.
    pub rate: f64,
}

impl TransferRecord {
    /// Bytes moved: over all its peers.
    pub fn bytes(&self) -> u64 {
        self.peers.iter().map(|p| p.bytes).sum()
    }
}

/// The log, shared by the downloads and the server.
#[derive(Debug, Clone, Default)]
pub struct TransferLog(Arc<Mutex<LogState>>);

#[derive(Debug, Default)]
struct LogState {
    next_id: u64,
    /// Oldest first, so by ID.
    entries: VecDeque<Entry>,
}

#[derive(Debug)]
struct Entry {
    record: TransferRecord,
    rate: Rate,
}

/// What a transfer is of, to start its entry.
pub struct Subject<'a> {
    pub direction: Direction,
    pub folder: NamespaceId,
    pub path: &'a str,
    pub content: Hash,
    pub size: u64,
    pub piece_length: u32,
    pub pieces: u32,
}

impl TransferLog {
    /// Starts an entry for `subject`, running until the handle ends it or
    /// is dropped. Drops the oldest finished entries past [`LOG_CAPACITY`].
    pub fn start(&self, subject: Subject<'_>) -> Logged {
        let mut state = self.0.lock().unwrap();
        let id = state.next_id;
        state.next_id += 1;
        state.entries.push_back(Entry {
            record: TransferRecord {
                id,
                direction: subject.direction,
                folder: subject.folder,
                path: subject.path.to_string(),
                content: subject.content,
                size: subject.size,
                piece_length: subject.piece_length,
                pieces: subject.pieces,
                started: SystemTime::now(),
                ended: None,
                outcome: None,
                peers: Vec::new(),
                sources: Vec::new(),
                bad_pieces: 0,
                remote_have: None,
                rate: 0.0,
            },
            rate: Rate::default(),
        });
        let mut over = state.entries.len().saturating_sub(LOG_CAPACITY);
        state.entries.retain(|e| {
            let drop = over > 0 && e.record.outcome.is_some();
            over -= usize::from(drop);
            !drop
        });
        Logged {
            log: self.clone(),
            id,
        }
    }

    /// Every entry, newest first, with the running ones' rates as of now.
    pub fn snapshot(&self) -> Vec<TransferRecord> {
        let now = Instant::now();
        let mut state = self.0.lock().unwrap();
        state
            .entries
            .iter_mut()
            .rev()
            .map(|e| {
                let mut record = e.record.clone();
                if record.outcome.is_none() {
                    record.rate = e.rate.per_second(now);
                }
                record
            })
            .collect()
    }

    fn update(&self, id: u64, f: impl FnOnce(&mut Entry)) {
        let mut state = self.0.lock().unwrap();
        let at = state.entries.binary_search_by_key(&id, |e| e.record.id);
        if let Ok(at) = at {
            f(&mut state.entries[at]);
        }
    }
}

/// A running transfer's entry: ended by [`Logged::end`], or as
/// [`TransferOutcome::Stopped`] when dropped (the transfer's task was stopped).
#[derive(Debug)]
pub struct Logged {
    log: TransferLog,
    id: u64,
}

impl Logged {
    pub fn id(&self) -> u64 {
        self.id
    }

    /// A download's pieces held before it starts.
    pub fn held(&self, have: &[bool]) {
        self.log.update(self.id, |e| {
            e.record.sources = have
                .iter()
                .map(|h| if *h { SOURCE_EARLIER } else { SOURCE_MISSING })
                .collect();
        });
    }

    /// A download verified `piece`, from `senders` (node, bytes), one per
    /// block.
    pub fn verified(&self, piece: u32, senders: &[(EndpointId, u32)]) {
        self.log.update(self.id, |e| {
            let record = &mut e.record;
            let mut source = None;
            for (id, bytes) in senders {
                let at = match record.peers.iter().position(|p| p.id == *id) {
                    Some(at) => at,
                    None => {
                        record.peers.push(PeerShare {
                            id: *id,
                            bytes: 0,
                            pieces: 0,
                        });
                        record.peers.len() - 1
                    }
                };
                record.peers[at].bytes += u64::from(*bytes);
                source = match source {
                    None => Some(at),
                    Some(s) if s == at => Some(at),
                    Some(_) => Some(usize::MAX),
                };
            }
            let source = match source {
                Some(at) if at < usize::from(SOURCE_MIXED) => {
                    record.peers[at].pieces += 1;
                    at as u8
                }
                _ => SOURCE_MIXED,
            };
            if let Some(slot) = record.sources.get_mut(piece as usize) {
                *slot = source;
            }
        });
    }

    /// A download's piece didn't match its hash.
    pub fn bad_piece(&self) {
        self.log.update(self.id, |e| e.record.bad_pieces += 1);
    }

    /// An upload sent `bytes` to `peer`.
    pub fn sent(&self, peer: EndpointId, bytes: u32) {
        self.log.update(self.id, |e| {
            e.rate.add(u64::from(bytes), Instant::now());
            match e.record.peers.first_mut() {
                Some(share) => share.bytes += u64::from(bytes),
                None => e.record.peers.push(PeerShare {
                    id: peer,
                    bytes: u64::from(bytes),
                    pieces: 0,
                }),
            }
        });
    }

    /// An upload's peer has `pieces` pieces now.
    pub fn remote_have(&self, pieces: u32) {
        self.log
            .update(self.id, |e| e.record.remote_have = Some(pieces));
    }

    pub fn end(self, outcome: TransferOutcome) {
        self.finish(outcome);
    }

    fn finish(&self, outcome: TransferOutcome) {
        self.log.update(self.id, |e| {
            if e.record.outcome.is_none() {
                e.record.outcome = Some(outcome);
                e.record.ended = Some(SystemTime::now());
                e.record.rate = 0.0;
            }
        });
    }
}

impl Drop for Logged {
    fn drop(&mut self) {
        self.finish(TransferOutcome::Stopped);
    }
}

/// Milliseconds since the Unix epoch.
pub fn unix_ms(at: SystemTime) -> u64 {
    at.duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn subject(direction: Direction) -> Subject<'static> {
        Subject {
            direction,
            folder: NamespaceId::from(&[1u8; 32]),
            path: "a.txt",
            content: Hash::new("a"),
            size: 3,
            piece_length: 1,
            pieces: 3,
        }
    }

    fn node(n: u8) -> EndpointId {
        iroh::SecretKey::from_bytes(&[n; 32]).public()
    }

    #[test]
    fn records_who_sent_each_piece() {
        let log = TransferLog::default();
        let logged = log.start(subject(Direction::Download));
        logged.held(&[true, false, false]);
        logged.verified(1, &[(node(1), 1)]);
        logged.verified(2, &[(node(2), 1), (node(1), 1)]);
        logged.bad_piece();
        let record = &log.snapshot()[0];
        assert_eq!(record.sources, vec![SOURCE_EARLIER, 0, SOURCE_MIXED]);
        assert_eq!(record.peers[0].bytes, 2);
        assert_eq!(record.peers[0].pieces, 1);
        assert_eq!(record.peers[1].pieces, 0);
        assert_eq!(record.bad_pieces, 1);
        assert_eq!(record.outcome, None);
        logged.end(TransferOutcome::Done);
        assert_eq!(log.snapshot()[0].outcome, Some(TransferOutcome::Done));
    }

    #[test]
    fn a_dropped_handle_stops_its_entry() {
        let log = TransferLog::default();
        let logged = log.start(subject(Direction::Upload));
        logged.sent(node(1), 10);
        assert!(log.snapshot()[0].rate > 0.0);
        drop(logged);
        let record = &log.snapshot()[0];
        assert_eq!(record.outcome, Some(TransferOutcome::Stopped));
        assert!(record.ended.is_some());
        assert_eq!(record.rate, 0.0);
        assert_eq!(record.bytes(), 10);
    }

    #[test]
    fn keeps_the_newest_finished_and_every_running_entry() {
        let log = TransferLog::default();
        let running = log.start(subject(Direction::Download));
        for _ in 0..LOG_CAPACITY + 10 {
            log.start(subject(Direction::Upload))
                .end(TransferOutcome::Done);
        }
        let entries = log.snapshot();
        assert_eq!(entries.len(), LOG_CAPACITY);
        assert_eq!(entries.last().unwrap().id, running.id());
        assert_eq!(entries[0].id, (LOG_CAPACITY + 10) as u64);
    }
}
