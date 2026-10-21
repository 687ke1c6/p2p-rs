### BitTorrent Plan

* **Progress:** [progress.md](progress.md) is the handoff between sessions; read it first.
* **Why:** [ADR 0002](../../docs/adr/0002-bittorrent-like-file-download.md). **Terms:** [GLOSSARY.md](../../GLOSSARY.md), Content section (BitTorrent's terms).
* **Objective:** file contents move over BitTorrent's peer wire protocol on iroh: swarms per file version, seeders and leechers, 16 KiB blocks, rarest first, choking for upload slots, endgame. Pieces are verified and served from disk, as now.
* **Replaces:** the transport built in [block plan](../blocks/plan.md) steps 2–4 (one stream per piece, seeders only, `Schedule`, `Busy`). Block plan step 5 item 4 (the Pi check) moves into step 8 here. The block plan's docs keep the old terms (block = piece, block list = piece hashes, provider = seeder).
* **Relation to the sync plan:** [sync-plan.md](../sync-plan.md) resumes at step 8 (relay policy) when this plan is done. The relay policy then applies per peer-wire request.
* **Rules:** work in order, one commit per step (the developer commits), tick a box only when its "Done when" holds. `just check` passes at the end of every step. The developer runs every armv7 build.

#### Fit of the work done so far

Nothing after `123bff9` is committed: block plan steps 1–5 (+1390/−550 lines in tracked files, about 1750 in new files). Most of it stays.

| Part | Where | With BitTorrent |
|---|---|---|
| Piece hashes in the index (`FileMeta`), the piece length rule, `Hasher`, `hash_reader` | `blocks.rs`, `index.rs` | **Kept.** Renamed only. |
| Placement: re-hash, piece hash check, journal, `Apply::Rehash`, shared `Entries` | `index.rs`, `folder.rs` | **Kept.** |
| On-disk checks for a served piece (`open_regular`, `read_block`, `NotKept`, `Changed` + rehash, `safe_path`) | `blocks.rs` | **Kept.** Extended to staging files. |
| Staging file, recheck on resume, local copy (`Staged`, `copy_local`, `open_staged`), stale staging cleanup | `blocks.rs`; `content.rs` | **Kept.** A staging file in use by a swarm is never stale. |
| Content task: actions, merge, conflicts, renames, `retry_at` | `content.rs` | **Kept.** |
| New seeders join a running fetch (`Content::fetch_from`, the doc watch) | `content.rs` | **Replaced** by swarm membership: bitfields and `Have` say who has what. |
| Framing (`write_message`, `read_message`, 8 KiB limit) | `blocks.rs` | **Kept**, for the new messages. |
| Wire (`BlockRequest`, `BlockResponse`, `Refusal`), one stream per piece, `BlocksProtocol`, `request_block` | `blocks.rs` | **Replaced.** |
| `Client::fetch_file`, `Schedule`, `Provider`, `Event`, `WINDOW`, `BUSY_*`, `MAX_SERVING`, endgame | `blocks.rs` | **Replaced.** The endgame's overdue rule moves to the piece picker. |
| `Node::fetched()`, `peers::PathTotals`, gate, `Peers` tracking | `node.rs`, `peers.rs`, `gate.rs` | **Kept.** |
| `--with-content` removed | `main.rs`, `node.rs` | **Kept.** |
| Membership fix (`Folders::fetch_missing` each presence interval) | `monitor.rs`, `folder.rs` | **Kept.** |
| Tests: piece hashing, index, staging (`tests/blocks.rs` first 7) | | **Kept.** Renamed. |
| Tests: `Schedule` (`tests/blocks.rs` last 7) | | **Replaced** by piece picker and choker tests. |
| Tests: serving checks (`tests/block_protocol.rs`) | | **Kept**, on the new wire. |
| Tests: `tests/content.rs` (14) | | **Kept.** 4 change their byte assertions: leechers upload too. |
| Tests: `tests/peers.rs`, `tests/starved.rs` | | **Kept.** `starved.rs` is the load yardstick. |

In `blocks.rs` about 35% stays and 65% is replaced. Across all the uncommitted work, about 80% stays.

#### Git: what to do with the uncommitted work

**Recommendation: the developer commits it now, before step 1, as one commit ("Block protocol, block plan steps 1–5").**

* It is tested: `just check` passes, and steps 1–3 were confirmed on the Pi.
* About 80% of it stays. The 20% that goes is the baseline that step 7 compares against (`just stress` results), so it must be in history.
* Step 1 renames most of these files. A rename on top of uncommitted work makes one diff that nobody can review.
* Suggested: commit on a branch (`feature/bittorrent`), and one commit per step on top. Merge to `main` after step 8.

Not recommended: discard the work (loses the 80% that stays), or keep it uncommitted through the plan (one diff of thousands of lines, no point to go back to).

#### Design

* **ALPN:** `/sync-node/peer-wire/0`. Gated and tracked like the others. `/sync-node/blocks/0` is removed.
* **Connections:** one peer-wire connection per pair of nodes (ALPN `/sync-node/peer-wire/0`, kept in the `Client` connection cache, dialled when not there). It is separate from the gossip, docs and blobs connections: a QUIC connection has one ALPN. **One bidirectional QUIC stream per swarm** on it: BitTorrent has one TCP connection per torrent; a QUIC stream is the same thing at a lower cost. The node that wants pieces opens the stream.
* **Handshake** (first message on the stream, then the answer): `{ folder, path, content }`. The swarm's id is the content hash with the folder and path, as an info hash. The other end answers with its bitfield (`HaveAll` for a seeder, as BEP 6), or rejects (`NotKept`, `NoSuchFile`) and closes the stream.
* **Messages after it** (both ways, framed as now): `Bitfield`, `HaveAll`, `HaveNone`, `Have(piece)`, `Interested`, `NotInterested`, `Choke`, `Unchoke`, `Request { piece, begin, length }`, `Piece { piece, begin }` + bytes, `Cancel { piece, begin, length }`, `Reject { piece, begin, length, why }` (BEP 6 fast extension: a request is answered, never dropped). `why`: `Choked`, `Changed`, `NotHave`, `Invalid`, `Policy` (sync-plan step 8).
* **Block:** 16 KiB (`BLOCK_LENGTH`); the last block of a piece may be short. A requested block of more than 128 KiB is rejected.
* **Serving:** only verified pieces. A seeder reads from the file, a leecher from its staging file; both re-hash the whole piece before the first block of it is sent (a cache of the last few verified pieces avoids a re-hash per block). A mismatch → `Reject(Changed)` and `Apply::Rehash` (file) or the piece goes back to missing (staging file).
* **Piece picker** (per swarm): partial pieces first, then rarest first among pieces some unchoked peer has, ties at random. One peer per piece while not in the endgame, so a bad piece has one sender, who is dropped from the swarm. **Endgame:** once every missing block is asked for, an overdue block (open more than 2 × the typical block time, as now) is asked of another peer too, and the loser gets `Cancel`.
* **Request queue:** per peer, sized from the speed measured to it: `rate × 2 s / BLOCK_LENGTH`, at least 4, at most 250 (the libtorrent rule). A peer with no block for 60 s is snubbed: queue of 1 until it sends.
* **Choker** (per node, over all swarms): every 10 s, unchoke the 4 interested peers this node uploads to fastest; every 30 s, move the optimistic unchoke to another interested peer. In a group of 2–4 nodes every interested peer is unchoked. No tit-for-tat.
* **Swarm membership:** the content task starts a swarm for a fetch and opens a stream to each online node that keeps the folder. A node fetching the same version answers with its bitfield and becomes a peer. A node that holds the version but whose entry hasn't synced yet is found on the next try.
* **Relays:** allowed by default. `--direct-only` (env `SYNC_NODE_DIRECT_ONLY`, `Config::direct_only`), per node, for testing: no requests to a peer whose selected path is a relay, `Reject(Policy)` for a request that arrives over one, and a peer's open requests go back to the picker when its path moves to a relay. Content only; the full relay policy (per peer, folder index too) stays in sync-plan step 8. See [expectations.md](expectations.md).
* **Superseded version:** when a new version of a path wins, the old swarm stops and its staging file is removed at once. The new swarm starts fresh; old staging pieces are not reused.
* **Leaving:** a leecher that completes the piece set sends `Have` for the last piece, places the file, and stays as a seeder (now served from the placed file). Each side closes a stream when neither is interested.

#### Step 0: Decision & Docs

* [x] ADR 0002 accepted; glossary in BitTorrent terms; this plan (2026-10-20).
* [x] The developer reviews the docs and commits the block work (`93d8f91`, on `main`; review of the docs 2026-10-21, with [expectations.md](expectations.md)).
* **Done when:** the docs are accepted and the block work is committed.

#### Step 1: Rename to BitTorrent Terms

No change of behaviour.

* [x] `blocks.rs` → `pieces.rs` (piece length, hashing, staging, on-disk reads, local copy) and `peer_wire.rs` (wire, server, client), split along the "Fit" table.
* [x] Renames: `block_size` → `piece_length`, `FileMeta::{block_size, blocks}` → `{piece_length, pieces}`, `Hashed::blocks` → `pieces`, `MIN/MAX_BLOCK_SIZE` → `MIN/MAX_PIECE_LENGTH`, `MAX_BLOCKS` → `MAX_PIECES`, `read_block` → `read_piece`, `Provider` → `Peer`, provider → peer in names, logs, comments and test names. `folder show --entries` prints pieces.
* [x] Tests: `tests/blocks.rs` → `tests/pieces.rs`, `tests/block_protocol.rs` → `tests/peer_wire.rs`.
* [x] Check that the index encoding is unchanged: an index written before the rename reads after it. `the_entry_encoding_is_unchanged` (`tests/pieces.rs`) holds an entry's bytes captured before the rename.
* Done 2026-10-21. Kept: the ALPN value `/sync-node/blocks/0` (a new value is a change of behaviour; step 4 removes it). Also renamed: `BlockRequest`/`BlockResponse` → `PieceRequest`/`PieceResponse`, `BlocksProtocol` → `PeerWireProtocol`, `request_block`/`fetch_block` → `request_piece`/`fetch_piece`, `BLOCK_TIMEOUT` → `PIECE_TIMEOUT`, "block server" → "peer-wire server".
* **Done when:** `just check` passes and no "block" in the code means a piece.

#### Step 2: Doc Sync Retry (index fault)

Not BitTorrent, but step 7's load test can't pass without it.

* [x] `Folders::sync_peers` starts sync with a node once only. Start it again each presence interval with every online node that keeps the folder **whose last doc sync failed or hasn't finished** (decided 2026-10-21: not every interval with every node; a full reconciliation each 5 s per peer and folder buys nothing after a success). `Running::synced` holds the peers whose last `SyncFinished` was `Ok`; `Folders::retry_sync` runs each presence interval; `resync_with` clears the peer, so a node that comes back syncs again until it succeeds.
* [x] Prove the cause first: a probe in `tests/starved.rs` that logs when b's doc gets a's entry for the file, and the iroh-docs sync results between them.
  * Probe (2026-10-21, 5 runs): `Stuck::timeline` has both docs' `SyncFinished`, gossip neighbours, the entry's arrival, and when each node first started doc sync with the other.
  * Index fault seen 2 times in 40 groups: b starts doc sync with a once, then nothing for 70 s (no sync result, no neighbour); a never starts sync with b (it hasn't read b's "keeps" record). b never gets the entry.
  * iroh-docs 0.101 (`engine/live.rs`) syncs only on `start_sync`, a new gossip neighbour, or a neighbour's report; a failed sync is not tried again. Our `sync_peers` calls `start_sync` once per node, `resync_with` only when a member comes online.
  * Most `Err("Failed to close connection1")` results moved the entry anyway (it arrives right after): the error is on close.
  * Other stuck groups were not index faults: a late folder listing (membership, 32–67 s) or the entry in view and the transport slow (steps 3–6).
* **Done when:** in 5 runs of `a_starved_node_fetches_every_file`, no b that kept the folder has `in_view: false` or waits more than 30 s for a's entry, counted from when b keeps the folder (`view_after` in the probe). A late folder listing (membership doc) is not this step's fault and is not counted; the fetch itself is steps 3–7.
* [x] Metadata blob retry (found by the probe after the sync retry): docs fetches an entry's metadata blob once, on arrival, and under load that always failed (no `ContentReady`); the entry then waited for the tick-based `fetch_missing_meta`, 30–49 s. The doc event task now fetches it from the sending node, 1 s apart, up to `META_FETCH_TRIES` (10) times.
* **Result (2026-10-21, 10 runs after the sync retry, 5 after the metadata retry):** the index fault is gone: no b that kept the folder lacked a's entry for want of a sync. **The 30 s criterion is not met:** in the last 5 runs, 11 of 37 listed groups had the file in view after 30–64 s, and 1 had the entry but not its metadata after 56 s. The time is spread over the doc layer under load: 10–17 s from keeping the folder to the first doc sync, failed syncs, the metadata blob's download. Also seen once: a doc sync that hung for 65 s, blocking every retry with that peer (iroh-docs has no timeout in `connect_and_sync`; only a patch of iroh-docs can add one). Not chased further here: step 7 counts these against the doc layer, as input to a possible ADR 0003 (own index exchange, Syncthing-style).

#### Step 3: Peer Wire, Seeder Side

* [x] `peer_wire.rs`: ALPN, messages, handshake, the per-stream serve loop. A seeder answers `HaveAll`, starts unchoked (no choker yet), serves blocks with the on-disk checks, answers every request with `Piece` or `Reject`.
* [x] Tests (port `tests/block_protocol.rs`): a member gets blocks that make up pieces matching the piece hashes; rejected: `NotKept`, `NoSuchFile`, `Changed` (and the index catches up), unsafe path, a request over 128 KiB, a block past the end of the piece; a non-member can't connect; a garbage or oversized message closes the stream, not the connection.
* **Done when:** a node serves exactly the blocks of the pieces its index describes, and rejects everything else.
* Done 2026-10-21. As built (more exact than the design above):
  * The old protocol stays beside it until step 4: its ALPN and server are now `PIECE_REQUEST_ALPN` and `PieceRequestProtocol`; `ALPN` and `PeerWireProtocol` are the new ones.
  * `Wire` is the stream at either end (`open`, `send`, `send_piece`, `recv`); step 4's download loop uses it.
  * Handshake answer: `Refused(NotKept | NoSuchFile)` and the stream is finished, or the answerer's bitfield message. The opener sends its own bitfield message after it.
  * A seeder starts choked and sends `Unchoke` on `Interested`, so step 6's choker only has to decide when.
  * `Why::Invalid` added (not in the design's list): a block past the end of its piece, empty, or over `MAX_BLOCK_LENGTH` (128 KiB), or a piece past the last. Checks run in order: entry still has the version (`Changed`), `Invalid`, `Choked`, then the disk read.
  * The cache of verified pieces is the last piece per stream only: a piece can be 16 MiB, and a cache per stream of more would cost too much memory on the Pi.
  * Requests are answered in order on the stream, so a `Cancel` always comes too late and is ignored.

#### Step 4: Download from Seeders

* [ ] Piece picker (pure bookkeeping, tested on its own like `Schedule`): partial first, rarest first, one peer per piece, endgame with `Cancel`, a peer's pieces back to the pool when it is dropped.
* [ ] Download loop per swarm: handshake with each seeder, `Interested`, requests up to a fixed queue of 16 per peer, write each verified piece to the staging file.
* [ ] `--direct-only` / `SYNC_NODE_DIRECT_ONLY` (`Config::direct_only`, off by default): the download loop skips relay-only peers, the server answers `Reject(Policy)` over a relay, and a path change to a relay returns that peer's requests to the picker.
* [ ] A superseded version stops its swarm and removes its staging file at once.
* [ ] The content task fetches with it. Remove `/sync-node/blocks/0`, `Client::fetch_file`, `Schedule`, `WINDOW`, `BUSY_*`, `MAX_SERVING`, `Content::fetch_from`'s doc watch.
* [ ] Tests: all of `tests/content.rs` passes. Picker tests replace the `Schedule` tests. A superseded version leaves no staging file. Direct only: on `Network::Local` every path is direct, so a test checks that fetches still pass with it on; the relay case is checked by the developer (block outbound UDP on one side, as sync-plan step 8).
* **Done when:** two and three nodes converge over the peer wire only, and a bad piece's sender is dropped.

#### Step 5: Leechers Serve

* [ ] A swarm's staging file is registered while the fetch runs; the server serves its verified pieces from it. `Bitfield` at the handshake, `Have` for each verified piece.
* [ ] Stale staging cleanup skips files of running swarms.
* [ ] Tests: a writes a 32 MiB file, b and c fetch it at the same time: a uploads less than 1.5 × the file, and b and c each receive pieces from the other. A staging piece that no longer matches is rejected and fetched again.
* **Done when:** a new file reaches several nodes while its author uploads most pieces about once.

#### Step 6: Choker & Request Queues

* [ ] Speed per peer, both ways. Request queue from the measured rate; snubbing.
* [ ] Choker: 4 upload slots + 1 optimistic unchoke, 10 s and 30 s rounds, over all swarms. `Reject(Choked)` for requests that arrive while choked.
* [ ] Tests (choker as bookkeeping, tested on its own): the fastest interested peers are unchoked; the optimistic unchoke rotates; a peer that only downloads is never choked for that alone; a snubbed peer gets one request at a time.
* **Done when:** with more interested peers than slots, the slots go to the fastest, and every peer is sometimes unchoked.

#### Step 7: Load

* [ ] `just stress`: `a_starved_node_fetches_every_file` and `a_starved_node_reads_every_record` pass 5 runs in a row.
* [ ] Compare with the block-protocol baseline (0, 2, 2, 2 of 8 stuck, 2026-10-20).
* [ ] Split each stuck fetch by its cause with the probe in `tests/starved.rs` (`Stuck::timeline`, `view_after`): doc layer (listing, sync, metadata blob) or peer wire. If the doc layer still causes stuck fetches, the developer decides on ADR 0003 (own index exchange for folder docs, Syncthing-style; folder index first, membership after, as its own plan).
* [ ] Constants that are guesses (block length, queue limits, slots, rounds, timeouts) are written in the notes with what the load tests showed.
* **Done when:** no fetch is stuck under load.

#### Step 8: Cleanup & Hand-Back

* [ ] Spec: `plans/bittorrent/spec.md` as built (wire, picker, choker, failure cases).
* [ ] Sync plan: points step 8 at the per-request `Reject(Policy)`.
* [ ] armv7 and CLI (includes the old block plan step 5 item 4): the developer rebuilds, syncs a folder with some large files between the Pi and the dev container, and checks disk use on the Pi.
* **Done when:** the sync plan can resume at step 8.
