### Block Protocol Plan

* **Superseded (2026-10-20)** by the [BitTorrent plan](../bittorrent/plan.md) ([ADR 0002](../../docs/adr/0002-bittorrent-like-file-download.md)), which keeps steps 1 and 5 and replaces the transport of steps 2–4. These docs keep the old terms: block = piece, block list = piece hashes, block size = piece length, provider = seeder.
* **Progress:** [progress.md](progress.md) is the handoff between sessions; read it first.
* **Why:** [ADR 0001](../../docs/adr/0001-own-block-protocol-for-file-contents.md). **What:** [brd.md](brd.md). **How:** [spec.md](spec.md).
* **Objective:** file contents move over our own block protocol on iroh, served from disk and verified per block, replacing iroh-blobs for file data.
* **Relation to the sync plan:** [sync-plan.md](../sync-plan.md) is paused after step 7 while this plan runs. Its step 8 (relay policy) and step 9 (cross-network check) resume on top of this one. Steps 6b and 7 keep their logic (merge, conflicts, renames, journal); this plan swaps the transport under them.
* **Rules:** as in the sync plan: work in order, one commit per step, tick a box only when its "Done when" holds. `just check` passes at the end of every step. The developer runs every armv7 build.

#### Step 0: Decision & Docs

* [x] ADR 0001, BRD, spec and this plan written (2026-10-19).
* [x] `GLOSSARY.md` started, with the terms the docs use.
* [x] The developer reviews and accepts the docs (2026-10-19).
* **Done when:** the docs are accepted and the sync plan points here.

#### Step 1: Block Lists in the Index

* [x] `FileMeta` gains `block_size` and `blocks` (spec §2). The index computes them in the same read as the whole-file hash; tombstones and empty files have none.
* [x] ~~Remove the by-reference import into the blob store~~ Moved to step 3 (changed 2026-10-19): a provider must have a file in its store before anyone asks for it, and nothing tells it when someone will, so the planned bridge couldn't work. Until step 3, with content transfer on, the index still imports by reference; it now hashes with our own reader as well (a second read), since the import gives no block list.
* [x] `folder show --entries` prints the block count and size.
* [x] Tests: the block size rule at each boundary (0, 1, 128 KiB, 256 MiB + 1, 32 GiB + 1); a scanned file's block list matches hashes computed independently; same content gives the same list; existing index and folder tests pass.
* [x] armv7: no new dependencies; config dirs are cleared on both nodes (breaking index change). Confirmed by the developer (2026-10-20).
* **Done when:** every entry carries a correct block list.

**Notes:**

* **Code:** `blocks.rs` holds the block size rule (`block_size`), `Hasher` (whole hash and block list in one pass) and `hash_reader`. `index.rs` hashes files with it; `FileMeta::file` and `FileMeta::tombstone` build entries. A fetched file's entry copies the block list from an entry holding that content (`content.rs`, `take`).
* **Placement checks the block list too:** the re-hash before a file is placed now also computes its block list and refuses the file if it differs from the entry's, since the list comes from another node. Test: `unsafe_paths_are_refused` writes an entry whose content hash is right but whose block list is wrong; it is never placed (and is, with the check removed).
* **Old config dirs:** entries in the old format can't be decoded. Opening an index logs how many of this node's own entries couldn't be read and says to clear the config dir; other nodes' old entries are skipped.
* **Tests:** `tests/blocks.rs`: block size boundaries, block hashes against hashes of each chunk computed separately, odd-sized updates, empty files, exact multiples, and a scanned folder's entries (same list for the same content at two paths; none for empty files and tombstones).

#### Step 2: Serving Blocks

* [x] `blocks.rs`: ALPN `/sync-node/blocks/0`, wire types, framing with the 8 KiB header limit, the server handler (spec §4), registered on the router, gated and tracked.
* [x] The index task shares a read handle on its entries; `Folders` keeps the registry the server uses (spec §6).
* [x] A client function that fetches one block, for tests and for step 3.
* [x] Tests: a member that keeps the folder gets a block that matches the block list; refused: a member that doesn't keep the folder (`NotKept`), an unknown path or old content (`NoSuchFile`), a file edited on disk (`Changed`, and the index catches up), an unsafe path; a non-member can't connect (gate); an oversized or garbage header closes the stream without harming the server.
* **Done when:** a node serves exactly the blocks its index describes, and refuses everything else.

**Notes:**

* **Code:** `blocks.rs` holds the wire types (`BlockRequest`, `BlockResponse`, `Refusal`), framing (`write_message`, `read_message`), the client (`request_block`, with a 30 s timeout and no hash check of its own) and the server (`BlocksProtocol`, one task per stream, 64 requests at once, then `Busy`). The gate already gates and tracks every ALPN it doesn't leave open, so the new one needed only registering on the router. `Folders` is now built before the router, which needs it.
* **Entries are shared:** `index::Entries` holds this node's entries for a folder (`Arc<FileMeta>` values, so a request doesn't copy a block list). The index task is the only writer; `Folders::serving` hands it to the server, with the folder root and a sender to the index task.
* **The index task always takes requests now**, content transfer or not, and has a new one: `Apply::Rehash`, which re-reads a file even if its size and time match. The server sends it (without waiting, dropped if the queue is full) whenever a file doesn't match its entry, which catches an edit that kept the size and time.
* **Reading a block:** every directory on the way must be a real directory, the file must be a regular file and the same inode after opening as before (no symlink, nothing swapped in), with the entry's size and time; then the block must hash to the entry's. Otherwise `Changed`. A folder the remote doesn't keep, or one this node doesn't keep, is `NotKept` either way, so the answer doesn't say which folders exist. A paused folder is `NoSuchFile`.
* **Tests:** `tests/block_protocol.rs`. Checked by breaking the code: without the block hash check, the Rehash nudge, the symlink checks or the `NotKept` check, the test fails.

#### Step 3: Fetching Files

* [x] `blocks::fetch_file` (spec §5): fixed-name temporary file, resume by checking existing blocks, a window of 8 requests, per-block verification, provider fallback, `Busy` retry.
* [x] The content task fetches with it; `Apply::Place` takes the finished temporary file; the index task re-hashes and places it as today.
* [x] Remove the iroh-blobs content path: the by-reference import (and the second read it causes), the `Downloader` for file contents, export, the store fallback. iroh-blobs stays for index metadata only.
* [x] Tests: all of `tests/content.rs` passes on the new transport, with the resume test rewritten (see notes); a provider that can't send hands over to another; blocks that don't match the block list are never written and their sender is dropped; R4: a file renamed on A is fetched intact by a new node C from A alone; R1: the blob store doesn't grow by the size of the files received.
* [x] armv7 and CLI: the developer rebuilds, clears both config dirs and repeats the 6b/7 CLI checks between the Pi and the dev container. Confirmed by the developer (2026-10-20).
* **Done when:** two and three nodes converge with file contents moving only over the block protocol, and a received file is stored once.

**Notes:**

* **Code:** `blocks::Client` keeps one connection per provider and fetches a file (`fetch_file`): it opens the staging file (`staging_path`, at the folder root) at full length, keeps the blocks already there that match the block list, then asks providers in turn, up to 8 requests open, checking each block before writing it at its offset. A refusal, a failed request or a block that doesn't match ends that provider's turn; the blocks it didn't send go to the next. `Busy` is retried 25 times, 200 ms apart. A local write error stops the fetch.
* **Content task:** `fetch` first copies another local file with the same content (the "local sources" of step 4, whole files only), else fetches over the network; either way the index task checks the staged file (hash and block list) when it places it, and deletes it if it doesn't check out. A failed network fetch waits 10 s before the next try of that file: retrying on every folder change asked a node sending bad blocks for a window of blocks each time (the mismatch test caught it).
* **Removed:** the by-reference import, the store checks and second read in the index, the export and store fallback in placement, the content `Downloader`, the `content` flag on `FolderIndex`, and the `bao-tree` test dependency. iroh-blobs is left only for iroh-docs' metadata (`fetch_missing_meta`).
* **Spec changes:** staging files live at the folder root, not beside the target; providers are tried in node ID order, not randomly (spec §5).
* **`Node::peers()`** returns traffic per connected node, whether or not this node's membership doc lists it yet. Tests read traffic through it: through `status()` they saw 0 bytes under load, before the membership doc had synced.
* **Resume test:** writes the staging file a fetch stopped half way leaves (full length, first half written) before the node starts fetching; only about half the file then crosses the wire. Stopping a real transfer at a chosen point isn't reliable on one machine, where 8 MiB moves in well under a second.
* **Tests:** `tests/content.rs` (8 tests). New: `a_renamed_file_reaches_a_new_node` (R4 and R1), `a_failing_provider_hands_over` (the first provider can't read its file: C gets it all from the second), `blocks_that_dont_match_are_refused` (a's doc advertises a wrong block list: nothing is written, and only about a window is fetched). Checked by breaking the code: without the client's block check, resuming, the hand-over or the path checks, the matching test fails. `unsafe_paths_are_refused` now gives b the bad content locally, so only the path checks stop the escape.
* **Under heavy load** (three test processes at once, about 60 nodes on one machine) some tests time out waiting for the membership doc to list a folder, as in sync-plan step 7. Single runs (and `just check`) pass.

#### Step 4: Local Blocks & Several Providers

* [x] Before requesting a block, look for it on disk: in the old version of the same path, or another file with the same content (spec §5, "local sources first"). An append to a large file then fetches only the new blocks.
* [x] ~~Optionally~~ spread one file's blocks across several providers (changed 2026-10-20: the developer wants it regardless of measurements, to spread upload across the group). Only nodes holding the complete file are providers.
* [x] Endgame (added 2026-10-20): once no block is left to hand out, an overdue block is also asked of a second provider; the first answer wins.
* [x] New providers join a running fetch (added 2026-10-20): a node that finishes its copy while another is still fetching starts sending to it.
* [x] Tests: appending to a 10 MiB file transfers about the appended size; editing the middle of a file transfers about one block.
* **Done when:** unchanged blocks are never fetched over the network.

**Notes:**

* **Code:** `blocks::Staged::open` opens the staging file, keeps the blocks already there that match (resume, as in step 3), then copies missing blocks from this node's own files (`copy_local`), and `Client::fetch_file` fetches only what is still missing. Every copied block is read through the same checks as a served one (`open_regular`: real directories, a regular file, the same inode) and hashed against the block list before it is written; a block that doesn't match stays missing and comes over the network. A file complete from local blocks alone is synced and placed without asking any node.
* **Sources:** `Content::sources`: this node's entry at the same path first (the old version), then every other entry sharing at least one block hash with the wanted list. A block is matched by hash, wherever it sits in the source. A source with a different block size has blocks of a different length, so it matches nothing.
* **Replaces** step 3's whole-file `local_source` copy: a file with the same content shares every block.
* **Limit:** fixed-size blocks, as in BEP: an insert or delete in the middle shifts every later block, so only the blocks before it are reused. An overwrite in place or an append reuses all unchanged blocks. A file that grows past 256 MiB gets a bigger block size and reuses nothing.
* **Several providers:** `Client::fetch_file` connects to every provider at once and keeps up to `WINDOW` requests open to each, all drawing from one queue of missing blocks, so a faster provider takes more. A provider that can't be reached, refuses, fails or sends a wrong block is dropped for the file; its open requests are aborted and their blocks go back to the queue (a request that finished just before the abort is ignored). `Busy` pauses only that provider. See the endgame below.
* **Endgame:** `blocks::Schedule` holds the bookkeeping, apart from the network code, so it is tested on its own: the queue, the window, which provider is asked for which block and since when, and a moving average of how long delivered requests took. In the endgame a block is asked of a second provider only when its request is open more than `OVERDUE` (2) times that average; a timer wakes the fetch loop when the next request becomes overdue. First version asked for every open block as soon as the queue was empty: with providers of the same speed both sent most of them, about a window (1 MiB) wasted per file and provider, 33% on 4 MiB files in `providers_share_the_load`.
* **Writes moved to the fetch loop:** a request's task only checks the hash; the loop writes the block if the schedule still wants it. Before, a stopped request could still be writing (its blocking write can't be aborted), which the endgame would make common: a late write after placing changes the file's mtime.
* **Tests:** `tests/blocks.rs`: `providers_draw_from_one_queue_up_to_the_window`, `the_endgame_asks_again_only_for_overdue_blocks`, `without_a_delivery_nothing_is_overdue`, `a_dropped_provider_s_blocks_go_to_the_others` (found blocks given back in reverse order), `a_returned_block_is_asked_for_again_first`. `providers_share_the_load` now fetches 4 files and asks each provider for more than 1/16 of the bytes: a fetch uses the providers known when it starts, and under load c learned of b only after most of a single file had arrived (seen in 1 of 5 runs). With only the first provider used, the other sends about 0.3 MB and the test fails.
* **Byte limits in tests:** the step 4 content tests allow `OVERHEAD` (512 KiB) on top of the data they expect, since traffic counts include presence and doc sync, which grow with the time a loaded machine takes (1.32 MB seen against a 1.31 MB limit). Without the local copy they receive 6–11 MB, so the margin still catches it.
* **Joining a running fetch:** `Content::fetch_from` runs the fetch alongside a watch of the folder's doc: on each change it reads that path's entries (`index::read_path`, one key) and offers any new node holding the wanted content to `fetch_file` over a `watch` channel; `fetch_file` connects to it and `Schedule::add_provider` gives it room. The watch takes the content task's wake (`Notify` keeps one), so it gives one back after the fetch. Test: `a_new_provider_joins_a_running_fetch` (c fetches 32 MiB from a; b adds the folder with the file part way through; b must send more than 1/32). Checked by breaking the code: with the offer never sent, b sends about 0.4 MB and the test fails. `Schedule`: `a_provider_added_late_takes_what_is_left`.
* **Byte counts in tests (2026-10-20, replaces `OVERHEAD`):** `an_edit_in_the_middle_fetches_one_block` once failed with 10.9 MB received. Cause: the traffic counter could go down. `Peers::snapshot` skipped a connection whose handle had gone, and a path that had left the connection's list, until their close events were handled; under load the counter dipped (up to 22 KB in each of three runs, measured) and came back, so a `before` read in a dip counted old traffic as new. Two fixes: `Node::fetched()` counts file data alone (verified blocks written, per provider, each once, from `blocks::Client`), and the step 3 and 4 tests assert exact amounts with it (`assert_eq!`; no allowance); `peers::PathTotals` keeps each path's bytes last seen until its close is handled, so traffic totals never go down (`tests/peers.rs`; the same dip check found no dip in three runs after the fix). `blocks_that_dont_match_are_refused` still reads traffic, since bad blocks are never written.
* **Idle traffic:** none to speak of: presence only, about 1.4 KB/s at the tests' 200 ms interval (about 56 B/s per peer at the default 5 s). The 150 KB/s seen under load was the tail of setup: two nodes with two docs opened 16 doc-sync connections (8–13 KB each way) and 4 metadata blob connections (10–45 KB). Each doc sync is a new connection, so most of it is connection setup; worth measuring on the Pi in sync-plan step 9.
* **Follow-ups:** speed as a factor (a window in bytes, sized from each provider's measured rate, as BitTorrent sizes its request queues).

* **Test:** `providers_share_the_load`: a and b hold a 16 MiB file, c fetches it; each sends more than a fifth, together less than 1.25 times the file. Checked by breaking the code: with only the first provider used, a sends all of it and the test fails. The drop paths are covered by `a_failing_provider_hands_over` and `blocks_that_dont_match_are_refused`. A provider going away mid-file isn't tested, for the reason given under step 3's resume test.
* **Tests:** `tests/content.rs`: `an_append_fetches_only_the_new_blocks` (1 MiB onto 10 MiB), `an_edit_in_the_middle_fetches_one_block` (100 bytes changed), `blocks_are_copied_from_other_files` (a new file made of half an existing one and new bytes). `tests/blocks.rs`: `staging_copies_only_blocks_that_match` (a source whose bytes no longer match its entry isn't copied). Checked by breaking the code: without the local copy, the three content tests fail (6–11 MB received instead of about 1 MB); without the hash check on copied blocks, the staging test fails.

**Open fault (found 2026-10-20, not fixed):** with the runtime stalled (300 ms blocked in every 350 ms; `tests/starved.rs` style), 8 two-node groups each fetching a 4 MiB file: after 90 s, 6 of 8 receivers had fetched 0 bytes and 2 had part of the file; in another run all 8 finished in 59–82 s. So some fetches never start, rather than run slowly. Not yet known where they stop: the folder listing, the folder doc's entries or their metadata blobs, the providers, or connecting through the gate. Same method as the membership fix: a stall harness and a probe at each of those points.

#### Step 5: Cleanup & Hand-Back

* [x] Remove stale temporary files ~~at scan time~~ (changed 2026-10-20: in each content pass, see notes): a `.sync-node-tmp-…` file that matches no version this node still wants.
* [x] Remove `--with-content`: content transfer is always on (`Config::content` goes too).
* [x] Update the sync plan: mark the parts of steps 6b and 7 this plan replaced (blob store notes, the deferred moved-files problem), and point step 8 at spec §8.
* [ ] armv7 and CLI: the developer rebuilds and runs a sync of a folder with some large files between the Pi and the dev container, checking disk use on the Pi.
* **Done when:** the sync plan can resume at step 8 with no iroh-blobs file-data code left.

**Notes:**

* **Stale staging files:** removed by the content task at the start of each pass (`Content::remove_stale_staging`), not by the index scan: only the content task knows which versions are wanted. A file goes when it has a staging name (`blocks::is_staging_name`: the prefix and 16 hex digits, so the placement journal never matches), no planned fetch or conflict resolution uses it, and nothing has written to it for `STALE_STAGING` (24 h). The age is needed because a name doesn't say which version it is for, and right after a start, before the folder's doc has synced, no fetch is planned: the first version, without it, deleted the partial download `a_partial_transfer_resumes` writes, in every run. A partial download untouched for a day is fetched again from the start. Test: `stale_staging_files_are_removed` (two old unwanted staging files go; an old file that only looks like one, and a recent unwanted one, stay). Checked by breaking the code: without the cleanup, the test fails.
* **`--with-content` removed:** `run` always transfers content, and `Config::content` is gone. `tests/folder_sync.rs` checked node states before any transfer (`Missing`, `Outdated`, `WillDelete`), which now last only a moment; it now waits for every path to be in sync and checks the files on disk (fetched files, the conflict copy on both nodes, the edit beating a delete, the delete). The states themselves stay covered by `tests/view.rs`.

