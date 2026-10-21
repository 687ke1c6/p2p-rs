# Progress Report (2026-10-21)

Handoff for the next session. Read this first, then [plan.md](plan.md) and [expectations.md](expectations.md).

## State

* Block work committed in `93d8f91` (on `main`).
* Step 0 done: the developer reviewed the docs and wrote [expectations.md](expectations.md) (the download ceremony in BitTorrent terms; reviewed and corrected 2026-10-21).
* Step 1 done: `blocks.rs` split into `pieces.rs` and `peer_wire.rs`, all renames, tests renamed, entry encoding fixture `the_entry_encoding_is_unchanged`. `just check` passes.
* Step 2 done in code: `Folders::retry_sync` (doc sync again each presence interval with online peers whose last sync failed), metadata blob fetched on entry arrival with retries (`META_FETCH_TRIES`), probe in `tests/starved.rs`. The index fault is gone; the 30 s latency criterion is not met (doc layer slow under load, see plan step 2 "Result"). Step 7 splits stuck fetches by cause. `just check` passes.
* Steps 1 and 2 committed (`658a2f4`, `9a211f3`).
* Step 3 done, committed (`ae38050`).
* Step 4 done, committed (`450e8fb`).
* Step 5 done, committed.
* Step 6 done (uncommitted): `choker.rs`, choking per peer node, request queues from rate, snubbing; step 5's test limit changed (developer's decision, plan step 6). `just check` passes twice (91 tests).
* **Next: step 7** (load).
* `just stress` runs the load tests in `tests/starved.rs`. Each run takes about 2 min and varies a lot (0 to 8 stuck of 8); judge on 5 runs, never on one.

## Decisions 2026-10-21

* Choking is upload only; no download-side choking (that is tit-for-tat).
* Leechers are not written to the doc. They are found by opening a stream to every online node that keeps the folder.
* A bitfield is sent once, at the handshake; then `Have`.
* A superseded version: the old swarm stops and its staging file is removed at once; no reuse of its pieces (plan step 4).
* Relays allowed by default. `--direct-only` / `SYNC_NODE_DIRECT_ONLY` (`Config::direct_only`) per node, for testing (plan step 4). The full relay policy stays in sync-plan step 8.

## Load-test baseline (block protocol, 2026-10-20)

`a_starved_node_fetches_every_file`: 8 two-node groups, the runtime blocked 300 ms of every 350 ms, 4 MiB file, 90 s. Stuck per run of 8: 1, 4, 5, 0, 2, 2, 0 (earlier), then 0, 2, 2, 2.

* Most stuck nodes had part of the file: slow, not stuck. The peer wire replaces this fetch path.
* One 0-byte case was an index fault: b's content passes planned no fetch from 8 s to 73 s, so a's entry arrived about 65 s late. Likely cause (not proved): `Folders::sync_peers` starts doc sync with a node only once. Plan step 2.

## Developer preferences

* Short replies when asked; ASD-STE100 in chat (CLAUDE.md); never touch the git index; never run the armv7 build.
* BitTorrent's design is the chosen direction (2026-10-20), not only a fix for speed. Follow its tried-and-tested choices; leave out only what a trusted group doesn't need (tit-for-tat).
* Wants to review expectations before more implementation; write down how a thing should work before building it.
