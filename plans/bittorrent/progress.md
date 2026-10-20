# Progress Report (2026-10-20)

Handoff for the next session. Read this first, then [plan.md](plan.md).

## State

* Nothing after `123bff9` is committed. The plan recommends that the developer commits the block work before step 1 (plan, "Git").
* ADR 0002 accepted, glossary in BitTorrent terms, this plan written. **Next: step 0, the developer reviews the docs and commits.** Then step 1 (the rename).
* `just check` passes. `just stress` runs the load tests in `tests/starved.rs`.

## Done in the last block-plan session (uncommitted, kept)

* Step 4: local block copy (`blocks::Staged`), several providers at once, endgame for overdue requests only (`blocks::Schedule`), new providers join a running fetch (`Content::fetch_from`).
* `Node::fetched()`: verified file bytes per provider. Content tests assert exact amounts with it.
* `peers::PathTotals`: traffic totals never go down (they dipped while a closed path's event was pending).
* **Membership fix:** iroh-docs doesn't retry a failed content download, so a member or folder record whose blob didn't arrive stayed unreadable for good. The monitor now calls `Folders::fetch_missing` for the membership doc each presence interval, from the members and the group's chain. Regression test: `a_starved_node_reads_every_record` (`just stress`).
* Step 5: `--with-content` and `Config::content` removed; stale staging files removed by the content pass (unwanted and unwritten for 24 h); sync plan updated.
* Block plan step 5 item 4 (the Pi check of large files and disk use) was not done. It moved to step 8 of this plan.

## Load-test baseline (block protocol, 2026-10-20)

`a_starved_node_fetches_every_file`: 8 two-node groups, the runtime blocked 300 ms of every 350 ms, 4 MiB file, 90 s. Stuck per run of 8: 1, 4, 5, 0, 2, 2, 0 (earlier), then 0, 2, 2, 2.

* Most stuck nodes had part of the file: slow, not stuck. The peer wire replaces this fetch path.
* One 0-byte case was an index fault: b's content passes planned no fetch from 8 s to 73 s, so a's entry arrived about 65 s late. Likely cause (not proved): `Folders::sync_peers` starts doc sync with a node only once. Plan step 2.

## Developer preferences

* Short replies when asked; ASD-STE100 in chat (CLAUDE.md); never touch the git index; never run the armv7 build.
* BitTorrent's design is the chosen direction (2026-10-20), not only a fix for speed. Follow its tried-and-tested choices; leave out only what a trusted group doesn't need (tit-for-tat).
