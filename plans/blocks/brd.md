# Block Protocol: Business Requirements

Light BRD. Decision record: [ADR 0001](../../docs/adr/0001-own-block-protocol-for-file-contents.md). Design: [spec.md](spec.md). Build order: [plan.md](plan.md).

## Problem

sync-node moves file contents with iroh-blobs. It works (sync-plan steps 6b and 7 converge in tests and between the Pi and the dev container), but the fit is poor:

* **Every received file is stored twice:** once in the blob store, once in the folder. On the Pi this halves the usable disk.
* **Moved files stop being sendable.** After a rename or a conflict copy, the store can still point at the old path. A node that needs the file from scratch (a new member, or one that was away) may never get it.
* **The relay policy can't be enforced per request.** Sync-plan step 8 needs "file data only over direct paths"; iroh-blobs offers no clean place to refuse one request.

All three come from one mismatch: iroh-blobs owns its bytes and assumes they never change; a synced folder changes all the time and the disk is the only truth.

## Goal

File contents move between nodes over a protocol of our own, on iroh, in which the files on disk are the only copy of the data.

## Who it is for

* **The developer**, running sync-node between the dev container, a laptop and a Raspberry Pi on separate networks.
* **Later, a few trusted machines** in one group (up to 9 fully meshed, more through neighbours).

## Requirements

| # | Requirement | Why |
|---|---|---|
| R1 | Each synced file is stored once per node: in its folder. | Disk on the Pi. |
| R2 | Every byte received is verified against the sender's index before it is used. | A corrupt or malicious peer must not corrupt a folder. |
| R3 | An interrupted transfer resumes where it stopped, across restarts. | Large files, flaky links, the Pi rebooting. |
| R4 | A node can send any file its index lists, whatever was renamed or moved before. | New members and returning nodes must converge. |
| R5 | A file renamed elsewhere is moved locally, never fetched again (as now). | Bandwidth. |
| R6 | A node only serves members that keep the folder, only files its own index lists, and only while the file on disk still matches that entry. | Trust is pinned; a file being edited must not be sent half-written. |
| R7 | Each request can be refused on its own, so the relay policy (sync-plan step 8) can keep file data off relays. | Constraint in the sync plan. |
| R8 | Merge rules, conflicts, renames and deletes behave exactly as today. | Steps 6a to 7 stay valid. |
| R9 | Runs on the Pi (armv7, `just armv7`), with no new native dependencies. | Deployment target. |
| R10 | `folder show --entries` and `b3sum` still agree on a file's content hash. | The developer's way of checking a sync by hand. |

## Out of scope

* Delta transfer inside a block (rsync-style rolling checksums).
* Content-defined chunking or deduplication across files.
* Encryption at rest, untrusted nodes (Syncthing's "untrusted device" mode).
* Moving the index or membership off iroh-docs.
* Syncing file permissions, symlinks or empty directories.

## Success measures

* A received file uses its size once on disk (blob store growth during a sync: index metadata only).
* A transfer stopped at about half way and restarted receives about half the file again, not all of it.
* A file renamed on A, then fetched from scratch by a new node C from A alone, arrives intact.
* Throughput between the dev container and the Pi on a direct path within about 80% of what iroh-blobs achieved (sync-plan step 9 measures both).
* All existing content tests (`tests/content.rs`) pass on the new transport, unchanged in what they assert.

## Risks

| Risk | Mitigation |
|---|---|
| A transfer protocol is easy to get subtly wrong (framing, timeouts, back-pressure). | One QUIC stream per request, so QUIC does the flow control; small fixed set of messages; tests with a misbehaving peer. |
| Hashing a file into blocks costs CPU on the Pi. | Same single read as today's BLAKE3; the block hashes are computed in the same pass. |
| Index entries get bigger. | Block size grows with the file (spec), so a list has at most 2,048 hashes (64 KiB) up to 32 GiB; a 1 GiB file carries 2,048 hashes of 512 KiB blocks. |
| Our code instead of n0's for verified transfer. | Per-block BLAKE3 is simple; the receiver also re-hashes the whole file before placing it, as today. |
