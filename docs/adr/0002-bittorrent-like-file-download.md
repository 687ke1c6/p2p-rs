---
status: accepted
---

# BitTorrent's peer wire protocol for file contents

File contents move as in BitTorrent. Each version of a file is a torrent, and the nodes that hold or fetch it are its swarm. A node fetches pieces from every peer in the swarm: seeders (their entry has the version) and leechers (they serve the pieces they have verified, from their staging file). Each pair of nodes keeps one connection on `/sync-node/peer-wire/0`, with one QUIC stream per swarm, as BitTorrent has one connection per torrent. On it: a bitfield when the swarm starts, `Have` for each verified piece, `Interested`, `Choke` and `Unchoke`, requests for 16 KiB blocks of a piece, rarest first, with a request queue sized from the speed measured to that peer, and the endgame with `Cancel`.

Choking manages upload slots, as a BitTorrent seeder does: the interested peers this node uploads to fastest are unchoked, plus one optimistic unchoke that rotates. It replaces `MAX_SERVING` and `Busy`. Tit-for-tat is left out: every node in a group is trusted, and a node that only downloads (a new laptop, a backup) must never be choked for it.

Decided 2026-10-20, after block-plan step 5. Changes the transport of ADR 0001; see `plans/bittorrent/plan.md`.

## Why

The developer chose the tried-and-tested BitTorrent design over more fixes to our own fetch. The step-4 fetch asks one piece per stream, only from seeders, with a fixed window of 8 requests. Under load (`tests/starved.rs`) a piece (128 KiB to 16 MiB) is all or nothing within `BLOCK_TIMEOUT`, a busy seeder answers `Busy` and is dropped after `BUSY_TRIES`, and every node fetches a new file from the one node that wrote it. Each of those was fixed one at a time; BitTorrent answers all of them with one model.

## Kept from ADR 0001

- Our own protocol on iroh, with the files on disk as the only copy of the data. iroh-blobs stays for iroh-docs' metadata only.
- Pieces and their BLAKE3 hashes in the index (the piece hashes) are the unit of verification. A piece is written, announced and served only once its hash matches.
- Local copy, resume (BitTorrent's recheck) from the staging file, staging names and their cleanup.
- The server's on-disk checks: a node serves a piece only if the file (or staging file) on disk still matches; else it rejects the request and the index re-hashes.
- The gate (members only) and the relay policy (sync-plan step 8), applied per request.

## Considered Options

- **Keep the step-4 fetch and tune it** (smaller requests, a bigger window, retry `Busy`). Less work, but each fault is fixed on its own, every node still fetches a new file from its author, and `MAX_SERVING` stays a blunt limit.
- **Full BitTorrent, tit-for-tat included.** Fair in a swarm of strangers, wrong in a trusted group: it punishes nodes that only download.
- **One stream per request, as now, with bitfields added.** Smaller change, but no place for choking or for per-peer state, which is where BitTorrent's behaviour under load comes from.
- **iroh-blobs' multi-provider download.** Rejected with iroh-blobs in ADR 0001.

## Consequences

- The terms change to BitTorrent's (`GLOSSARY.md`): block → piece, block list → piece hashes, block size → piece length, provider → peer (seeder or leecher); a block is now the 16 KiB request unit. The code is renamed to match. The index encoding doesn't change (postcard doesn't store field names), so config dirs are kept.
- `/sync-node/blocks/0` is removed, not kept beside the new ALPN. Every node in a group must run the new version.
- A leecher serves from its staging file, so a staging file must be found from (path, content) while its fetch runs, and must not be removed while it is served.
- Content tests that assert exact bytes per seeder change: leechers now upload too.
- The step-4 `Schedule`, `WINDOW`, `BUSY_*` and `MAX_SERVING` go. A piece picker (rarest first, partial pieces first, endgame) and a choker replace them.
- Not fixed by this change: an entry or metadata blob that iroh-docs fails to sync waits until something starts a new sync (the 0-byte case in `tests/starved.rs`). The plan fixes it as its own step.
