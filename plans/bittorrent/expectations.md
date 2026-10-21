### BitTorrent Expectations

How the developer expects file download to work, from the view of a user and of a node. Not how to build it. Reviewed against [plan.md](plan.md), [ADR 0002](../../docs/adr/0002-bittorrent-like-file-download.md) and [GLOSSARY.md](../../GLOSSARY.md).

* **Status:** draft, reviewed 2026-10-21
* **Open questions:** mark them **TO CLARIFY** where they occur, and list them in the last section.

#### Goals

Replicate a BitTorrent download between iroh nodes of one group. A download starts after the folder's doc has synced across the nodes (done, out of scope) and a node's merged view says that n files are missing or stale on its disk. Each of those files (one version of one file) is a swarm, and the node starts a download ceremony for each.

**Relays allowed, direct only behind a switch.** By default, pieces may go through a relay. For testing (and maybe the default later), `--direct-only` (env `SYNC_NODE_DIRECT_ONLY`) makes content move only between peers on a direct path (selected path is IP, as the sync plan's "Direct only" policy). One switch per node, for content only; the full per-peer relay policy is still sync-plan step 8. Under direct only, a pair with only a relay path still converges through a neighbour that has a direct path to each (sync plan, "Convergence is transitive"):

* A node doesn't request blocks from a peer it reaches only through a relay.
* A node answers a request that arrives over a relay with `Reject(Policy)`.
* If a path changes from direct to relay during a download, that peer's open requests go back to the piece picker, as if it went offline. When the path is direct again, requests to it start again.

#### Terms

Refer to GLOSSARY.md. Used here:

* **Connection:** one QUIC connection between two nodes, on one ALPN (`/sync-node/peer-wire/0`). A connection has exactly one ALPN, so it is separate from the gossip, docs and blobs connections. One per pair of nodes.
* **Stream:** one bidirectional QUIC stream on that connection, for one swarm. Many swarms between the same two nodes are many streams on one connection.

#### Setup: Nodes, Groups and Connections

__Out of scope__
- How members become part of the group.
- How a merged view emerges over iroh-docs.
- How a merged view describes a needed download.

__In scope__
- The doc says who the **seeders** are: an entry with the version means that node has every piece.
- The doc does **not** say who the leechers are, and it must not. An entry describes what is on a node's disk; a "partial" entry would break the meaning of its version, and each state change would write to the doc and gossip it to the group. Leechers are found on the wire (see below). In a small trusted group, the group is the tracker.

#### Scenario: A File Changes on One Node

1. A member changes a file locally. The watcher sees it, the node re-hashes the file (content hash and piece hashes) and writes a new entry to the doc.
2. The doc syncs to the group. Every node computes the new merged view. A node whose disk doesn't match it has a download to do.
3. **Local copy first.** The node opens the staging file for that version (or finds the one from a stopped fetch and rechecks it), and copies every piece it already has in its own files. Each copied piece is checked against the piece hashes.
4. **Join the swarm.** The node opens a stream to each online node that keeps the folder, and sends the handshake `{ folder, path, content }` (the swarm's info hash). From now on it is a **leecher**, even with zero pieces.
5. The other end answers with its bitfield: `HaveAll` (a seeder), `Bitfield` (a leecher that has some pieces), `HaveNone` (a leecher with none), or rejects (`NotKept`, `NoSuchFile`) and closes the stream. This is how leechers find each other: any node fetching the same version answers with its bitfield and becomes a peer.
6. The bitfield is sent **once**, at the handshake. After that, each peer sends `Have(piece)` for each piece it verifies. Nothing re-fetches bitfields.
7. The leecher sends `Interested` to each peer that has pieces it lacks, and waits for `Unchoke`.
8. When unchoked, it sends `Request { piece, begin, length }` for 16 KiB blocks. The piece picker picks: partial pieces first, then rarest first (among pieces held by unchoked peers), ties at random. One peer per piece while not in the endgame.
9. The peer answers every request with `Piece` + bytes, or `Reject` with why (`Choked`, `Changed`, `NotHave`, `Policy`). A request is never dropped (BEP 6).
10. The leecher writes blocks to the staging file. When all blocks of a piece are there, it verifies the piece against its hash:
    * Match: the piece is kept, and the leecher sends `Have(piece)` to every peer in the swarm.
    * Mismatch: the piece goes back to missing, and the peer that sent it is dropped from the swarm (only one peer sent it).
11. **Endgame:** once every missing block is asked for, an overdue block is asked of another peer too. The first answer wins; the other gets `Cancel`.
12. When every piece is verified, the leecher sends `Have` for the last piece, places the file (rename from the staging file; same file system, so atomic), and stays in the swarm as a **seeder**, now serving from the placed file. Nothing pauses: open handles still read after the rename.
13. A stream closes when neither end is interested.

Fin

#### Scenario: Several Peers Have the File

* A leecher downloads from **every** peer that has unchoked it. There is no limit on download peers.
* How much it asks of each peer depends on the speed measured to that peer: the **request queue** is `rate × 2 s / 16 KiB` blocks, at least 4, at most 250. A fast peer gets more requests, a slow one fewer.
* Rarest first spreads the pieces, so leechers soon have pieces to give each other, and the author uploads each piece about once (plan step 5: the author uploads less than 1.5 × the file to two leechers).

#### Scenario: Serving (Seeder and Leecher)

* **Choking is about upload.** Each node chooses whom it uploads to. Every 10 s it unchokes the 4 interested peers it uploads to fastest. Every 30 s it moves the optimistic unchoke to another interested peer, so that peer's speed can be measured. One choker per node, over all swarms.
* **No tit-for-tat.** What a peer gives this node does not count. A node that only downloads (a new laptop, a backup) is never choked for that.
* In a group of 2–4 nodes, every interested peer is unchoked.
* A request from a choked peer gets `Reject(Choked)`.
* A node serves only verified pieces: a seeder from its file, a leecher from its staging file. It re-hashes the whole piece from disk before the first block of it is sent. If the file changed on disk, it sends `Reject(Changed)`, and the index re-hashes (file) or the piece goes back to missing (staging file).

#### Scenario: A Leecher Has Some Pieces

* Its bitfield at the handshake lists the pieces it has verified. Then it sends `Have` for each new one.
* It serves those pieces from its staging file, under the same choker and the same on-disk checks as a seeder.
* A staging file in use by a swarm is never removed as stale.
* When it has every piece, it places the file and seeds from there (step 12 above).

#### Scenario: A Peer Goes Offline or Comes Back

* **Goes offline:** the connection closes, so the stream ends and the peer leaves the swarm. Its open requests go back to the piece picker. The counts used for rarest first go down by its pieces.
* **Comes back:** the content task opens a new stream to it on its next try, with a new handshake and bitfield.
* **This node stops and starts again:** it finds its staging file, rechecks the pieces in it (BitTorrent's resume), and continues from there.

#### Scenario: The File Changes During a Download

* A new version has a new content hash, so it is a **new swarm**. The old swarm stops, and its staging file is removed at once (not after the 24 h stale cleanup).
* The trigger is a change of the winning content in the merged view, not only a version bump. If the version changes but the content doesn't (an mtime change, for example), there is nothing to download.
* The new swarm starts fresh: a new staging file, local copy from this node's placed files (step 3), then the ceremony as above. Pieces in the old staging file are not reused; simpler, and the cost is only the pieces fetched for the old version.

#### Scenario: Slow or Overloaded Node

* **As a peer this node downloads from:** it isn't choked by the downloader (only an uploader chokes). It gets fewer requests, because its queue follows its measured speed. A peer that sends no block for 60 s is **snubbed**: queue of 1 until it sends. In the endgame, its overdue blocks are asked of another peer too.
* **As an uploader (the Pi, for example):** its choker unchokes the peers it uploads to fastest; the rest wait or get pieces from other peers.

#### Limits and Rules

* Block: 16 KiB, the last block of a piece may be short. A request for more than 128 KiB is rejected.
* Request queue per peer: from measured speed, 4 to 250 blocks. Snubbed after 60 s with no block.
* Upload slots: 4 + 1 optimistic unchoke, per node over all swarms. Rounds of 10 s (unchoke) and 30 s (optimistic).
* Not configurable. They are constants; plan step 7 writes down what the load tests showed for each.

#### What You Expect to See

Needs expansion. What exists now:

* `Node::fetched()`: verified bytes per peer. Content tests assert exact amounts with it.
* `peers::PathTotals`: traffic totals per path.
* `just stress`: the load tests in `tests/starved.rs`. Baseline (block protocol): 0, 2, 2, 2 of 8 stuck.
* Plan step 5: the author of a new file uploads less than 1.5 × the file while two nodes fetch it.

#### Differences from BitTorrent

Kept: swarms, seeders and leechers, bitfield + `Have`, interested and choke states, 16 KiB blocks, rarest first, partial pieces first, endgame with `Cancel`, request queues from measured speed, snubbing, upload slots + optimistic unchoke, the fast extension (`HaveAll`, `HaveNone`, `Reject`), resume by recheck.

Different:

* **No tit-for-tat:** the group is trusted, and download-only nodes are normal.
* **No tracker or DHT:** the doc names the seeders; leechers answer when asked; the group is the tracker.
* **Info hash:** the content hash, with the folder and path.
* **One connection per pair of nodes, one stream per swarm**, instead of one TCP connection per torrent.
* **Piece hashes:** BLAKE3, in the doc entry, instead of a .torrent file.
* **Served from disk:** the files are the only copy, so each piece is re-hashed from disk before it is served.
* **Members only:** the gate closes connections from non-members.
* **Direct only (switch):** with `--direct-only`, pieces never go through a relay.

#### Out of Scope

* Tombstones: a delete downloads nothing.
* A member removed during a download: the gate closes its connections, and it leaves every swarm as an offline peer does.

#### Open Questions

None open. Decided 2026-10-21:

* [Goals](#goals): relays allowed for now; direct only behind `--direct-only` / `SYNC_NODE_DIRECT_ONLY`, for testing.
* [The File Changes During a Download](#scenario-the-file-changes-during-a-download): discard the old staging file, start fresh.
