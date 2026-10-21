### Peer Wire Spec (as built)

How file contents move between nodes, as built in BitTorrent plan steps 1–7 (2026-10-21). Terms: [GLOSSARY.md](../../GLOSSARY.md), Content. Why: [ADR 0002](../../docs/adr/0002-bittorrent-like-file-download.md). How the developer expects it to work: [expectations.md](expectations.md). Code: `crates/node/src/` `peer_wire.rs` (wire, server, download), `picker.rs`, `choker.rs`, `pieces.rs` (piece length, hashing, staging), `content.rs` (when to fetch).

#### 1. Pieces

* A file version's **piece length** depends on its size only: the smallest power of two from 128 KiB to 16 MiB that splits it into at most 2048 pieces (`pieces::piece_length`); past 32 GiB, more pieces of 16 MiB. 0 for an empty file.
* The **piece hashes** (BLAKE3, in order) are in the file's entry (`FileMeta::pieces`), with the content hash (BLAKE3 of the whole file). The content hash is the swarm's info hash, with the folder and path.
* A **block** is the request unit: 16 KiB (`BLOCK_LENGTH`), the last of a piece may be short. Blocks aren't checked alone; their piece is.

#### 2. Connections and streams

* ALPN `/sync-node/peer-wire/0`. Gated (members only) and tracked like every other ALPN.
* One connection per pair of nodes (the downloader's connection cache, dialled when not there; `CONNECT_TIMEOUT` 10 s). One bidirectional stream per swarm **per downloader**: when both nodes download a file, each opens its own stream to the other. On a stream, pieces and `Have`s go from the server to the downloader only.
* Framing: a little-endian `u32` length, then a postcard body of at most 8 KiB (`MAX_HEADER`). A `Piece` message is followed by exactly its block's bytes (at most 128 KiB, `MAX_BLOCK_LENGTH`).
* A message that doesn't parse, an oversized one, or one that breaks the protocol resets that stream (code 400, `BAD_REQUEST`), never the connection.

#### 3. Messages

The opener sends a `Handshake { folder, path, content }` (within `REQUEST_TIMEOUT`, 10 s). The other end answers with `Refused(NotKept | NoSuchFile)` and finishes the stream, or with its bitfield message. The opener then sends its own bitfield message. After that, `Message`s:

| Message | Meaning |
|---|---|
| `Refused(Refusal)` | Only as the handshake's answer. `NotKept`: the folder isn't kept by this node for that peer (or the peer doesn't keep it). `NoSuchFile`: unsafe path, or this node neither has nor downloads the version. |
| `Bitfield(bytes)`, `HaveAll`, `HaveNone` | Which pieces the sender has: one bit per piece, piece 0 in the high bit of byte 0; spare bits zero (`bitfield_message`, `holds`). Sent once, after the handshake. |
| `Have(piece)` | The sender verified this piece. |
| `Interested`, `NotInterested` | The sender wants pieces the other end has. |
| `Choke`, `Unchoke` | The sender won't / will serve requests. |
| `Request(Block)` | `Block { piece, begin, length }`. |
| `Piece(Block)` + bytes | The answer to a request. |
| `Cancel(Block)` | A request no longer wanted (endgame). Still answered. |
| `Reject(Block, Why)` | A request not served (BEP 6): every request gets a `Piece` or a `Reject`, in order. |

`Why`: `Choked`; `Changed` (the file no longer matches this node's own entry; the index re-hashes it); `NotHave` (this node doesn't have that piece: a leecher, a staging piece that no longer matches, or the version gone or between download and placing); `Invalid` (piece past the last, block empty, past the piece's end, or over 128 KiB); `Policy` (`--direct-only` and the request came over a relay).

The enum order is the wire encoding (postcard stores variant numbers): new variants go at the end.

#### 4. Serving

* **Seeder** (its entry has the version): answers `HaveAll`, serves from the file.
* **Leecher** (it is downloading the version, `peer_wire::Swarms`): answers its download's bitfield, then sends `Have` as its download verifies pieces; serves verified pieces from its staging file.
* Before a piece's first block is sent, the whole piece is read from disk and checked against its hash; the last verified piece of each stream is kept for its next blocks. From the file: only a regular file reached through real directories, with the entry's size and modification time (`read_piece`). From the staging file: `read_staged`.
* A mismatch: from the file, `Reject(Changed)` and an `Apply::Rehash` nudge to the index; from the staging file, `Reject(NotHave)`, the piece leaves the leecher's bitfield, and its download fetches it again.
* Where the version is is decided per request: a leecher whose download completes and whose file is placed serves as a seeder on the same stream.
* Checks run in order: the version still here (else `NotHave`), `Invalid`, a leecher's `NotHave`, `Policy`, `Choked`, then the disk read.

#### 5. Choking

* The choker's unit is a peer **node**, over all its swarm streams. A stream is unchoked while it is interested and the choker has its node unchoked; it sends `Choke` / `Unchoke` when that changes. Requests on a choked stream get `Reject(Choked)`.
* A round (`choker::Choker::round`): the `SLOTS` (4) interested nodes this node uploads to fastest (`Rate` over 5 s), plus one optimistic unchoke. Rounds every `ROUND` (10 s); every `ROTATE_EVERY` (3) rounds the optimistic unchoke moves to the choked interested node that had it least recently, so every node is unchoked in turn. A round also runs whenever a stream's interest changes, so groups of up to 5 nodes are all unchoked at once.
* No tit-for-tat: only what this node uploads counts. A node that only downloads is never choked for that.

#### 6. Downloading

* **When:** the content task (`content.rs`) plans a fetch when the merged view has a version this node lacks. It stages first: a staging file at the folder root, named after path and version (`staging_path`), pieces already there kept (resume), pieces this node has in other files copied (`Staged::open`). Then `Client::download` fetches the rest.
* **The swarm:** the nodes whose entry has the version and every other node that keeps the folder (`Folders::folder_peers`). Nodes not in the swarm are asked again every `REJOIN` (2 s), so a node that gets the version (or comes back) joins a running download. With no peer in the swarm for `GIVE_UP` (30 s), the download ends, and the content task tries again after `RETRY` (10 s).
* **Requests:** to each unchoked peer, as many as `queue_length` says for the rate measured from it: `rate × 2 s / 16 KiB`, from 4 to 250. A peer with requests open and no block for `SNUB_AFTER` (60 s) is snubbed: one request at a time until it sends. `--direct-only`: no requests to a peer reached through a relay.
* **The piece picker** (`picker.rs`): one peer per piece; partial pieces first (one whose peer left is finished by another), then rarest first among the pieces the peer has, ties at random. **Endgame:** once every missing block is asked for, a block whose only request has been open more than 2 × (`OVERDUE`) the typical block time is asked of another peer too; the first answer wins, the other gets `Cancel`.
* **Verification:** a piece's blocks stay in memory until all have arrived; the piece is checked against its hash, then written to the staging file. Nothing unchecked reaches the disk.
* **Dropping a peer for this download:** a piece that fails its hash (every peer that sent part of it), `Reject(Changed | Invalid)`. `Reject(NotHave)` only marks that the peer lacks the piece; `Choked` and `Policy` give the block back.
* **Counting:** `Node::fetched()` counts each verified block once, for the peer that sent it.
* **Done:** the staging file is synced; the index task re-hashes it against the entry, gives it the entry's modification time and moves it into place (`Apply::Place`). The node then seeds it.
* **Superseded:** if another version of the path wins (or it is deleted) during a download, the download stops; the staging file of a version no longer wanted is removed at the next pass (`Memory::staging`). Other stale staging files go after 24 h unwritten (`STALE_STAGING`), never one a running download uses.

#### 7. Failure cases

| Case | What happens |
|---|---|
| A peer goes offline | Its stream ends; its requests go back to the picker; its partial pieces are finished by others. It rejoins on a later `REJOIN` when it is back. |
| This node restarts mid-download | The staging file is rechecked piece by piece on the next fetch; verified pieces are kept. |
| A peer sends a bad piece | The piece is fetched again from scratch; its senders are dropped for this download. |
| A seeder's file changed on disk | `Reject(Changed)`, the seeder re-hashes; the downloader drops it for this download. |
| A leecher's staging piece changed | `Reject(NotHave)`; the piece leaves its bitfield; its own download fetches it again. |
| A newer version wins mid-download | The download stops; its staging file is removed. |
| The path moves to a relay (`--direct-only`) | No more requests to that peer; requests already sent over the relay get `Reject(Policy)` and go back to the picker. |
| A non-member connects | The gate closes the connection. |
| Garbage on a stream | That stream is reset; the connection goes on. |

#### 8. Relays

Allowed by default. `--direct-only` (env `SYNC_NODE_DIRECT_ONLY`, `Config::direct_only`), per node, for testing: a path counts as direct if the selected one isn't a relay (`peers::is_direct`, as `status` counts it). The full relay policy (per peer, the folder index too) is sync-plan step 8; on the peer wire it applies per request, as `Reject(Policy)`.

#### 9. Constants

All guesses, measured only on one machine (BitTorrent plan step 7): none was a cause of a stuck fetch under load. Real links: plan step 8 (the Pi).

| Constant | Value | Where |
|---|---|---|
| `BLOCK_LENGTH`, `MAX_BLOCK_LENGTH` | 16 KiB, 128 KiB | `peer_wire` |
| `MAX_HEADER` | 8 KiB | `peer_wire` |
| `REQUEST_TIMEOUT`, `CONNECT_TIMEOUT` | 10 s, 10 s | `peer_wire` |
| `REJOIN`, `GIVE_UP` | 2 s, 30 s | `peer_wire` |
| `SLOTS`, `ROUND`, `ROTATE_EVERY` | 4 (+1), 10 s, 3 rounds | `choker` |
| `RATE_WINDOW`, `QUEUE_TIME`, `MIN_QUEUE`–`MAX_QUEUE` | 5 s, 2 s, 4–250 | `choker` |
| `SNUB_AFTER` | 60 s | `choker` |
| `OVERDUE` | 2 × typical block time | `picker` |
| `MIN_PIECE_LENGTH`–`MAX_PIECE_LENGTH`, `MAX_PIECES` | 128 KiB–16 MiB, 2048 | `pieces` |
| `RETRY`, `RENAME_GRACE`, `STALE_STAGING` | 10 s, 2 s, 24 h | `content` |

#### 10. Known limits

* **The doc layer decides whether a file arrives in time under load**, not the peer wire (plan step 7): the membership listing, doc sync and metadata blobs. [ADR 0003](../../docs/adr/0003-own-index-exchange-for-folder-docs.md) (proposed) replaces iroh-docs for folder indexes.
* A content task fetches one file at a time per folder; many small files in one folder download one after another.
* On one machine the author of a new file uploads up to about 1.5 × it when two nodes fetch it at once (deep queues to an unlimited seeder; plan step 6). The real ratio is measured on the Pi.
* Partial pieces aren't kept across a restart: only whole verified pieces are in the staging file.
* Every node in a group must run this version: the old block protocol (`/sync-node/blocks/0`) is gone.
