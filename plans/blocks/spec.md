# Block Protocol: Technical Spec

Light spec. Why: [ADR 0001](../../docs/adr/0001-own-block-protocol-for-file-contents.md). What for: [brd.md](brd.md). Build order: [plan.md](plan.md). Terms as in [GLOSSARY.md](../../GLOSSARY.md).

The model is Syncthing's Block Exchange Protocol (BEP), cut down to what sync-node needs: block lists in the index, one request per block, served from disk, verified per block.

## 1. What changes, what stays

| Part | Today (sync-plan steps 6b, 7) | With the block protocol |
|---|---|---|
| Transport, NAT traversal, relays | iroh | iroh (unchanged) |
| Membership, presence | iroh-docs, iroh-gossip (patched) | unchanged |
| Index (entries, versions, merged view) | iroh-docs, `FileMeta` metadata blobs | unchanged, but `FileMeta` gains a block list |
| Index metadata blobs | iroh-blobs (via iroh-docs) | unchanged |
| Local files in the blob store | imported by reference | **not imported** |
| Fetching a file | iroh-blobs `Downloader`, then export to a temporary file | **block protocol**, straight into the temporary file |
| Placing, conflicts, renames, journal, merge rules | index task, `content.rs` | unchanged (place takes a finished temporary file) |
| `run --with-content` | turns content transfer on | removed in plan step 5: content transfer is always on |

## 2. Block lists in the index

`FileMeta` gains two fields:

```rust
pub struct FileMeta {
    pub content: Option<Hash>, // BLAKE3 of the whole file, as today (b3sum agrees)
    pub size: u64,
    pub mtime_ns: u64,
    pub node: EndpointId,
    pub version: Version,
    pub block_size: u32,       // 0 for a tombstone or an empty file
    pub blocks: Vec<Hash>,     // BLAKE3 of each block, in order
}
```

* **Block size:** the smallest power of two from 128 KiB to 16 MiB that splits the file into at most 2,048 blocks; files over 32 GiB use 16 MiB blocks and longer lists. It depends only on the size, so the same content always has the same block list. The last block may be short.

  | File size | Block size | Blocks (max) | List size (max) |
  |---|---|---|---|
  | up to 256 MiB | 128 KiB | 2,048 | 64 KiB |
  | up to 1 GiB | 512 KiB | 2,048 | 64 KiB |
  | up to 32 GiB | 16 MiB | 2,048 | 64 KiB |

* **Hashing:** the index reads each file once, feeding every buffer to the whole-file hasher and to the current block's hasher. Same I/O as today.
* **Compatibility:** none. The postcard encoding of `FileMeta` changes, so config dirs are cleared (`sync-node --clear-config`) when a node first runs this version. Nothing is migrated.

## 3. Wire protocol

* **ALPN:** `/sync-node/blocks/0`. Gated like docs, gossip and blobs: only admitted members (`gate.rs`), and tracked by `Peers` for traffic stats.
* **Connections:** a node keeps one connection per provider while it fetches from it, and drops it when idle.
* **One bidirectional QUIC stream per request.** QUIC streams are cheap and carry their own flow control, so there is no multiplexing or back-pressure logic of our own. A client limits how many requests it has open per provider (the window, §5).
* **Framing:** a message is a little-endian `u32` length and a postcard body; a body over 8 KiB is a protocol error. Block bytes follow their header raw.

```rust
// Client → server, then the client finishes its send side.
struct BlockRequest {
    folder: NamespaceId,
    path: String,   // `/`-separated, relative; must pass `index::safe_path`
    content: Hash,  // the version of the file the block belongs to
    index: u32,     // block number
}

// Server → client, then the server finishes the stream.
enum BlockResponse {
    Data { len: u32 },     // followed by exactly `len` bytes
    Refused(Refusal),
}

enum Refusal {
    NotKept,     // the requester doesn't keep this folder
    NoSuchFile,  // no live entry for `path` with this `content`
    Changed,     // the file on disk no longer matches the entry
    Busy,        // too many requests in progress; try later or elsewhere
    Policy,      // the relay policy forbids file data on this path (sync-plan step 8)
}
```

The request names the content and the block number, not a byte range: both sides derive offset and length from the block size, and the client already knows the expected hash from the entry.

## 4. Serving a block

On each request, the server:

1. Reads the request with a 10 s timeout; refuses a malformed one by closing the stream.
2. Checks the requester keeps the folder (it is in the folder's node list) → `NotKept`.
3. Checks the relay policy for this connection's selected path (step 8; always allowed until then) → `Policy`.
4. Looks up its **own** entry for `path` in that folder, from the folder's in-memory index (the index task shares a read handle). No live entry, or a different `content` → `NoSuchFile`. Paths that fail `safe_path` never match an entry.
5. Takes a permit from a global limit (64 requests in progress) → `Busy` if none.
6. Opens the file without following symlinks, checks its size and modification time still match the entry → `Changed`, and asks the index task to reconcile the path.
7. Reads the block, hashes it, compares with its own block list → `Changed` (same size and time but different bytes: an edit the watcher hasn't reported yet), and asks for a reconcile.
8. Sends `Data` and the bytes.

A node only ever serves bytes it has just read and checked against its own index. It never serves a path outside a folder, a file it hasn't indexed, or a file mid-edit.

## 5. Fetching a file

The content task, for an `Action::Fetch` or the winner of an `Action::Resolve`:

1. **Temporary file:** `.sync-node-tmp-<16 hex of BLAKE3(path ‖ 0 ‖ content)>` at the folder root (changed in step 3 from "beside the target": nothing is written below the root before the index task has checked the way, and it creates the parent directories when it places the file). The name is fixed for a path and version, so a restart finds it again. Temporary files are ignored by the index, as today. Limit: a file can't be placed across a mount point inside a folder (the rename fails and is logged).
2. **Resume:** if the file exists with the right length, each block is read and checked against the block list; matching blocks count as done. Otherwise it is created at the full length.
3. **Providers:** the nodes whose entry for the path has the wanted content, which means a complete copy of that version (as today). All are connected at once (changed in plan step 4 from one at a time), so an unreachable one costs nothing but its own connect timeout (10 s). During the fetch the content task watches the folder's doc: a node whose entry comes to hold the wanted content (it finished its own copy) joins the fetch. A dropped provider stays dropped for that fetch. Connections are kept per provider and reused across files.
4. **Requests:** up to 8 open at once to each provider, all drawing from one queue of missing blocks: a faster provider finishes sooner and is given more. Each received block is hashed and compared with the block list before it is written at its offset; a mismatch drops that provider for this file. Only the fetch loop writes, and only a block still wanted, so a stopped request never writes.
5. **Endgame:** once the queue is empty, a provider with room is also asked for a block whose request to another provider is overdue: open more than twice as long as requests in this fetch typically take (a moving average of delivered requests). The first answer is written and the other request is stopped. A block is asked of at most two providers at once. Providers of about the same speed are rarely asked twice for a block; a slow or stalled one can't hold up the end.
6. **Failures:** a timeout (30 s per block), a refusal or a broken connection drops that provider for this file; its open blocks go back to the queue for the others. `Busy` pauses that provider for a short wait, then it is asked again. When no provider is left, the temporary file stays, and that file waits 10 s before the next try (not every change to the folder, which would ask a node sending bad blocks again and again).
7. **Done:** once every block is verified, the file is synced to disk and handed to the index task: `Apply::Place { path, expect, meta, temp }`. The index task re-hashes the whole file against `content` (R10, and a guard against a block list that doesn't match its content hash), sets the modification time, writes the journal, renames, and writes the entry, exactly as today.

**Local sources first** (plan step 4): before any request, blocks whose hash already exists locally, in the old version of the same path or any other file of this node sharing a block, are copied from disk instead of requested. Each is checked against the block list before it is written. A file complete from local blocks alone is placed without asking any node.

## 6. Code layout

| Module | Role |
|---|---|
| `blocks.rs` (new) | ALPN, wire types, framing, block size rule, the server handler, the client (`fetch_file`). |
| `index.rs` | Computes block lists while hashing; shares a read handle on this node's entries; `place` takes a finished temporary file. The by-reference import and the blob store checks go. |
| `content.rs` | `fetch` calls `blocks::fetch_file` instead of the `Downloader`; the action logic is unchanged. |
| `folder.rs` | Keeps a registry the server uses: folder → root, entries handle, node list. |
| `gate.rs`, `peers.rs` | The new ALPN is gated and tracked. |
| `node.rs` | Registers the block protocol on the router. |

## 7. Failure cases

| Case | Behaviour |
|---|---|
| Provider goes offline mid-file | Remaining blocks move to the next provider; the temporary file keeps what arrived. |
| Receiver restarts mid-file | The temporary file is found again and checked block by block (§5.2). |
| File edited on the provider mid-transfer | `Changed`; the provider re-indexes; the receiver's next pass sees the new version and starts over for it (old temporary file cleaned up later). |
| Provider sends wrong bytes | Block hash mismatch; that provider is dropped for the file. |
| Block list doesn't match the content hash (bad entry) | Whole-file check fails at placement; the temporary file is deleted; logged. |
| Stale temporary files (version no longer wanted) | Removed by the content task once no planned fetch uses them and nothing has written to them for 24 h (plan step 5). |

## 8. Relay policy (sync-plan step 8)

Both ends know the connection's selected path at every request. The server refuses with `Policy` when the policy forbids file data over a relay; the client doesn't send requests to a provider whose selected path is a relay under direct-only. This covers file contents. The folder index (iroh-docs and its metadata blobs) still needs its own answer in step 8.

## 9. Open questions

* **Window and limits** (8 requests per provider, 64 served at once, 30 s per block) are first guesses; step 9 of the sync plan measures them on the Pi.
* **Several providers at once:** done in plan step 4, among nodes holding the complete file, with an endgame for overdue blocks (§5). Not done: the window is a number of requests, not bytes or a measured speed, so with 16 MiB blocks a slow provider holds up to 128 MiB.
* **Serving part of a file** (BitTorrent's model): a node fetching a file could serve the blocks it already has, so a new file reaches several nodes while its author uploads each block about once. Needs a way to say which blocks a node has, and serving from staging files, which §4 rules out today. Decide after the Pi measurements.
* **Bigger index entries over iroh-docs:** a 64 KiB metadata blob per large file. Expected to be fine; check in the Pi run.
