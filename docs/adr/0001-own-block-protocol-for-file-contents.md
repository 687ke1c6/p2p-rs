---
status: accepted
---

# Our own block protocol for file contents, not iroh-blobs

File contents move between nodes over our own block protocol on iroh, modelled on Syncthing's Block Exchange Protocol: each index entry carries a block list, a node asks a peer for one block of one file, and the peer reads it straight from the file on disk. iroh-blobs is a content-addressed store for immutable data that wants to own the bytes, while a synced folder is mutable and the disk is its only source of truth; using it for file contents cost us a store copy of every received file, stale references once a file moved (renames, conflict copies), and no way to apply the relay policy per request. iroh stays as the transport, iroh-docs as the index and membership store, iroh-gossip for presence; iroh-blobs stays only because iroh-docs uses it for index metadata.

Decided 2026-10-19, after sync-plan step 7. Its transport is replaced by ADR 0002, which also renames the terms: a block here is a piece, a block list is the piece hashes. See `plans/blocks/`.

## Considered Options

- **Keep iroh-blobs and patch it** (read each referenced path, add GC to drop received copies). Smaller now, but every fix works against the store's model, the duplicate copies need a GC subsystem that must also protect iroh-docs' blobs, and the relay policy still can't be applied per request.
- **Copy files into the blob store.** No patch, but twice the disk on every node, and old versions pile up until GC.
- **Leave iroh** (for example, run Syncthing). The transport is the part that has worked best; the problems were all in the content path.

## Consequences

- The index format changes (block lists in each entry), so existing config dirs must be cleared; nothing is migrated.
- Verified, resumable transfer becomes our code: per-block BLAKE3 instead of iroh-blobs' bao tree.
- Sync-plan step 8 (relay policy) gates our own ALPN per request, instead of iroh-blobs' provider events.
