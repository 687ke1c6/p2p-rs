---
status: proposed
---

# Our own gossip protocol for documents, membership and presence

Folder documents, membership records and presence move between nodes over our own gossip protocol, on its own ALPN. It replaces iroh-docs and iroh-gossip, and with them the patched copy of iroh-gossip in `patches/`.

Each node writes only its own entries and numbers them (1, 2, 3, ...). The entry itself is in the message, signed by the node that wrote it, so any neighbour can pass it on. A node connects to at most the active view of members (its neighbours); entries reach the other members through them.

Proposed 2026-10-21, at BitTorrent plan step 7; scope widened 2026-10-27 with the developer (iroh-gossip, membership, presence; own ALPN; relay through neighbours). **To be designed with the developer after the BitTorrent plan**, as its own plan: this ADR records the decision and the shape, not the wire format.

## How it works

Three parts, as in most gossip protocols. Anti-entropy alone makes the protocol correct; push only makes it fast.

1. **Push.** A node that writes an entry sends it to its neighbours at once. A neighbour that hasn't got it yet keeps it and sends it on to its own neighbours. The writer and sequence number identify the entry, so a node drops one it already has and loops end.
2. **Anti-entropy.** At intervals, and on each new connection, two neighbours swap a summary: for each writer, the highest sequence number held without a gap. Each then sends the other the entries the other lacks, from any writer. This repairs every lost push, and is how a node that was offline catches up.
3. **Neighbours.** Each node keeps live connections to at most the active view of members and knows the addresses of more (the passive view). When a neighbour goes away, the node connects to a member from its passive view. In a group no larger than the active view plus one, every node is a neighbour of every other (a full mesh) and nothing needs to be relayed.

**Relay example.** A and C can't connect (NAT), and both are neighbours of B. A writes an entry and pushes it to B. B checks A's signature, keeps the entry and pushes it to C. If that push is lost, C gets the entry at its next anti-entropy with B. C trusts it because A signed it, not because B sent it.

Each writer's sequence numbers have no gaps, so a summary is one number per writer: no Merkle tree, and no CRDT, since no two nodes write the same entry. The merged view is computed by us from all writers' entries, as now.

## Why

- **iroh-gossip needed two local patches** (`patches/iroh-gossip`, see `Cargo.toml`): it never redialled a peer whose first dial failed, it leaked a connection per disconnect, it made duplicate connections, and one topic dropping a peer dropped it for every topic. All are in its neighbour management, and iroh-docs uses it for every live sync.
- **iroh-docs logs a warning on every sync** (sync plan, "Known issue, upstream": `Failed to close … closed stream`) and then skips part of its own bookkeeping.
- **The doc layer, not the transport, decides whether a file arrives under load.** In `tests/starved.rs` (the runtime blocked 86 % of the time), every one of 17 stuck fetches in BitTorrent plan step 7 lost its time there, and in none did the peer wire stop:
  - **Membership listing:** in 15, b saw the folder listed 35–84 s into a 90 s budget.
  - **Metadata blobs:** an entry's value is a separate iroh-blobs download. iroh-docs fetches it once, when the entry arrives, and under load that always failed; the entry then waited 16–49 s for our own retry (`META_FETCH_TRIES`, `fetch_missing`).
  - **Sync not retried:** iroh-docs syncs on `start_sync`, a new gossip neighbour or a neighbour's report only, and never tries a failed sync again; entries written before a node joined never reached it (BitTorrent plan step 2, fixed by `Folders::retry_sync`). One sync hung for 65 s, holding the peer's only sync slot: `connect_and_sync` has no timeout.
  - **Prefix keys** (found 2026-10-22): iroh-docs treats keys as a hierarchy by bytes (the Willow model). A new entry removes its author's older entries whose keys start with its key, so with the path as the key a write of `file.tmp` removed the entries of `file.tmp2` and `file.tmp10`, and their deletes never reached the other node. Fixed with a terminator byte (`index::doc_key`). Our own protocol keys entries by full path, with no prefix rule.
- Each of these was fixed or worked around one at a time, outside the libraries. ADR 0002 left our own fetch for the same reason.
- Our model is simpler than what iroh-docs solves: each node writes only its own entries, and membership records, removals and presence messages are signed already.
- The developer wants to learn how a gossip protocol spreads data by building one.

## Considered Options

- **Keep iroh-docs and iroh-gossip, and go on fixing around them** (what plan steps 2 and 7 did). Least work now; each fault stays a fix of its own, and the patched copy of iroh-gossip stays until upstream releases fix it.
- **Patch iroh-docs too** (timeouts, retries), as with iroh-gossip. Fixes the hung sync; keeps the two-step entry + blob, the sync triggers we don't control and the warning.
- **Direct exchange only, as in Syncthing's Block Exchange Protocol** (the first version of this ADR): each node sends its own entries to each node it connects to, and no node passes on another's. Simplest, and enough for a full mesh; but two members that can't connect, or that aren't neighbours in a group larger than the active view, never get each other's entries.
- **Our own gossip protocol** (this ADR). Most work; one protocol we control, entries inline, retries and timeouts ours, and relay through neighbours.
- **Carry it on the peer-wire connection.** One connection per pair; but it mixes the documents' traffic with piece traffic, and the peer wire exists only within a swarm. Rejected: its own ALPN.

## Open for the design

- **Active view size.** The sync plan sets 8 live connections and 125 known members; the developer has since said 25. To be settled in the plan.
- **Presence**: it is sent every 5 s and replaced, not kept. Push only, or also in anti-entropy (latest message per node)?
- **Compaction**: a writer's later entry for a path makes its earlier one useless, but the sequence numbers must stay without gaps. Send only each path's latest entry, with the summary still counting up to the latest number?
- **Persistence** of each writer's entries and of the summary.
- **Tests that make relay happen**: a group larger than the active view, or a pair that the gate stops from connecting.
- **Order of the move**: folder documents first, then membership, then presence; or all at once.

## Consequences

- A new plan after the BitTorrent plan; the sync plan's step 8 (relay policy) then covers this protocol per message as it does the peer wire per request.
- iroh-docs, iroh-gossip and `patches/iroh-gossip` leave the project once all three kinds of data have moved. iroh-blobs stays only as far as the content code uses it (its `Hash` type, `FsStore`).
- The doc-layer workarounds (`Folders::retry_sync`, `META_FETCH_TRIES`, `fetch_missing_meta`, `index::doc_key`'s terminator) go with iroh-docs.
- We take on neighbour management, the part of iroh-gossip where its bugs were. It needs its own tests for redial after a failed dial, connection cleanup and duplicate connections; `tests/reconnect.rs` is a start.
- The membership doc becomes the membership records and removals, carried by this protocol. ADR 0006's "Its folder entries stay in the folder docs ... until ADR 0003" then becomes: until the removal reaches every member.
- Moving to the protocol is a break between versions: nodes on iroh-docs and nodes on this protocol can't sync with each other.
