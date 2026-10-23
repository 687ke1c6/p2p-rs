---
status: proposed
---

# Our own index exchange for folder docs, not iroh-docs

The folder index moves between nodes over our own protocol, as in Syncthing's Block Exchange Protocol: each node sends a peer its whole index for a folder when they connect (`Index`), then each change (`IndexUpdate`), with a sequence number per node so a peer that comes back asks only for what it lacks. The entry (`FileMeta`) is in the message itself. iroh-docs would no longer carry folder indexes; the membership doc moves later, as its own step.

Proposed 2026-10-21, at BitTorrent plan step 7. **To be designed with the developer after the BitTorrent plan**, as its own plan: this ADR records the decision to do it and why, not the design.

## Why

Under load (`tests/starved.rs`: the runtime blocked 86 % of the time), the doc layer, not the transport, decides whether a file arrives. In BitTorrent plan step 7 every one of 17 stuck fetches lost its time there, and in none did the peer wire stop:

- **Membership listing:** in 15, b saw the folder listed 35–84 s into a 90 s budget.
- **Metadata blobs:** an entry's value is a separate iroh-blobs download. iroh-docs fetches it once, when the entry arrives, and under load that always failed; the entry then waited 16–49 s for our own retry (`META_FETCH_TRIES`, `fetch_missing`).
- **Sync not retried:** iroh-docs syncs on `start_sync`, a new gossip neighbour or a neighbour's report only, and never tries a failed sync again; entries written before a node joined never reached it (BitTorrent plan step 2, fixed by `Folders::retry_sync`). One sync hung for 65 s, holding the peer's only sync slot: `connect_and_sync` has no timeout.
- **Prefix keys** (found 2026-10-22): iroh-docs treats keys as a hierarchy by bytes (the Willow model). A new entry removes its author's older entries whose keys start with its key, so with the path as the key a write of `file.tmp` removed the entries of `file.tmp2` and `file.tmp10`, and their deletes never reached the other node. Fixed with a terminator byte (`index::doc_key`: the path and a NUL byte); old keys are refused, so nodes clear their config dir once. Our own exchange keys entries by full path, with no prefix rule.
- Each of these was fixed or worked around one at a time, outside iroh-docs. ADR 0002 left our own fetch for the same reason.

Our model is simpler than what iroh-docs solves: each node writes only its own entries, and the merged view is computed by us. There is no multi-writer merge to need a CRDT for.

## Considered Options

- **Keep iroh-docs and go on fixing around it** (what plan steps 2 and 7 did). Least work now; each fault stays a fix of its own, two of them (the hung sync, the one-shot blob fetch) need a patch of iroh-docs.
- **Patch iroh-docs** (timeouts, retries), as with iroh-gossip. Fixes the hung sync; keeps the two-step entry + blob and the sync triggers we don't control.
- **Our own index exchange** (this ADR). Most work; one protocol we control, entries inline, retry and timeouts ours. Open for the design: carry it on the peer-wire connection or its own ALPN; transitive convergence (an entry from any neighbour, not only its writer: entries signed, or trusted from members); persistence; the membership doc's move.

## Consequences

- A new plan after the BitTorrent plan; the sync plan's step 8 (relay policy) then covers the index exchange per message as it does the peer wire per request.
- iroh-docs and iroh-blobs leave the folder path; while the membership doc stays on iroh-docs, both stay as dependencies.
- The doc-layer workarounds (`Folders::retry_sync`, `META_FETCH_TRIES`, `fetch_missing_meta`) go with it.
