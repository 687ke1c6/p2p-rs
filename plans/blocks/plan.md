### Block Protocol Plan

* **Why:** [ADR 0001](../../docs/adr/0001-own-block-protocol-for-file-contents.md). **What:** [brd.md](brd.md). **How:** [spec.md](spec.md).
* **Objective:** file contents move over our own block protocol on iroh, served from disk and verified per block, replacing iroh-blobs for file data.
* **Relation to the sync plan:** [sync-plan.md](../sync-plan.md) is paused after step 7 while this plan runs. Its step 8 (relay policy) and step 9 (cross-network check) resume on top of this one. Steps 6b and 7 keep their logic (merge, conflicts, renames, journal); this plan swaps the transport under them.
* **Rules:** as in the sync plan: work in order, one commit per step, tick a box only when its "Done when" holds. `just check` passes at the end of every step. The developer runs every armv7 build.

#### Step 0: Decision & Docs

* [x] ADR 0001, BRD, spec and this plan written (2026-10-19).
* [x] `GLOSSARY.md` started, with the terms the docs use.
* [x] The developer reviews and accepts the docs (2026-10-19).
* **Done when:** the docs are accepted and the sync plan points here.

#### Step 1: Block Lists in the Index

* [ ] `FileMeta` gains `block_size` and `blocks` (spec §2). The index computes them in the same read as the whole-file hash; tombstones and empty files have none.
* [ ] Remove the by-reference import into the blob store and the store checks in `index_file`. Content transfer keeps working through the old path for now: the content task adds a file to the store just before it is fetched from (temporary bridge, removed in step 3).
* [ ] `folder show --entries` prints the block size and block count.
* [ ] Tests: the block size rule at each boundary (0, 1, 128 KiB, 256 MiB + 1, 32 GiB + 1); a scanned file's block list matches hashes computed independently; same content gives the same list; existing index and folder tests pass.
* [ ] armv7: no new dependencies; config dirs are cleared on both nodes (breaking index change).
* **Done when:** every entry carries a correct block list and nothing is imported by reference.

#### Step 2: Serving Blocks

* [ ] `blocks.rs`: ALPN `/sync-node/blocks/0`, wire types, framing with the 8 KiB header limit, the server handler (spec §4), registered on the router, gated and tracked.
* [ ] The index task shares a read handle on its entries; `Folders` keeps the registry the server uses (spec §6).
* [ ] A client function that fetches one block, for tests and for step 3.
* [ ] Tests: a member that keeps the folder gets a block that matches the block list; refused: a member that doesn't keep the folder (`NotKept`), an unknown path or old content (`NoSuchFile`), a file edited on disk (`Changed`, and the index catches up), an unsafe path; a non-member can't connect (gate); an oversized or garbage header closes the stream without harming the server.
* **Done when:** a node serves exactly the blocks its index describes, and refuses everything else.

#### Step 3: Fetching Files

* [ ] `blocks::fetch_file` (spec §5): fixed-name temporary file, resume by checking existing blocks, a window of 8 requests, per-block verification, provider fallback, `Busy` retry.
* [ ] The content task fetches with it; `Apply::Place` takes the finished temporary file; the index task re-hashes and places it as today.
* [ ] Remove the iroh-blobs content path: the `Downloader` for file contents, export, the step 1 bridge, the store fallback. iroh-blobs stays for index metadata only.
* [ ] Tests: all of `tests/content.rs` passes on the new transport, with the resume test rewritten to stop a real transfer (a partial temporary file left by a stopped node); a provider that goes away mid-file hands over to another; a provider sending wrong bytes is dropped and the file still arrives; R4: a file renamed on A is fetched intact by a new node C from A alone; R1: the blob store doesn't grow by the size of the files received.
* [ ] armv7 and CLI: the developer rebuilds, clears both config dirs and repeats the 6b/7 CLI checks between the Pi and the dev container.
* **Done when:** two and three nodes converge with file contents moving only over the block protocol, and a received file is stored once.

#### Step 4: Local Blocks & Several Providers

* [ ] Before requesting a block, look for it on disk: in the old version of the same path, or another file with the same content (spec §5, "local sources first"). An append to a large file then fetches only the new blocks.
* [ ] Optionally spread one file's blocks across several providers. Only if step 9's measurements (or a quick test against the Pi) show a single provider can't fill the link; otherwise record that it wasn't needed.
* [ ] Tests: appending to a 10 MiB file transfers about the appended size; editing the middle of a file transfers about one block.
* **Done when:** unchanged blocks are never fetched over the network.

#### Step 5: Cleanup & Hand-Back

* [ ] Remove stale temporary files at scan time: a `.sync-node-tmp-…` file that matches no version this node still wants.
* [ ] Remove `--with-content`: content transfer is always on (`Config::content` goes too).
* [ ] Update the sync plan: mark the parts of steps 6b and 7 this plan replaced (blob store notes, the deferred moved-files problem), and point step 8 at spec §8.
* [ ] armv7 and CLI: the developer rebuilds and runs a sync of a folder with some large files between the Pi and the dev container, checking disk use on the Pi.
* **Done when:** the sync plan can resume at step 8 with no iroh-blobs file-data code left.
