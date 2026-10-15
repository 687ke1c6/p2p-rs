### Phase 2: File Sync Over iroh

* **Builds on:** [prototype-plan.md](prototype-plan.md) (concluded). The prototype proved that direct connections work between our networks; this phase hands the transport to iroh and builds the sync on top.
* **Objective:** A local-first sync tool in Rust. Nodes in a group keep one or more folders in sync over iroh connections, and each node can see who else is in the group and how it's connected to them (direct or relayed).
* **Focus:** Distributed state and synchronisation. Transport, NAT traversal, encryption and peer identity come from iroh.

### Constraints

* **Relay policy, per peer or per folder** (decided in the prototype's **Concluded** section):
  * **Direct only:** no application data is sent until the connection's selected path is direct (`Path::is_ip()` and `is_selected()`), and the connection is closed if it falls back to a relay.
  * **Relay allowed:** data may flow through a relay. Sync is a background task, so slow, bandwidth-limited relays are acceptable.
  * Either way the relay only sees end-to-end encrypted traffic. A self-hosted `iroh-relay` (`RelayMode::Custom`) keeps even that off third-party infrastructure.
* **Trust is pinned, not discovered.** A node is its iroh `EndpointId` (a public key). Nodes accept connections only from IDs the user has paired with. No server, relay or discovery service can add a peer to a group.
* **Local first.** Each node keeps working on its own copy offline and catches up when peers reappear.

### Library Choice

* **`iroh` 1.x** for endpoints, connections and path information. Pin exact versions: the protocol crates below are still pre-1.0.
* **Candidates from n0, to be confirmed in step 1:**
  * **`iroh-blobs`:** content-addressed transfer (BLAKE3), verified while streaming and resumable. Covers moving file contents.
  * **`iroh-gossip`:** topic-based gossip. Covers group membership and connection-type sharing (step 4).
  * **`iroh-docs`:** a multi-writer key-value store that syncs between peers, built on blobs and gossip. It might hold the file index, but it may not fit our conflict and delete rules. Weigh it against keeping our own index.
* The client must still cross-compile for armv7 (`just client-armv7`) to run on the Raspberry Pi.

### Open Design Questions

Settle each before or during the step that needs it, and record the decision in this file.

* **Index model:** what each node records per file (path, size, BLAKE3 hash, modification time, version) and how versions are compared between nodes (e.g. a version vector per file).
* **Conflicts:** what happens when two nodes change the same file between syncs. The likely default is to keep both and rename the loser, Syncthing-style.
* **Deletes:** tombstones, so a file deleted on one node isn't restored from another, and when tombstones can be cleaned up.
* **Renames:** detect them (same hash, new path) rather than treating them as a delete plus a new file.
* **Pairing UX:** how two nodes exchange IDs (printed ID, QR code, short-lived invite) and how a third node joins an existing group.
* **Ignore rules:** format and whether they sync between nodes.

---

### Ordered Implementation Steps

Same rules as the prototype: work in order, one commit per step, tick a box only when its "Done when" holds.

#### Step 0: iroh Direct-Only Spike

* [ ] Two iroh 1.x endpoints with relays on (n0's defaults are fine here). Watch `Connection::paths()` and log each path and every change to the selected path.
* [ ] Implement the direct-only policy: open no streams until a direct path is selected, give up after a timeout, and close the connection if the selected path becomes a relay.
* [ ] Run it between the dev container and the Raspberry Pi on their separate networks. Send a few megabytes once direct, and check the relay path's `PathStats` to confirm no application bytes went over the relay.
* [ ] Block outbound UDP on one side to force relay-only, and confirm that direct-only refuses while relay-allowed transfers.
* **Done when:** both policies behave as specified across the two networks, and the logs show what the relay carried.

#### Step 1: Protocol Crate Check

* [ ] For `iroh-blobs`, `iroh-gossip` and `iroh-docs`, find the newest version built on iroh 1.x, its maturity and recent breaking changes.
* [ ] Confirm the chosen crates build for armv7.
* [ ] Decide which crates to use, and in particular whether `iroh-docs` holds the index or we keep our own. Record the decision and reasons here.
* **Done when:** dependencies are pinned and the decision is written down.

#### Step 2: Workspace Reset

* [ ] Once the prototype commit is tagged, set up the workspace for the sync node, e.g. a `node` crate for the library and a CLI binary. Decide whether the prototype crates are removed (the tag keeps them) or moved aside.
* [ ] Carry over what still applies: the dev container, `just check`, logging, and the armv7 build.
* **Done when:** `just check` passes on the new layout and the armv7 build runs on the Pi.

#### Step 3: Identity, Pairing & Relay Policy

* [ ] Persist each node's secret key. Add a command that prints the node ID. A missing or unreadable key file is a clear error, never a silent new identity.
* [ ] Add pairing (adding a trusted node ID) and refuse connections from unpaired IDs at accept time.
* [ ] Make the relay policy configurable per peer, using the step 0 logic. Configure which relays to use (n0 defaults or self-hosted).
* [ ] Tests: paired nodes connect; an unpaired node is refused in both directions; direct-only refuses a relay-only connection.
* **Done when:** two paired nodes connect under each policy, and an unpaired node can't.

#### Step 4: Group Membership & Connection Types

* [ ] Nodes in a group share, via gossip, which members are online and each pair's connection type (direct, relayed, none). This replaces the per-connection stats the rendezvous server couldn't hold.
* [ ] Add a `status` command showing the group as this node sees it.
* [ ] Tests: three nodes agree on membership; a node going offline is noticed within a set time.
* **Done when:** each node in a three-node group shows the same members and correct connection types.

#### Step 5: Local File Index

* [ ] Scan a folder into an index (per the index-model decision), hashing with BLAKE3 so hashes match `iroh-blobs`. Persist it across restarts.
* [ ] Watch the folder for changes, with debouncing so editors and bulk copies don't flood the index. Apply ignore rules.
* [ ] Tests: scan, change, rename, delete and restart all leave the index correct; a burst of changes yields one update per file.
* **Done when:** the index tracks a working folder accurately through edits and restarts.

#### Step 6: Index Sync & Content Transfer

* [ ] Nodes exchange indexes and fetch content they're missing. Write each file to a temporary file and move it into place only after its hash checks out.
* [ ] An interrupted transfer resumes rather than starting over.
* [ ] Tests: two nodes converge from different starting folders; a transfer killed midway resumes; an unsafe path from a peer is refused.
* **Done when:** two nodes converge on the same folder contents, verified by hash.

#### Step 7: Conflicts, Deletes & Renames

* [ ] Implement the conflict, tombstone and rename decisions.
* [ ] Tests for each: concurrent edits keep both versions; a delete stays deleted after an offline node returns; a rename moves the file without re-sending its content.
* **Done when:** a three-node group converges correctly through concurrent edits, deletes and renames, including one node offline during the changes.

#### Step 8: Cross-Network Verification

* [ ] Sync a realistic folder (many files, some large) between the dev container and the Pi on their separate networks, under both relay policies.
* [ ] Log throughput, how long the initial sync takes, and path changes during the run.
* **Done when:** the folder converges across the two networks under both policies, and direct-only never sends application data over a relay.
