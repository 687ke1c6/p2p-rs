### Phase 2: File Sync Over iroh

* **Builds on:** [prototype-plan.md](archive/prototype-plan.md) (concluded). The prototype proved that direct connections work between our networks; this phase hands the transport to iroh and builds the sync on top.
* **Objective:** A local-first sync tool in Rust. Nodes in a group keep one or more folders in sync over iroh connections, and each node can see who else is in the group and how it's connected to them (direct or relayed).
* **Focus:** Distributed state and synchronisation. Transport, NAT traversal, encryption and peer identity come from iroh.

### Constraints

* **Relays are always available for connectivity.** Reaching a peer, hole-punch coordination, gossip (presence, connection types), the join protocol and the membership doc may all go through a relay, whatever the relay policy says. Turning relays off is not an option: it would cost relay-assisted hole punching and leave relay-only pairs unable to see each other at all.
* **Relay policy, per peer or per folder, covers file data only:** folder index sync and file contents (decided in the prototype's **Concluded** section; enforced by a hook added in step 8, which may narrow it to per node). Until step 8, nothing is gated and file data may also cross a relay.
  * **Direct only:** file data is sent only while the selected path to that peer is direct (`Path::is_ip()` and `is_selected()`). If the path falls back to a relay, file sync with that peer pauses until it is direct again; the connection itself, and everything else on the relay, carries on.
  * **Relay allowed:** file data may flow through a relay too. Sync is a background task, so slow, bandwidth-limited relays are acceptable.
  * Either way the relay only sees end-to-end encrypted traffic. A self-hosted `iroh-relay` (`RelayMode::Custom`) keeps even that off third-party infrastructure.
* **Trust is pinned, not discovered.** A node is its iroh `EndpointId` (a public key). Nodes accept connections only from IDs with a valid signed membership record (see **Membership & Topology**). The one exception is the join protocol, which an unknown ID may use only to redeem an invite. No server, relay or discovery service can add a peer to a group: only an existing member can, by signing an invite.
* **Local first.** Each node keeps working on its own copy offline and catches up when peers reappear.

### Library Choice

* **`iroh` 1.x**  for endpoints, connections and path information. Pin exact versions: the protocol crates below are still pre-1.0.
* **From n0, pinned in step 1** (see its findings):
  * **`iroh-blobs`:** content-addressed transfer (BLAKE3), verified while streaming and resumable. Covers moving file contents.
  * **`iroh-gossip`:** topic-based gossip. Covers group membership and connection-type sharing (step 4).
  * **`iroh-docs`:** a multi-writer key-value store that syncs between peers, built on blobs and gossip. **Decided: it holds the file index and the membership records**, so we don't keep our own. Step 1 still confirms the version, its maturity and the armv7 build. Our conflict rules need causal information it doesn't store itself (see **Index model**).
* The node must still cross-compile for armv7 (`just armv7`) to run on the Raspberry Pi.

### Open Design Questions

Settle each before or during the step that needs it, and record the decision in this file.

* **Index model:** decided 2026-10-18, see **Folders & the File Index** below.
* **Conflicts:** decided 2026-10-18, see **Folders & the File Index**. Step 7 implements the renames.
* **Deletes:** tombstones, so a file deleted on one node isn't restored from another (decided: a tombstone is an index entry with no content, see below). Still open: when tombstones can be cleaned up.
* **Renames:** detect them (same hash, new path) rather than treating them as a delete plus a new file.
* **Removing a member:** who may revoke a membership record, and what happens when an add and a remove race. The likely default is that any member may remove any other, and a removal always wins.
* **Ignore rules:** decided 2026-10-18, see **Folders & the File Index** below.

### Design Decisions

#### Membership & Topology

* **Connection limits, borrowed from Bitcoin Core and HyParView:** each node holds at most **8 live connections** (its active view) and knows the addresses of up to **125 other members** (its passive view). Map these onto `iroh-gossip`'s HyParView settings (`active_view_capacity = 8`, `passive_view_capacity = 125`; its defaults are 5 and 30).
  * In a group of 9 or fewer nodes, every node connects to every other, so step 4 can report a real connection type for every pair.
  * In a larger group, a pair may have no connection. `status` then reports "not neighbours", which is different from "can't connect".
  * In Bitcoin, 8 is the number of outbound connections and 125 is the cap on all connections. Here we use 125 as the size of the known-address list.
* **Convergence is transitive.** A node can fetch index entries and content from any neighbour that has them, so two nodes that never connect still end up with the same folder. This also covers pairs that only have a relay path under the direct-only policy: they still gossip over the relay, but get their file data through a neighbour with a direct path.
* **Two ways to share state.** Gossip pushes live changes ("I'm online", "path X changed"), but anyone offline misses those messages. `iroh-docs` set reconciliation runs whenever two nodes connect, and is how a node that was offline catches up.

#### Folders & the File Index

Decided 2026-10-18.

* **Blobs and docs.** `iroh-blobs` stores and moves bytes by BLAKE3 hash and knows nothing of names. An `iroh-docs` entry maps a key to the hash of a blob, and syncing a doc reconciles entries and then fetches the blobs they point at. The membership doc already works this way: each `members/<id>` entry points at a small blob holding the signed record.
* **One doc per synced folder**, separate from the membership doc. Folder data may have to stay off relays (step 8) while membership never does, and that can only be enforced per namespace. It also keeps the membership doc small, and lets nodes share some folders and not others. The membership doc lists the group's folders as `folders/<doc id>` → the folder's name.
* **A folder entry points at a small metadata blob, not at the file's bytes.** The key is the path relative to the folder root, `/`-separated. The metadata (`FileMeta`) is the content hash (none for a tombstone), size, modification time (nanoseconds), the node that wrote it, and a **version vector**. Docs therefore syncs only the index; our code decides which content to fetch, when and over which path (steps 6 and 8). A delete is a tombstone: an entry with no content hash.
* **Each node writes only its own entries**, describing its own disk. The doc is the union of every node's entries. The **merged view**, what the folder should look like, is computed from them on each node and never stored: every node holding the same entries computes the same view.
* **Version vectors, not a `replaces` hash** (changed 2026-10-18, before any folder data was synced). iroh-docs keeps only the latest entry per author and key, so a node that edits twice between syncs leaves no trace of the middle version, and a one-step `replaces` hash can't show that its edit came after the version another node holds. A version vector maps node → counter. A local change takes the vector of the version it replaced, merged with every other entry for that path with the same content (the same file, wherever it came from), and adds one to this node's counter. One version came after another if its vector dominates.
* **Merging a path:** drop every entry another entry came after; of the rest, entries with the same content agree. If one content remains, that's the result (a tombstone means deleted). If live and deleted versions remain, **the edit wins** and the file stays. If several live versions remain, it's a **conflict: both are kept**. The one with the newest modification time keeps the name (ties: the smaller node ID); each other version is kept beside it as `<stem>.sync-conflict-<YYYYMMDD-HHMMSS>-<node>.<ext>`, using that version's modification time (UTC) and its node's name. Concurrent deletes are just deleted.
* **One doc per folder name.** `folder add <name> <path>` joins the group's folder of that name if there is one, rather than creating a second. The membership entry `folders/<doc id>` holds the name and the folder doc's secret, which gives members write access (they can already write to the membership doc). Nodes that created a folder of the same name while apart all settle on the one whose entry is oldest (ties: the smaller doc ID); a node on another one switches its local folder to it and re-indexes. Each node that keeps a folder lists itself as `folder-nodes/<doc id>/<node id>`, and syncs the folder doc with the others listed.
* **Content** is fetched by hash from any member that has it, and serves transitive convergence. How file bytes get into the blob store (copy, or import by reference so they aren't stored twice) is decided in step 6; step 5 only hashes them.
* **What is indexed:** regular files with UTF-8 paths. Directories are implied by their files, so an empty directory isn't synced. Symlinks, special files and non-UTF-8 names are skipped with a warning. File permissions aren't synced yet.
* **Folder marker:** a `.syncfolder` file at the folder root, created by `folder add`, always ignored. A folder whose marker is missing is paused, not scanned: its directory moved, was deleted or isn't mounted, and scanning it would tombstone every file on every node. The node resumes it once the marker is back.
* **Ignore rules:** a `.syncignore` file at the folder root, in `.gitignore` syntax (the `ignore` crate's matcher). It is an ordinary file, so it syncs and every node ignores the same paths. The node's own temporary files are always ignored.

#### Signed Membership

* **The founder** creates the group: the `iroh-docs` namespace for the membership document and a self-signed record for itself.
* **A membership record** is `{ group, member: EndpointId, added_by: EndpointId, added_at }`, signed by `added_by`'s endpoint key. It is stored in the membership document under `members/<EndpointId>`. A record is valid if its signer has a valid record, following the chain back to the founder.
* **A node carries its own chain.** It presents its record, and the records of its signers up to the founder, over the members ALPN when it connects, so a member that hasn't yet received the record through sync can still check it. Without this, a new node would have to wait for its record to spread.
* **An invite** is a ticket (printed or as a QR code) holding the inviter's address, the group ID, a random one-time secret and an expiry time. The inviter keeps only a hash of the secret. Anyone holding the ticket can redeem it once, so treat it like a password until it is used or expires.
* **The join protocol** is a separate ALPN. It is the only thing an unknown ID may connect to, and it accepts exactly one message: an invite redemption, or a request for the record a member wrote for the joiner's ID. Rate-limit it. The node proving it owns its `EndpointId` comes for free from iroh's TLS handshake.
* **Every node has the same role.** There is no server. The membership doc is the group's allow-list: every node holds a copy and gates every connection against it, in both directions.
* **Any member may add a node, transitively** (decided 2026-10-18). Being on the list is what lets you add to it. There are two ways, and both end in the same signed record:
  * **By node ID:** the new node prints its ID (`sync-node id`) and sends it to a member, who runs `member add <id>`. Nothing secret changes hands: the two nodes just swap IDs. The new node then runs `join <member id>` and asks that member for its chain over the join ALPN; it learns the group from the reply. The member looks up `members/<remote id>`, checks the chain and replies with the usual `Welcome`. Any member whose copy of the doc holds the record can answer, so the member who added it needn't be online.
  * **By invite:** as above, for when the new node's ID isn't known in advance (a QR code in person). The ticket is a bearer secret until redeemed.
* **Names** (decided 2026-10-18). A member may have a display name, like proxy-rs's labels. Names live in the membership doc under `names/<id>`, separate from the signed record, so any member can set or change one without re-signing anything, and every node sees the same name. The newest entry wins. A name is cosmetic and grants nothing; a rename is logged on every node (`pc-old is now pc`) so nobody can quietly take another member's name. Names are unique within the group: setting a name another member already has is refused.

```mermaid
sequenceDiagram
    autonumber
    actor U as User
    participant A as Member A (inviter)
    participant N as New node N
    participant B as Member B
    participant C as Member C

    U->>A: invite create
    A->>A: generate secret, store hash and expiry
    A-->>U: ticket (A's address, group ID, secret, expiry)
    U->>N: join <ticket>

    N->>A: connect on join ALPN (TLS proves N's EndpointId)
    A->>A: accept: unknown ID, join ALPN only
    N->>A: Redeem { group, secret }
    A->>A: check secret hash, unused, unexpired; mark used
    A->>A: sign record { member: N, added_by: A }
    A->>A: write members/N to membership doc
    A-->>N: Welcome { N's chain (N → A → founder), membership-doc ticket }
    A--)B: membership doc syncs members/N (gossip)
    A--)C: membership doc syncs members/N (gossip)

    N->>N: store group and own chain
    N->>B: connect on members ALPN, present own chain
    B->>B: check signature chain N → A → founder
    B-->>N: B's chain (N checks it too)
    N->>C: connect on members ALPN, present own chain
    C->>C: check signature chain N → A → founder
    C-->>N: C's chain (N checks it too)
    Note over N,C: N joins the gossip topic with up to 8 neighbours (A, B, C here)
    N->>B: iroh-docs reconciliation (membership doc, then folder index)
    B-->>N: missing entries
    N->>B: fetch missing blobs (iroh-blobs)
```

---

### Ordered Implementation Steps

Same rules as the prototype: work in order, one commit per step, tick a box only when its "Done when" holds.

#### Step 0: Workspace Reset

* [x] The prototype is tagged `prototype-final`. Its crates (`client`, `server`, `protocol`), the rendezvous `Dockerfile` and its recipes are removed; the tag keeps them. Its plans are in [archive/](archive/).
* [x] One `crates/node` crate (package `sync-node`) with a library and a binary, so tests drive the library. Split out a CLI crate if it grows.
* [x] Carried over: the dev container, `just check`, logging to stdout, and the armv7 build (`just armv7`, `Dockerfile.armv7`).
* [x] Run the armv7 binary on the Pi.
* **Done when:** `just check` passes on the new layout and the armv7 build runs on the Pi.

#### Step 1: Protocol Crate Check

* [x] For `iroh`, `iroh-blobs`, `iroh-gossip` and `iroh-docs`, find the newest compatible versions, their maturity and recent breaking changes. Pin them exactly.
* [x] Confirm the chosen crates build for armv7. They compile with `just armv7`. The binary doesn't call them yet, so step 2's armv7 build is the first one that links them.
* [x] Confirm `iroh-docs` can hold the index and membership records as decided, and that `iroh-gossip` exposes the HyParView view sizes.
* [x] Find how to refuse a connection by `EndpointId` at accept time, including connections the protocol crates accept themselves (needed in step 3).
* [x] Find where a direct-only gate could go when `iroh-docs` and `iroh-gossip` open their own connections (needed in step 8).
* **Done when:** dependencies are pinned and the answers are written down here.

**Findings (2026-10-16):**

* **Versions, pinned in the workspace `Cargo.toml`:** `iroh =1.3.0`, `iroh-docs =0.101.0`, `iroh-gossip =0.101.0`, `iroh-blobs =0.103.1`. This is the newest set that fits together: `iroh-docs` needs `iroh ^1`, `iroh-blobs ^0.103` and `iroh-gossip ^0.101`. They resolve to a single `iroh`.
  * `iroh` releases about every two to three weeks; `iroh-docs` and `iroh-gossip` haven't released since 2026-06-15, at iroh 1.0. All four need Rust 1.91.
  * `iroh-blobs` re-released 0.100 to 0.103 as `.1` patches on 2026-10-06 and yanked each earlier `.0`. One of the fixes makes `EventMask` respect its per-request modes. Don't go below 0.103.1.
* **Features:** the RPC features are off (`default-features = false`; `fs-store` on for docs and blobs, `net` on for gossip). We run everything in one process. TLS is `ring`, iroh's default, and `aws-lc-rs` isn't in the tree, so the armv7 cross-compile needs no extra toolchain.
* **iroh-docs:** `Docs::persistent(dir).spawn(endpoint, blobs, gossip)`, then `create`, `import(DocTicket)`, `set_bytes`, `get_many`/`get_exact`, `subscribe` and `start_sync`. It uses the `Gossip` instance we pass it, so our view sizes apply to its topics. It replicates entries but knows nothing about our membership records: checking a record's signature chain is our code, run on each entry it delivers. Every member holds the namespace write capability, which they get in the Welcome message.
* **iroh-gossip:** `Gossip::builder().membership_config(HyparviewConfig { active_view_capacity, passive_view_capacity, .. })`. The defaults are 5 and 30, as expected.
* **Accept-time gating: `EndpointHooks::after_handshake`** (installed with `Builder::hooks`). It runs for every connection in both directions, including those opened or accepted by `iroh-docs`, `iroh-gossip` and `iroh-blobs`. It sees the remote `EndpointId` and the ALPN, and can reject with a close code. `Router::incoming_filter` acts earlier, before TLS, but doesn't know the ID yet, so it's only good for rate-limiting.
  * ALPNs to gate: `/iroh-sync/1` (docs), `/iroh-gossip/1`, `/iroh-bytes/4` (blobs) and our own join ALPN, the only one open to unknown IDs.
* **Direct-only fits in the same hook, but only for the ALPNs that carry file data** (see **Constraints**: gossip, join and membership stay open over relays). The hook is async, and `Endpoint::connect` and the router's accept only hand the `Connection` on once every hook has finished. A direct-only hook can wait on `Connection::paths_stream()` for a selected IP path, rejecting after a timeout, before any protocol crate gets the connection. A task holding `Connection::weak_handle()` then closes the connection if the selected path moves to a relay. The hook knows the remote ID, so the policy can be per peer after all. Two catches for step 8:
  * The hook only stops our side. A peer could send over the relay before our hook decides, so both peers need to run the hook.
  * The protocol crates have their own dial timeouts, which must be longer than the hook's wait.
  * An ALPN doesn't line up with "file data". `/iroh-sync/1` reconciles the membership doc as well as the folder index, and `iroh-docs` fetches every entry's content, membership records included, over `/iroh-bytes/4`. Gating those two ALPNs per connection would also block membership over a relay. Step 8 has to gate finer: per namespace for docs sync and per request for blobs (`iroh-blobs` provider events, whose `EventMask` modes were fixed in 0.103.1), or move membership off those ALPNs.

#### Step 2: Identity & a Running Node

* [x] Persist each node's secret key. `run` and `id` create it if the file doesn't exist and log that they did. A file that exists but can't be read or is the wrong size is an error, never replaced.
* [x] Add `sync-node id`, which prints the node ID.
* [x] Start an endpoint (n0's default relays for now) with the docs, gossip and blobs protocols registered, and shut it down cleanly on Ctrl-C.
* [x] Tests: the key survives a restart; a corrupt key file is an error; two in-process nodes can connect.
* [x] armv7: the first build that links iroh. The developer builds it and runs `id` and `run` on the Pi.
* **Done when:** a node starts with a stable ID and accepts connections.

**Notes:**

* **CLI:** `sync-node [--config-dir DIR] id | run`. The config directory defaults to `~/.sync-node` and can be set with `SYNC_NODE_CONFIG_DIR`, for example to run two nodes on one machine. It holds `key` (32 raw bytes, mode 600), `blobs/` and `docs/`. Creating the key also creates the directory with mode 700, because the docs store holds namespace and author secrets. A wrong `--config-dir` gives the node a new identity rather than an error; the "created a new node identity" log line is the only sign.
* **Code:** `key.rs` loads and creates the key; `node.rs` holds `Node`, which wires the endpoint, blobs store, gossip (8 active, 125 passive) and docs onto one `Router`. `Network::Local` (no relays or address lookup) is for tests on one machine.
* **Shutdown:** `Router::shutdown` shuts each protocol down, and the blobs protocol closes the blob store itself. Closing the store a second time fails.
* **Logging:** switched from `env_logger` to `tracing-subscriber`, since iroh logs through `tracing`. Through `env_logger`, its spans showed up as log lines, including fake `ERROR` lines that were just span names. The default filter is `warn,sync_node=info`, and `RUST_LOG` overrides it. Output goes to stdout, coloured only on a terminal.
* **Tests:** `tests/key.rs` covers create, reload, refusal to replace, missing and corrupt keys, and file modes. `tests/node.rs` covers a restart keeping the ID, a doc and its content, and two nodes syncing a doc over local addresses.

#### Step 3: Founding, Invites & the Membership Doc

* [x] `create`, `invite create` and `join <ticket>` per **Signed Membership** and its sequence diagram.
* [x] At accept time, refuse any ID without a valid record, except on the join ALPN.
* [x] Tests: an invited node joins and appears in every member's copy of the membership doc; a reused, expired or forged invite is refused; a record whose chain doesn't reach the founder is refused; a node with no record is refused in both directions.
* [x] armv7: new dependencies (`serde`, `postcard`, `data-encoding`, `blake3`, `rand`), all pure Rust and already in the tree through iroh. The developer rebuilds and tries `create`, `invite create`, `join` and `run` between the Pi and the dev container.
* **Done when:** a third node joins a two-node group by invite and all three hold the same membership doc, and a node without a record can't connect.

**Notes:**

* **Group:** the group ID is the membership doc's `NamespaceId`. The founder is the self-signed root of a chain. A node's `group` file (in the config directory, mode 600) holds the group ID, the founder and its own chain; a node is in at most one group.
* **Records:** `record.rs`. Postcard-encoded, signed by `added_by`'s endpoint key over a domain tag plus the record. `validate_chain` follows signers from a member to a self-signed root and requires that root to be the founder; it refuses a missing link, a bad signature, a record for another group and a loop.
* **The gate:** `gate.rs`, an `after_handshake` hook. The join and members ALPNs are open; everything else (docs, gossip, blobs, and any ALPN we don't know) needs the remote to be admitted. Admitted are the members of our own chain, a node that just redeemed an invite with us, and any node that presents a valid chain over the members ALPN (`/sync-node/members/0`). The side that dials presents first: every outgoing connection opens a members connection and swaps chains, even to an ID we already know, because the remote may have restarted and forgotten us; an incoming one from an unknown ID waits up to 5 s (`ADMIT_TIMEOUT`) for that ID to present. (The first version skipped presenting to known IDs, so after the inviter restarted, a joiner dialling it was refused; `members_sync_after_inviter_restarts` covers this.) So a node only shows its chain to nodes it chose to dial or that showed theirs. Refusals close with code 403.
  * This replaces the diagram's "present own record on the sync ALPN": the docs and gossip ALPNs belong to n0's crates, so we can't add a message to them.
  * Admission is per process and never revoked. Revocation waits for **Removing a member**.
  * The members ALPN is open to unknown IDs and not rate-limited; checking a chain is cheap.
* **Invites:** `invite.rs`. The ticket is `invite` plus base32 postcard of `{ inviter address, group, secret, expiry }`. The inviter stores `invites/<blake3(secret)>` holding the expiry; redeeming deletes the file, which is what makes an invite single-use, even across a race. `invite create` only writes that file, so it works while `run` holds the stores; from the CLI the ticket carries only the inviter's ID, found through n0's DNS.
* **Join:** `join.rs`, ALPN `/sync-node/join/0`. Redemptions run one at a time and a refusal waits 1 s. The Welcome carries the joiner's chain and a write ticket for the membership doc, not a list of member addresses: the joiner finds the other members through the doc's gossip swarm. `join` is one-shot: it imports the doc, waits for the first sync and its content (up to 30 s, so exiting doesn't cut the sync off and leave an empty doc), and exits. `run` resumes syncing the membership doc with every member it knows of (own chain plus its copy of the doc).
* **Tests:** `tests/record.rs` (chain checks), `tests/invite.rs` (ticket text, single use, expiry), `tests/membership.rs` (three nodes agree on the doc, also after a restart; reused, expired and forged invites; an outsider refused both ways; a chain rooted at a fake founder refused). `tests/node.rs`'s doc-sync test now puts both nodes in a group first.
* **Known issue, upstream:** every `iroh-docs` sync logs `WARN … sync failed … Failed to close … read error: closed stream`. It happens with plain `iroh-docs` 0.101 on `iroh` 1.3 and no gate. The entries are exchanged first; the error comes from the teardown's final `read_to_end(0)` on a stream that has already been read to its end, which `noq` reports as `ClosedStream`. `iroh-docs` still emits `SyncFinished` and `PendingContentReady`, but it skips registering the peer as useful and broadcasting a sync report to neighbours. Convergence doesn't depend on either. 0.101.0 is the newest release; recheck when a new one comes out.
* **Cross-network (2026-10-17):** the Pi joined the dev container's group by invite through n0's relays and DNS, and both ran as members.

#### Step 4: Group Membership & Connection Types

Built as two parts, done together.

**4a: what this node sees (logging, metrics, `status`)**

* [x] Log from `run`: the group and its members at startup, each member added, each peer admitted by the gate and why, connects and disconnects, and every change of a peer's selected path (direct IP or relay).
* [x] Track each peer's selected path, RTT and bytes sent and received, split into direct and relay, across reconnects. (A summary per peer every 60 s was logged at first; removed 2026-10-19, since `status` shows the same.)
* [x] `sync-node status`, printing a file the running node rewrites every presence interval.

**4b: the group's view**

* [x] Nodes in a group share, via gossip, which members are online and each pair's connection type (direct, relayed, none). This replaces the per-connection stats the rendezvous server couldn't hold.
* [x] Set the gossip view sizes to 8 active and 125 passive (done in step 2).
* [x] `status` shows the group as this node sees it.
* [x] Tests: three nodes agree on membership; a node going offline is noticed within a set time.
* [x] The developer runs `run` and `status` on the Pi and the dev container (armv7 rebuild: adds the `bytes` crate, already in the tree).
* **Done when:** each node in a three-node group shows the same members and correct connection types.

**Notes:**

* **Connection tracking:** `peers.rs`. The gate hook hands every admitted docs, gossip or blobs connection to `Peers::track`. A task per connection follows `Connection::path_events()`: it logs a peer's selected path when it changes (once per peer, not per connection) and, when the connection closes, adds the final per-path stats from `WeakConnectionHandle::closed()` to the peer's totals. Bytes are UDP payload bytes per path (`PathStats::udp_tx/udp_rx`), so they include QUIC overhead. A peer's kind is direct if any of its connections has a direct selected path.
* **Presence:** `presence.rs`. A gossip topic per group (`blake3("sync-node presence topic v0" ‖ group ID)`). Every interval (5 s by default, `Config::presence_interval`) each node broadcasts a signed `Presence { group, node, sent_at_ms, links }`, where `links` is its own connection kind to each member it is connected to. Messages are signed because gossip forwards them and `delivered_from` is only the last hop; unsigned, wrong-group or non-member messages are dropped. A member unheard for 4 intervals (20 s) is offline.
* **Links:** a pair's state comes from the newest report by either of the two, counting only online reporters: `direct`, `relay`, `not connected` (both online, neither reports the other) or `unknown (offline)`. The plan's "not neighbours" doesn't arise yet: each node keeps asking gossip to join every member it has no gossip link to, up to the active view of 8, so groups of 9 or fewer are fully meshed.
* **Monitor:** `monitor.rs`, one task per node once it is in a group. Each interval it re-reads the membership doc (logging new members), re-joins missing gossip peers, broadcasts presence, logs members coming online or going offline, and rewrites `status`. It no longer logs a periodic summary (removed 2026-10-19): the log keeps events (members joining, coming online, going offline, path changes) and `status` shows the current state.
* **Status file:** `status` in the config dir, replaced atomically (written to `status.tmp`, then renamed). `sync-node status` prints it and warns if it is older than 3 intervals, which means `run` has stopped.
* **First real run (2026-10-17, two nodes over n0):** connections start on the relay and switch to direct within a second or two, and the path change is logged. About 60 KiB each way crossed the relay before the direct path came up (handshakes, the chain exchange, gossip, the first membership sync). That's fine under the relay policy, which only covers file data, but it shows step 8 has to hold file transfers until the path is direct; just watching for the first direct path isn't enough.
* **Pi check (2026-10-18):** `run` and `status` on the Pi and the dev container both show the other member online over a direct link, with total bytes sent and received and the links summary.
* **Tests:** `tests/status.rs` runs three nodes with a 200 ms interval: all see three members online and every pair direct; traffic to a peer is non-zero and all direct; the status file catches up; after one node shuts down the other two mark it offline within 7 intervals.

#### Step 4b: Join by Node ID

* [x] `member add <id> [--name <name>]` signs a record for `id` and writes it to the membership doc. The running node holds the docs store, so the command drops a file `adds/<id>` (like `invites/`), holding the name if given, and the monitor applies it on its next interval and removes it. Running it again for an existing member only updates the name, so it is also how to rename a member, yourself included.
* [x] `invite create --name <name>` stores the name with the invite, and it is set when the invite is redeemed.
* [x] Wherever a member is shown (logs, `status`, stdout), show its name if it has one, otherwise its short ID. `status` also lists each member's full ID once, so names can be matched to keys.
* [x] `join` also takes a member's node ID (at first a `join…` ticket holding the member's address and group ID; reduced to the ID, since a node is in one group and the reply names it). The join ALPN gets a second request: fetch my chain. The reply is `Welcome` if `members/<remote id>` has a valid chain, otherwise a refusal that says to retry once the record has synced.
* [x] Fix the join protocol's one-at-a-time permit: release it before waiting for the connection to close, and put a timeout on reading the request, so an idle connection can't block other joins.
* [x] Tests: a name set on one node shows on every node; a rename replaces it everywhere; a duplicate name is refused.
* [x] Tests: a node added by ID joins without an invite, through the member that added it and through another member once the record has synced; an ID not in the doc is refused; an idle connection on the join ALPN doesn't block a real join.
* [x] armv7: the developer rebuilds and adds the Pi to the dev container's group by ID.
* **Done when:** a node added by ID joins a two-node group through either member, and all three hold the same membership doc.

**Notes:**

* **Module:** `member.rs` holds the `adds/` queue, names (`Names`, `read_names`, `check_name`) and `write_record`, which the join protocol, `create` and the monitor share.
* **Queue:** `member add` writes `adds/<id>` (via `adds/<id>.tmp` and a rename), holding the name or nothing. The monitor applies queued adds at the start of each interval: writes the record if the ID isn't a member yet, then the name if no other member has it. A file that can't be used (bad ID, bad name) is logged and removed; one that fails to write is kept and retried. If `run` isn't going, the add waits for the next start.
* **Names:** `names/<id>` holds the name as UTF-8, 1–32 characters, no control characters. Reading takes the newest entry per key across authors; if two members claim one name, the earliest claim keeps it. Refusals happen in the running node, so a refused name shows only in its log (`not naming … already has that name`), not in `member add`'s output.
* **Display:** gate, peers, monitor, join logs and `status` show the name if there is one, otherwise the short ID. `status` ends with an `ids:` list mapping each shown name to its full ID. Logs about non-members still use IDs.
* **Join by ID:** `join` takes an invite ticket or a node ID (`Ticket::{Invite, Member}`). With an ID the joiner finds the member through n0's DNS and takes whatever group that member is in; `join` prints the group and its founder so you can check. `member add` prints the `sync-node join <this node's id>` line to send to the new node.
* **Join protocol:** the request is now `JoinRequest::{Redeem, Fetch}`, so old and new nodes can't join through each other; rebuild both. Reading the request has a 10 s limit (`REQUEST_TIMEOUT`) and happens before taking the one-at-a-time turn; the turn covers only handling and the refusal delay, and waiting for the joiner to close is also capped at 10 s.
* **First CLI run (2026-10-18, two nodes over n0):** `member add <id> --name "pi 4"`, then `join <member id>` on the new node; both nodes then showed `pi 4` in `status` and the logs, linked direct. A 40-character name is refused by `member add` itself.

#### Step 5: Local File Index

* [x] `folder add <name> <path>` creates the folder's doc, lists it in the membership doc and records the local path. Queued for the running node, like `member add`.
* [x] Scan a folder into an index (per the index-model decision), hashing with BLAKE3 so hashes match `iroh-blobs`. Persist it across restarts.
* [x] Watch the folder for changes, with debouncing so editors and bulk copies don't flood the index. Apply ignore rules.
* [x] Tests: scan, change, rename, delete and restart all leave the index correct; a burst of changes yields one update per file.
* [x] armv7: new dependencies `notify` (inotify on Linux) and `ignore` (gitignore matching), both pure Rust. The developer rebuilds, runs `folder add` on the Pi and edits files in it.
* **Done when:** the index tracks a working folder accurately through edits and restarts.

**Notes:**

* **Code:** `index.rs` holds `FileMeta`, `FolderIndex` (scan and reconcile) and `watch` (the watcher task); `folder.rs` holds the `folders` file, the `folder-adds/` queue, the `folders/` listing in the membership doc and `Folders`, which runs one watcher per folder. `status` lists each folder with its file count and size, or `paused`.
* **Reconcile:** `FolderIndex::reconcile(path)` walks the path (without following symlinks), indexes each file whose size or modification time differs from its entry, and tombstones live entries under the path that are gone. It never tombstones an ignored path, or anything under a directory it couldn't read, and checks the marker before writing any tombstone. A file that changes while it is hashed is skipped; its next event brings it back. Only this node's own entries (its docs author) are read and written.
* **Unchanged content:** a file whose modification time changed but whose content didn't gets its entry rewritten with the same hash and version, so the next restart doesn't hash it again. Other nodes see the same content hash and have nothing to fetch.
* **Watcher:** `notify` with recursive watching, started before the first scan so nothing is missed. Changed paths are collected until none has changed for the debounce interval (`Config::debounce`, 1 s by default), then each is reconciled; a path that keeps changing is indexed anyway after 10 intervals. A watcher error, a queue overflow, a change to the root or to `.syncignore` triggers a full rescan.
* **Adding a folder:** `folder add <name> <path>` queues `folder-adds/<name>` holding the path; the monitor applies it on its next interval. A later request for the same name replaces an unapplied one. A name the group already has, or a path overlapping another folder, is refused in the node's log. A new folder's doc isn't shared with other members yet: that is step 6.
* **Tests:** `tests/index.rs`: scan, edit (including a same-size rewrite), rename, delete, a renamed directory, ignore and un-ignore, and changes made while the node was down are all indexed correctly, and unchanged files aren't rewritten on restart; 200 writes to 10 files give one entry update per file; a queued folder add is applied once per name; a missing root pauses the folder with no tombstones, and it resumes when the root is back.
* **First CLI run (2026-10-18):** 30 appends to a file and a directory rename made one pass of 3 updates; moving the folder root away paused the folder, and moving it back resumed it with nothing deleted.

#### Step 6a: Index Sync & Merged View

Indexes only: no file contents move and nothing on disk changes. The merged view is a prediction of the folder after step 6b, so the merge rules can be checked first.

* [x] Replace `replaces` with a version vector in `FileMeta`, and record the writing node.
* [x] Folder entries in the membership doc carry the folder doc's secret. `folder add` joins an existing folder of that name. Concurrently created folders of one name settle on one.
* [x] Each node lists itself under `folder-nodes/` and syncs the folder doc with the other nodes listed. Docs fetches the `FileMeta` blobs; nothing fetches file contents.
* [x] The merged view, computed the same on every node: per path, in sync, to fetch, deleted, or a conflict with the name each version would get.
* [x] `sync-node folder show <name>` prints it (written by the running node, like `status`).
* [x] Tests: two nodes with overlapping folders show the same view; files on one side are in the union; the same content on both is in sync; different content is a conflict with the expected names; a delete of a file the other has; repeated edits on one node while the other holds the old version are not a conflict; an edit beats a concurrent delete; concurrent folder creation settles on one folder; three nodes agree.
* [x] armv7: no new dependencies. The developer adds a folder of the same name on the Pi and the dev container and compares `folder show` on both.
* **Done when:** two and three nodes with overlapping folders show identical merged views that match the rules above.

**Notes:**

* **Code:** `view.rs` holds `Version`, `merge` (the rules for one path), `FolderView` and its rendering, and conflict names; `index.rs` gives each change its version (`next_version`) and reads every node's entries (`read_all`); `folder.rs` lists, joins, settles and syncs folders and writes their views.
* **Versions in practice:** a change takes this node's previous version of the path, merged with the version of every other node's entry with the same content, plus one. So two nodes that start with the same file independently agree on it, and an edit on either comes after both.
* **Membership doc entries:** `folders/<doc id>` → `FolderRecord { name, secret }` (postcard); `folder-nodes/<doc id>/<node id>` → `keeps`. A record whose secret doesn't match its doc ID is ignored. The canonical folder of a name is the one whose entry has the oldest docs timestamp, then the smaller doc ID.
* **Syncing:** each folder doc is synced with the nodes listed under `folder-nodes/`, starting with each one as it appears. The node's monitor runs `Folders::check` every interval: settle, sync, fetch missing metadata, resume paused folders; then rewrites changed views in `views/<name>` (postcard), which `folder show` renders. `--all` includes paths already in sync.
* **Views:** the view is rebuilt only after the folder doc changes (local or remote inserts, content arriving) or its node list grows. In a conflict, every node is shown as needing something (the conflict copies), so a conflicting path is never "in sync" until step 7 resolves it.
* **Found while testing — iroh-gossip 0.101.0 never redials a peer whose first dial failed.** After a failed dial to a peer that isn't in its active view, the peer's pending-send queue stays non-empty, and later sends only queue behind it (`net.rs`, `SendMessage` with `PeerState::Pending`; nothing clears it, since `handle_connection_closed` emits no `DisconnectPeer` for a non-active peer). The state is per peer, shared by every topic, so presence and docs' gossip for every doc both stop reaching that peer. It recovers only when the other node dials in. Two nodes that start while unable to reach each other (a laptop waking without network while the Pi reboots) stay apart until one restarts. Open: carry a one-line patch to iroh-gossip (`[patch.crates-io]`: drop the pending peer state on a failed dial) or report it upstream and wait.
* **Mitigations in our code**, which also help without the bug:
  * When presence shows a member coming online, the monitor re-syncs the membership doc and every shared folder doc with it directly (`start_sync` runs a direct sync, not only gossip).
  * Docs fetches an entry's content only from the node that sent the entry, and retries a failed fetch only when a neighbour announces the content over gossip. `Folders::check` fetches any folder metadata blob still missing from the folder's nodes (`META_FETCH_TIMEOUT` 5 s each). Without this, the three-node test failed about one run in four.
* **Test address book:** `Config::address_book` (tests only) is a `MemoryLookup` every test node publishes itself to when it starts, like n0's DNS. Tests that restart nodes use it; learning addresses after spawning hit the gossip bug.
* **Tests:** `tests/view.rs` (the rules on hand-made entries: ordering, same content, skipped edits, conflicts and their winner, deletes, node states, conflict names) and `tests/folder_sync.rs` (two nodes with overlapping folders: union, same content in sync, a conflict named as expected, three edits on one node not a conflict, a delete, an edit beating a concurrent delete; folders created apart settle on the older; three nodes agree).
* **Checking the index:** `folder show <name> --entries` prints every node's raw entries instead of the merged view: content hash in hex (compare with `b3sum`), size, modification time (UTC, to the nanosecond) and version vector. The running node writes them into the view file with the view.
* **Starting over:** `sync-node --clear-config [command]` deletes the config dir after a y/N question, then runs the command if one is given. It refuses a directory without a `key` file, and one whose `status` file is fresh (`run` is using it). Synced folders aren't touched.
* **First CLI run (2026-10-18, two nodes over n0):** `folder add pictures` on both; `folder show pictures` printed the same view on both, matching the rules.

#### Step 6b: Content Transfer

* [x] Nodes apply the merged view (step 6a): fetch content they're missing and delete what was deleted. Write each file to a temporary file and move it into place only after its hash checks out.
* [x] An interrupted transfer resumes rather than starting over.
* [x] Tests: two nodes converge from different starting folders; a transfer killed midway resumes; an unsafe path from a peer is refused.
* [x] CLI run: two nodes with `run --with-content` converge on a folder, checked with `b3sum` against `folder show --entries`.
* [x] armv7: no new runtime dependencies (`bao-tree` is for tests only). The developer rebuilds and runs the CLI check with the Pi.
* **Done when:** two nodes converge on the same folder contents, verified by hash.

**Notes:**

* **Behind a flag, for now:** `run --with-content` (`Config::content`). Without it a node behaves as in step 6a: it syncs the index and changes nothing on disk. Remove the flag once content transfer has proved itself.
* **Code:** `content.rs` holds the content task, one per folder, started and stopped with the folder's index task. It wakes on every change to the folder doc and every 10 s (`RETRY`), merges every path, and for each path where this node is behind: fetches the winning content from the nodes holding it (`Downloader`) and asks the index task to place it, or asks it to delete the file. Deletes go first. Conflicts are left alone (step 7).
* **The index task makes every disk change** (`index::Apply`), since it owns this node's entries: `FolderIndex::place` and `remove` change the disk and write the entry together. Each first checks that this node's entry is still the one the content task saw and that the disk still matches it (same size and modification time, or no file), so a local edit not indexed yet is never overwritten or deleted; the reconcile then indexes it, and the merge decides.
* **Placing a file:** exported from the blob store to `.sync-node-tmp-<random>` beside the target, hashed again with BLAKE3 and checked against the entry's hash and size, given the entry's modification time, synced, and renamed into place. The new entry takes that size and modification time, so the watcher's event for the rename finds nothing to index. Its version merges this node's old version with every entry holding that content, without a new edit of its own. Deleting a file also removes the directories it leaves empty.
* **Unsafe paths:** a path from another node is written only if it is relative, `/`-separated, has no empty, `.` or `..` part and no NUL (`index::safe_path`), and isn't ignored here (the marker, temporary files, `.syncignore`). Parent directories are created one at a time and must be real directories, never symlinks. Refusals are logged once per path.
* **Blob store (decided):** with content on, the index adds every file to the store **by reference** (`ImportMode::TryReference`): no copy, and the import's hash replaces our own hashing. A file indexed earlier without `--with-content` is imported on the next scan. The store never deletes a referenced file. If a file changes, its old hash's reference goes stale; a node fetching that old hash gets a verification error and tries another node.
* **Fetched files are stored twice:** the downloaded blob stays in the store after it is exported to the folder, since iroh-blobs only deletes blobs through GC, which is off. Open: turn GC on with tags protecting docs' blobs, or re-import placed files by reference and drop the owned copy.
* **First CLI run (2026-10-19, the Pi and the dev container):** both nodes ran `run --with-content` and converged on a shared folder.
* **Resume:** iroh-blobs keeps a partial download in the store across restarts and fetches only the missing chunks. The test puts half a blob into the store directly (the state a cut-off download leaves) rather than killing a transfer at a precise moment, restarts the node, and checks that only about half the file crosses the wire.
* **Not yet:** leftover temporary files after a crash aren't cleaned up (they're ignored, so they never sync); no progress is logged during a large download, and one slow download holds up the rest of that folder's pass; file permissions aren't synced, so a fetched file gets the default mode.
* **Tests:** `tests/content.rs`: two nodes converge (files on one side, nested directories, the same file on both), a conflict is left alone, then a delete, an edit and a delete that empties its directories all reach the other node; unsafe paths (`../`, `x/../../`, absolute, the marker, through a symlink) are refused while an honest file arrives; a partial download resumes after a restart.

#### Step 7: Conflicts, Deletes & Renames

* [ ] Implement the conflict, tombstone and rename decisions.
* [ ] Tests for each: concurrent edits keep both versions; a delete stays deleted after an offline node returns; a rename moves the file without re-sending its content.
* **Done when:** a three-node group converges correctly through concurrent edits, deletes and renames, including one node offline during the changes.

#### Step 8: Relay Policy

Deferred until membership and sync work. This step adds the hook that keeps file data (folder index and contents) off relays under direct-only; connectivity, gossip, join and membership keep using relays under either policy (see **Constraints**). Step 1 found that an `after_handshake` hook can hold a connection until a direct path is selected, per peer, before any protocol crate gets it, but that docs and blobs connections also carry membership, so the gate has to be finer than per ALPN (see step 1's findings for the catches). If that doesn't hold up in practice, fall back to a per-node policy, or to stopping file sync once a path moves to a relay (some file data may already have crossed it). Turning relays off is not a fallback.

* [x] Watch each connection's paths and log every change to the selected path (done in step 4, `peers.rs`).
* [ ] Implement direct-only and relay-allowed, per peer if step 1 shows that's possible, otherwise per node. Configure which relays to use (n0 defaults or self-hosted).
* [ ] Block outbound UDP on one side to force relay-only, and confirm that under direct-only gossip and membership still work over the relay while file sync refuses, and that relay-allowed syncs files. Check the relay path's `PathStats` to see what it carried.
* [ ] Record which form of the policy was possible, and update **Constraints** to match.
* **Done when:** both policies behave as recorded, file data never crosses a relay under direct-only, and the logs show what the relay carried.

#### Step 9: Cross-Network Verification

* [ ] Sync a realistic folder (many files, some large) between the dev container and the Pi on their separate networks, under both relay policies.
* [ ] Log throughput, how long the initial sync takes, and path changes during the run.
* **Done when:** the folder converges across the two networks under both policies, and direct-only never sends file data over a relay.
