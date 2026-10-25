# Progress Report (2026-10-22)

Handoff for the next session. Read this first, then [plan.md](plan.md) and [spec.md](spec.md).

## Outstanding checks

* **`just stress` before and after step 7** (plan step 7 "Done when"): 5 runs each, no page; compare failures and median times. The developer makes the before checkout (`git worktree add ../rustp2p-before <step-6 commit>`).
* **armv7 build** with the direct `arc-swap` dependency, and the fixed `just armv7` (copies to `~/ui/`, builds the UI first).
* Not checked: a download from more than one peer in the Transfers view.

## Next

* ADR 0003 (own index exchange) stays *proposed*; the developer comes back to it later (2026-10-22). No plan for it yet. The sync plan's step 8 (relay policy) still waits for it.

## State

* The BitTorrent plan is done: the developer confirmed the armv7 build runs on the Pi (2026-10-21).
* Grilling session done (2026-10-21): the decisions are in [spec.md](spec.md), [ADR 0004](../../docs/adr/0004-web-ui-in-the-node-process.md) and GLOSSARY (Interface).
* Step 0 done (2026-10-22): the developer accepted the spec, with TanStack Router added. Library choices are only in spec §7; the plan refers to it.
* **Step 1 in progress (2026-10-22):** `ui/`, `.gitignore` and the `just ui*` recipes are done; `just ui` and `just ui-check` pass (checked with a bun 1.2.23 binary from the session scratchpad, not installed). The dev server serves every view path and proxies `/api`. **Open:** the developer rebuilds the dev container (bun feature), then checks `just ui-dev` in the browser with hot reload.

* **shadcn-solid added (2026-10-22):** the developer wants only shadcn components. shadcn/ui is React-only; the developer chose to keep SolidJS and use the port shadcn-solid (spec §7). Theme, `cva` helper and Button are in `ui/src/`; the nav bar uses `buttonVariants`. `just ui` and `just ui-check` pass. Not seen in a browser yet (no browser in the container).
* **Step 2 done (2026-10-22):** `src/ui.rs` (`UiBackend`, `ui::router`, `UiConfig`, `ApiError`, the status structs), `run --ui-addr` / `--ui-dir`, `tests/ui.rs` (8 tests). `just check` passes. By hand: `run --ui-addr 127.0.0.1:7781` in no group answers `/api/status`, 403 for a bad `Host`, 415 for a non-JSON `POST`, `index.html` for `/setup`; a taken address fails `run`; Ctrl-C exits 0. **Open:** the developer checks the armv7 build (new crates: `axum`, `serde_json`, `tower`, `tower-http`; tokio `net` feature).

## Decisions 2026-10-21

* The web UI is part of `sync-node run`, not a separate process (ADR 0004).
* For the developer and for users: it shows what the node sees and does the setup actions.
* Off by default; on only with `--ui-addr` / `SYNC_NODE_UI_ADDR`. Loopback in use.
* Auth parked. Meanwhile: `Host` check and `Content-Type: application/json` on writes.
* SolidJS, Tailwind v4, TypeScript, Vite, bun (devcontainer feature).
* Not embedded: `just ui` builds `ui/dist/`; the server reads the UI dir, `ui/` next to the executable by default.
* axum; the page polls; live values only, no history.
* ts-rs generates the TS types; `just ui-check` checks them. `just check` stays Rust-only.
* Member add and folder add go through the existing queues: 202 `{ queued: true }` after validation. Join and create wait (60 s limit).
* Tests: the router with a fake `UiBackend` through `oneshot`. No node, no port.
* The UI comes before ADR 0003, which gets its own plan.

## Facts found

* `Node::create_group` and `Node::join` start the monitor on a running node (`node.rs`), so `run` can join from the page. The monitor's `Folders::check` then starts the folder watchers.
* `join`/`create` on the CLI start a second node and can't run while `run` runs; that stays.
* The presence interval is 5 s (`PRESENCE_INTERVAL`).
* `hyper`, `http`, `tower` are already in `Cargo.lock` (through iroh).

* **Step 3 done (2026-10-22):** ts-rs export test in `ui.rs` (unit test, so `cfg(test)` works for lib types), `just ui-types`, freshness check in `just ui-check`. Checked: a renamed field in `PeerInfo` makes `just ui-check` fail with the diff; `just check` and `just ui-check` pass. The page: `lib/api.ts`, `/` redirects by `/api/status`, Alert on a load error. A node serves the built page (`--ui-dir ui/dist`). **Open:** the developer opens the page in a browser: `/` must go to Setup with no group, and the error Alert must show with no node (`just ui-dev` with no node running).

* **Step 4 done (2026-10-22):** `GET /api/folders` and `GET /api/folders/{name}` (`UiBackend::folders`, `UiBackend::folder`, `Node::view_file`), 3 new tests (11 in `tests/ui.rs`). Page: `lib/poll.ts` (every 2 s while the view is shown, no overlap, the last answer stays beside an error), `lib/format.ts`, Group view, Folders view, folder view at `/folders/$name`. New shadcn-solid components: Card, Table, Badge, Switch. Checked with two nodes in the dev container (one group, one folder, a conflict, a delete): `/api/status` matches `status` and `/api/folders/photos` matches `folder show --all`; the 404 text is correct. **Validated by the developer on both devices (2026-10-22).** Seen, not fixed: Ctrl-C on a node in a group took 3 to 8 s to exit, with the UI idle; not checked without `--ui-addr`.

* **Step 5 done (2026-10-22):** `ui/topics.rs` (`Topics`, `Subscription`, `ClientMessage`, `ServerMessage`, `Update`, `TOPIC_INTERVAL` 2 s), `GET /api/ws` with the `Origin` check, the socket loop, `Topics::stop` before graceful shutdown in `run`. 9 new tests (20 in `tests/ui.rs`), with paused time and no socket. Page: `rxjs`, `lib/socket.ts` (`live`, `once`), views on UI topics, `lib/poll.ts` and `lib/api.ts` removed, `PollError` is now `LiveError`. Vite proxy: `ws: true`, `Origin`. Checked against a real node with a bun WebSocket script: a foreign `Origin` is refused; subscribe, unsubscribe and an unknown topic work; an unchanged `status` is sent once; the log shows a topic's timer start and stop (`web UI: reading topic …` / `stopped reading topic …`), also when the page closes its socket; Ctrl-C with the socket open closes it with 1001 and `run` exits 0 in 0.2 s. **Validated by the developer (2026-10-22): works in the browser, and the armv7 build runs on the Pi.**

* **Step 6 done (2026-10-22), validated by the developer from the page:** `ui/actions.rs` (five `POST` routes, request and answer types), `UiBackend::create_group` / `join`, `GroupError` → 409, `Topics::spawn` for create and join. 7 new tests (27 in `tests/ui.rs`): each route, each refusal, the queue files, the 60 s limit (paused time), a bad body. Page: `lib/api.ts` (`POST` client), `lib/action.ts`, Setup view (create, join; invite with the ticket to copy, add member, add folder), new shadcn-solid component TextField. Checked with three running nodes through the same `POST`s the page sends: A creates, A invites, B joins in 1.9 s, both add `docs`, A's file reaches B in 7 s; A adds C by ID, C joins with A's ID; 409s and their texts are correct; all three stop cleanly. **Open:** the developer's check from the page only (step 6 "Done when"). Folder paths on the page must be absolute (the CLI takes relative ones).

* **Step 7 built (2026-10-22); browser check validated by the developer, 1 to 1 (2026-10-22):** a large sync between two nodes shows in the Transfers view. Not checked: a download from more than one peer (the developer has no setup for it). `peer_wire::Transfers` (`SwarmSnapshot`, `SwarmPeer`, `InSwarm`, `UploadSnapshot`), `Swarms::snapshot`, `Uploads::snapshot`, `Node::transfers`; `Picker::held`, `Picker::active`, `Choker::optimistic`. The download copies its peers and picker counts into its `Leeching` entry at most once a second (`SNAPSHOT_EVERY`). `UiBackend::transfers` and `UiBackend::label`, `GET /api/transfers`, UI topic `transfers`, `TransfersAnswer` types. Page: Transfers view, new shadcn-solid component Progress. Tests: 3 new in `tests/ui.rs` (30 now), `progress_counts` in `tests/picker.rs`, `Choker::optimistic` in `tests/choker.rs`, and in `tests/peer_wire.rs` (`a_leecher_serves_from_its_staging_file`) the seeder's upload row and the leecher's swarm after the seeder stops. That check is at the end of the test: before the bad-piece check, its wait let b fetch the bad piece again from c (it failed once in `just check`). `just check`, `just ui-check`, `just ui` pass. Checked with two nodes in the dev container (debug build): during a 600 MiB sync B's swarm went 281 → 1195 of 1200 pieces, A in the swarm and unchoked, 250 open requests, about 25 MB/s; A's upload row showed B unchoked at the same rate; `transfers` over the WebSocket; both nodes stopped at Ctrl-C. **Open:** `just stress` with the page open, 5 runs (the stress tests start their own nodes in the test process, so no page can watch them. Proposal: run `just stress` 5 times with a third node syncing and its Transfers page open on the same machine, and 5 times without the page, and compare). **armv7:** no new crates.

* **Step 8 (2026-10-22):** the developer reported the copy of `ui/dist` and `run --ui-addr` working on the Pi. The `armv7` recipe copied `ui/dist` to `~/dist` (not the default UI dir `~/ui`) and didn't build the UI first: fixed (`armv7: ui`, `rsync -avP --delete ui/dist/ g@raspberrypi:~/ui/`), not run. Spec marked as built; GLOSSARY: Snubbed, Stuck added, `transfers` in UI topic. **Open:** the developer runs the fixed `just armv7` and removes the old `~/dist` on the Pi if it's there. Step 7's `just stress` item is still open.

* **Snapshots always published (2026-10-22, after step 8):** the developer wants the download loop unchanged by an observer. Now `Leeching::view` is an `ArcSwap<DownloadView>` (with `have` from the new `Picker::have_count`; the reader no longer borrows the `have` watch, which blocked `send_modify`), and `Uploads` publishes its rows every second from the choker's task into an `ArcSwap` (`Uploads` has named fields `state`, `published` now). Spec §4 has the design and the rejected one (the reader asks the loop). New direct dependency `arc-swap` 1 (1.9.2, already in the tree through `iroh-dns`; no new crate in `Cargo.lock`). The peer-wire test waits for the seeder's published rows (`eventually`). `just check` and `just ui-check` pass; checked with two `run` nodes. Plan step 7 "Done when" changed: `just stress` before and after step 7, no page. **Open:** that comparison (the developer makes the before checkout with `git worktree add`); the armv7 build (new direct dependency).

## Decisions 2026-10-22

* **The page streams its reads over one WebSocket, by UI topic (ADR 0005, accepted 2026-10-22).** Topic names are the read routes (`status`, `folders`, `folders/<name>`). `GET` read routes stay; actions stay `POST`. The server reads a topic on a timer, but only while some page subscribes to it, with one timer per topic for every connection, and sends only on change. No test opens a port: the UI topics are tested without a socket; the socket loop is checked in the browser. `rxjs` / `rxjs/webSocket` on the page. New plan step 5; actions are step 6, Transfers step 7, Pi step 8.
* Term: **UI topic**, not "topic" alone (that's the gossip presence topic in `presence.rs`).
* Components: shadcn-solid only (copied, no CLI). Rejected: React with the official shadcn/ui; the solid-ui port (Tailwind v3 docs).

## Notes for step 5 and later

* `run` holds the node in an `Arc` for the server. At Ctrl-C the server stops with graceful shutdown, then `Arc::try_unwrap` gets the node back for `shutdown`. A long request (the 60 s join of step 6) delays the stop; an open WebSocket would hang `run` or make it skip `Node::shutdown`, so step 5 adds a stop signal that ends every socket loop first (spec §2, Stopping).
* With `cargo run`, the default UI dir is `target/debug/ui`. Use `--ui-dir ui/dist` or `just ui-dev`.
* A new API struct: derive `TS` as in `ui.rs` and add its `export_all` call to `export_api_types` (dependencies come with it).
* `ui/src/api/` is generated only; hand-written API code goes in `ui/src/lib/api.ts`.
* Views read with `live(topic, kind)` from `lib/socket.ts` and show errors with `LiveError`. A new read: a `Topic` variant and an `Update` variant in `ui/topics.rs`, a `read_*` function shared with its `GET` route, and `just ui-types`.
* A view's data depends only on the API answer; labels (name or short ID) come from the server.

## Developer preferences

* ASD-STE100 in chat (CLAUDE.md); never touch the git index; never run the armv7 build.
* Write down how a thing must work before building it; review the spec first.
