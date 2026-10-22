### Web UI Plan

* **Progress:** [progress.md](progress.md) is the handoff between sessions; read it first.
* **What:** [spec.md](spec.md). **Why in the process:** [ADR 0004](../../docs/adr/0004-web-ui-in-the-node-process.md). **Terms:** [GLOSSARY.md](../../GLOSSARY.md), Interface.
* **Objective:** `sync-node run --ui-addr …` serves a web UI: a JSON API and a SolidJS page with the Group, Folders, Transfers and Setup views.
* **Order:** this plan comes before ADR 0003 (own index exchange), which gets its own plan after it. The sync plan's step 8 (relay policy) waits for both.
* **Rules:** work in order, one commit per step (the developer commits), tick a box only when its "Done when" holds. `just check` passes at the end of every step, and `just ui-check` from step 3. Views use shadcn-solid components only (spec §7). The developer runs every armv7 build.

#### Step 0: Spec review

* [x] The developer reviews [spec.md](spec.md) and corrects it (2026-10-22: TanStack Router for navigation; library choices only in the spec).

**Done when:** the developer accepts the spec.

#### Step 1: Tooling and the empty page

* [ ] bun as a devcontainer feature in `.devcontainer/devcontainer.json` (`ghcr.io/shyim/devcontainers-features/bun:0`, added). The developer rebuilds the container.
* [x] `ui/` with the front end of [spec §7](spec.md#7-front-end-and-build). One page with the nav bar and four empty views, one router path each (`/group`, `/folders`, `/transfers`, `/setup`; `/` goes to `/group` until `/api/status` exists). `bun.lock` committed.
* [x] shadcn-solid set up ([spec §7](spec.md#7-front-end-and-build)): theme in `index.css`, `lib/cva.ts`, `components/ui/button.tsx`; the nav bar uses `buttonVariants`.
* [x] `.gitignore`: `ui/node_modules`, `ui/dist`.
* [x] `just ui` (build to `ui/dist/`), `just ui-dev` (dev server, `/api` proxy), `just ui-check` (`tsc --noEmit`, lint). `just ui-dev <addr>` proxies to `127.0.0.1:7777` by default.

**Done when:** `just ui` writes `ui/dist/index.html`; `just ui-dev` shows the page with hot reload; `just ui-check` passes.

#### Step 2: The server, the seam and `GET /api/status`

* [x] `ui.rs` (or `ui/` module): `UiBackend` trait, `ui::router(backend, config)`, axum. `Host` check (403) and `Content-Type` check on `POST` (415). JSON error body.
* [x] Files from the UI dir; unknown non-API paths get `index.html`; `UI not found at <path>` when missing.
* [x] `--ui-addr` / `SYNC_NODE_UI_ADDR`, `--ui-dir` / `SYNC_NODE_UI_DIR` (default `ui/` next to the executable) on `run`. No UI address, no server. Bind failure fails `run`.
* [x] `GET /api/status`, in a group and in none. `impl UiBackend for Node`.
* [x] `tests/ui.rs`: fake backend, `oneshot`. Status in and out of a group, 403, 415, `UI not found`.
* [x] Check: `run --ui-addr` with no group starts and answers `/api/status` (by hand, in the dev container).

**Done when:** `just check` passes; `curl` against `run --ui-addr 127.0.0.1:<port>` gets the status JSON. **armv7:** new crates (axum, serde_json, maybe tower-http); the developer checks the build.

#### Step 3: Shared types

* [ ] `ts-rs` (dev dependency) on the API structs; a test writes `ui/src/api/*.ts`.
* [ ] `just ui-check` fails when the written types differ from the committed ones.

**Done when:** `just check` and `just ui-check` pass; changing an API struct without rerunning the test makes `just ui-check` fail.

#### Step 4: Group and Folders views

* [ ] `GET /api/folders`, `GET /api/folders/{name}` (404 when not kept or no view yet). Tests.
* [ ] Group view and Folders view, polling every 2 s while shown.

**Done when:** with two nodes in the dev container, the Group and Folders views show the same as `status` and `folder show`.

#### Step 5: Actions and the Setup view

* [ ] `POST /api/group`, `/api/join` (60 s limit, 504), `/api/invites`, `/api/members` (202, queued), `/api/folders` (202, queued). Same validation and error texts as the CLI; 409 for the wrong state. Tests: each route, each refusal, queue files written.
* [ ] Setup view: create or join when in no group; invite (ticket to copy), add member, add folder when in one. Setup is the first view when in no group.

**Done when:** a new node joins a group from the page only (`run --ui-addr`, paste a ticket), and a folder added from the page syncs.

#### Step 6: Transfers view

* [ ] Snapshot functions for the swarms (`Swarms`/`Leeching`, the picker's progress, peers, open requests, snubbed, stuck), uploads (`Uploads`, rate per peer) and the choker (unchoked set, optimistic unchoke). Copy, release the lock, then return.
* [ ] `GET /api/transfers` and the Transfers view. Tests with the fake backend.

**Done when:** during a large file sync between two nodes, the Transfers view shows the swarm's pieces filling up and the peer unchoked; a test sync under `just stress` doesn't get worse with the page open (judge on 5 runs).

#### Step 7: Pi and as built

* [ ] `just armv7` runs `just ui` and copies `ui/dist/` to `~/ui` on the Pi.
* [ ] The developer builds for armv7, runs `run --ui-addr 127.0.0.1:<port>` on the Pi, and opens the page from the laptop through `ssh -L`.
* [ ] `spec.md` updated to as built; GLOSSARY checked.

**Done when:** the developer reports the page working against the Pi.
