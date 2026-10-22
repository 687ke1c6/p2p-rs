### Web UI Spec

How the web UI must work, agreed with the developer on 2026-10-21 (grilling session). **The developer reviews this spec before any code is written** (plan step 0). Terms: [GLOSSARY.md](../../GLOSSARY.md), Interface. Why in the process: [ADR 0004](../../docs/adr/0004-web-ui-in-the-node-process.md).

#### 1. Purpose

* **For the developer:** see what the node does while testing: members and links, folders, and the transfers (swarms, pieces, choking, rates, stuck fetches).
* **For users:** set up a node without the CLI: create a group, join one, make an invite, add a member, add a folder.
* **Live values only.** No history, no graphs over time. A ring buffer for rates can come later behind the same API.

#### 2. Process and settings

* The server is part of `sync-node run` (ADR 0004). No other command starts it.
* **Off by default.** It starts only when a UI address is given: `--ui-addr <addr:port>` / `SYNC_NODE_UI_ADDR`. Use a loopback address (`127.0.0.1:<port>`). On the Pi, reach it with `ssh -L <port>:localhost:<port> g@raspberrypi`.
* **UI dir:** `--ui-dir <path>` / `SYNC_NODE_UI_DIR`; by default `ui/` next to the executable (the directory of `std::env::current_exe()`). The page is not embedded in the binary.
* If the UI dir is missing or has no `index.html`, the API still works and `/` answers with a short text: `UI not found at <path>`.
* `run` with a UI address works when the node is in no group yet. Then the user creates or joins a group from the page (§5). `Node::create_group` and `Node::join` already start the monitor on a running node; the folder watchers start on the monitor's next `Folders::check`.
* If the server can't bind its address, `run` fails at start with the error. It doesn't run on without the UI.
* The server shares the node's tokio runtime. A handler copies what it needs, releases every lock, then serializes. No lock is held across an `.await`.

#### 3. Access (auth parked)

Auth is **parked**: no token, no login. Until it is decided:

* The server refuses (403) a request whose `Host` header isn't the UI address (`<addr>:<port>`, or `localhost:<port>` for a loopback address). Stops DNS rebinding.
* Every `POST` must have `Content-Type: application/json` (else 415). A web page in the same browser then can't send one with a plain HTML form.
* A LAN bind (`0.0.0.0`, a LAN address) works but has no auth. The spec for auth comes before a LAN bind is recommended to users.

#### 4. API

JSON under `/api`. Every other path is a file from the UI dir; an unknown path that isn't a file gets `index.html` (the page routes in the browser). No API versioning: the page and the binary are built together.

**Errors:** any non-2xx answer has the body `{ "error": "<text>" }`. The text is what the CLI prints for the same fault. 400 for bad input, 409 for the wrong state (in a group already, in no group), 504 for a time limit, 500 for the rest.

**Reads** (the page polls each about every 2 s, only the routes of the shown view):

| Route | Answer |
|---|---|
| `GET /api/status` | This node's ID and whether it is in a group. In a group: the same data as `sync-node status` (`Status`): group, founder, members (name, online, last heard, this node's connection and traffic), pair links, folders. |
| `GET /api/folders` | Each folder this node keeps: name, path, file count, bytes, paused, doc peers (`FolderState`). |
| `GET /api/folders/{name}` | The folder's merged view, the data of `sync-node folder show` (`ViewFile`): files to fetch, deletes, conflicts, in sync. 404 if this node doesn't keep it or there is no view yet. |
| `GET /api/transfers` | Each active swarm: folder, path, content hash, size, piece count, pieces held, peers (choked or not, interested, rate, open requests, snubbed), whether stuck; this node's uploads per peer (unchoked, rate); the choker's unchoked set and optimistic unchoke. New snapshot functions on the swarm and choker types (plan step 6). |

**Actions:**

| Route | Body | Answer |
|---|---|---|
| `POST /api/group` | `{}` | 200 `{ "group": <id> }`. Creates a group (`Node::create_group`). 409 if in a group. |
| `POST /api/join` | `{ "ticket": <invite ticket or member ID> }` | 200 `{ "group": <id>, "founder": <id> }`. Waits until the join is done (`Node::join`), at most 60 s (then 504). 409 if in a group. |
| `POST /api/invites` | `{ "hours": <n, default 24>, "name": <optional> }` | 200 `{ "ticket": <text> }`, as `invite create`. |
| `POST /api/members` | `{ "id": <node ID>, "name": <optional> }` | 202 `{ "queued": true }`. Validated first (`check_name`, in a group), then queued with `member::queue_add`, as `member add`. |
| `POST /api/folders` | `{ "name": <folder name>, "path": <directory on this node> }` | 202 `{ "queued": true }`. Validated first (`check_folder_name`, path canonicalized and a directory, in a group), then queued with `folder::queue_add`, as `folder add`. |

Queued actions apply within one presence interval (5 s). The page says so; the next polls show the result.

**Types:** the API's request and answer structs derive `ts-rs` `TS`. A test writes the TS types to `ui/src/api/`; `just ui-check` fails when the committed file differs.

#### 5. The backend seam

The handlers take a `UiBackend` trait object, not a `Node`:

* `status()`, `folders()`, `folder(name)`, `transfers()`, `create_group()`, `join(ticket)`.
* The running node implements it (`impl UiBackend for Node`). The tests implement a fake.
* Invites and queued writes use the config dir only (`invite::create`, `member::queue_add`, `folder::queue_add`), so the router gets the config dir too. In tests, a temp dir.
* The router is built by one function, `ui::router(backend, config)`, so tests call it with `tower::ServiceExt::oneshot`: no port, no node.

#### 6. Views

One page, four views, a nav bar to move between them:

1. **Group:** this node, the members (name or short ID, online, last heard), each pair's link, traffic per peer split into direct and relay. The data of `status`.
2. **Folders:** each folder with its file count, size and paused state; open one to see its merged view (to fetch, deletes, conflicts; in sync on request).
3. **Transfers:** the developer view of §4 `GET /api/transfers`.
4. **Setup:** in no group: create a group, or join with a ticket or member ID. In a group: make an invite (hours, name) and show the ticket to copy; add a member (ID, name); add a folder (name, path).

The node's state decides the first view: Setup when in no group, Group otherwise.

**Navigation:** TanStack Router (`@tanstack/solid-router`) moves between the views. Each view has its own path, so a view can have a bookmark and the browser's back button works. The server's `index.html` fallback (§4) lets a reload on any view path work.

#### 7. Front end and build

* Source in `ui/` at the repo root, outside the cargo workspace: SolidJS, TypeScript, Vite (`vite-plugin-solid`), TanStack Router (`@tanstack/solid-router`), Tailwind v4 (`@tailwindcss/vite`). Linter: ESLint with `typescript-eslint` and `eslint-plugin-solid`. TypeScript stays on 6 until `typescript-eslint` supports 7. **bun** as package manager and runner, `ui/bun.lock` committed. `ui/node_modules` and `ui/dist` are git-ignored.
* **Components: shadcn-solid only.** Every component on the page is a [shadcn-solid](https://github.com/hngngn/shadcn-solid) component (the community SolidJS port of shadcn/ui, on Kobalte; the official shadcn/ui is React-only). No hand-made components: views put shadcn-solid components together, with plain elements and Tailwind classes for layout and text only. A link that looks like a button uses `buttonVariants`, as in shadcn.
    * There is no CLI: copy a component from `apps/docs/src/registry/ui/` when a view first needs it, into `ui/src/components/ui/`, and change `@/registry/lib/` to `@/lib/`. Helpers from `registry/lib/` go to `ui/src/lib/`. Keep the copied files as they are (their own code style); changes go in the view.
    * Copied from commit `4c75b89` (2026-02-13). The theme in `ui/src/index.css` is the port's installation style (Tailwind v4, `tw-animate-css`, dark mode by `data-kb-theme="dark"`).
    * Packages: `@kobalte/core`, `cva` (`1.0.0-beta.4`, as the port uses), `tailwind-merge`, `tw-animate-css`. `@/` is `ui/src/` (tsconfig `paths` and a Vite alias).
* The dev container gets bun as a devcontainer feature (the developer rebuilds the container).
* `just ui`: `bun install` and `vite build` into `ui/dist/`.
* `just ui-dev`: the Vite dev server, with a proxy from `/api` to a running `sync-node run --ui-addr …` (the proxy rewrites `Host`, `changeOrigin`). The usual workflow in the dev container.
* `just ui-check`: `tsc --noEmit`, the linter, and the ts-rs freshness check. `just check` stays Rust-only and needs no bun.
* `just armv7`: runs `just ui` first, and copies `ui/dist/` to `~/ui` on the Pi next to `~/sync-node`. `Dockerfile.armv7` doesn't change.
* New Rust crates: `axum`, `serde_json`, `tower-http` (serving files, if needed), `ts-rs` (dev only). They can affect the armv7 build: the developer checks it.

#### 8. Tests

* Rust, `tests/ui.rs`: the router with a fake `UiBackend` and a temp config dir, through `oneshot`. Every route: answer code and JSON; the errors (400, 404, 409, 415, 403); a queued member and folder land in the queue files; `UI not found` when the UI dir is missing.
* No test starts a node or opens a port. No front-end component tests in the first version.

#### 9. Not in this plan

* Auth (parked), and so a recommended LAN bind.
* History, graphs, SSE or WebSocket push.
* The CLI using the API (`join` and `create` while `run` runs). The CLI stays as it is.
* ADR 0003 (own index exchange): its own plan, after this one.
