### Web UI Spec

How the web UI works, agreed with the developer on 2026-10-21 (grilling session) and reviewed before any code was written (plan step 0). **As built** at plan step 8 (2026-10-22); the "As built" notes say where the build differs from or adds to the first text. Terms: [GLOSSARY.md](../../GLOSSARY.md), Interface. Why in the process: [ADR 0004](../../docs/adr/0004-web-ui-in-the-node-process.md). Why a WebSocket: [ADR 0005](../../docs/adr/0005-web-ui-reads-stream-over-a-websocket.md) (2026-10-22).

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
* **Stopping:** at Ctrl-C, `run` first sends the router's stop signal. Every WebSocket loop (§4 UI topics) sends a close frame and ends, so it drops its subscriptions and its hold on the node, and the topic timers stop. Then the server stops with graceful shutdown, and `run` shuts the node down. An open page never makes `run` hang or skip `Node::shutdown`. The page shows that it can't reach the node and connects again every 2 s.
* The server shares the node's tokio runtime. A handler copies what it needs, releases every lock, then serializes. No lock is held across an `.await`.

#### 3. Access (auth parked)

Auth is **parked**: no token, no login. Until it is decided:

* The server refuses (403) a request whose `Host` header isn't the UI address (`<addr>:<port>`, or `localhost:<port>` for a loopback address). Stops DNS rebinding.
* Every `POST` must have `Content-Type: application/json` (else 415). A web page in the same browser then can't send one with a plain HTML form.
* A LAN bind (`0.0.0.0`, a LAN address) works but has no auth. The spec for auth comes before a LAN bind is recommended to users.

#### 4. API

JSON under `/api`. Every other path is a file from the UI dir; an unknown path that isn't a file gets `index.html` (the page routes in the browser). No API versioning: the page and the binary are built together.

**Errors:** any non-2xx answer has the body `{ "error": "<text>" }`. The text is what the CLI prints for the same fault. 400 for bad input, 409 for the wrong state (in a group already, in no group), 504 for a time limit, 500 for the rest.

**Reads** (one-shot `GET`s for `curl` and the tests; the page reads the same data as UI topics, below):

| Route | Answer |
|---|---|
| `GET /api/status` | This node's ID and whether it is in a group. In a group: the same data as `sync-node status` (`Status`): group, founder, members (name, online, last heard, this node's connection and traffic), pair links, folders. |
| `GET /api/folders` | Each folder this node keeps: name, path, file count, bytes, paused, doc peers (`FolderState`). |
| `GET /api/folders/{name}` | The folder's merged view, the data of `sync-node folder show` (`ViewFile`, the view file the node last wrote): the nodes with their labels, the counts, and every path (in sync too) with its class (`InSync`, `ToFetch`, `ToDelete`, `Conflict`, `DeletedEverywhere`, as `folder show` counts them), size, source node, each node's state and the conflict copies' names. 404 if this node doesn't keep it (`this node doesn't keep folder <name>`) or there is no view yet (`no view of folder <name> yet`). |
| `GET /api/transfers` | Each active swarm: folder, path, content hash, size, piece count, pieces held, peers (choked or not, interested, rate, open requests, snubbed), whether stuck; this node's uploads per peer (unchoked, rate); the choker's unchoked set and optimistic unchoke. New snapshot functions on the swarm and choker types (plan step 7). |

**As built (plan step 7):** `TransfersAnswer` = `swarms` and `uploads`. A swarm (`SwarmInfo`): folder name (its doc ID if this node no longer keeps it), path, content hash, size, piece length, piece count, `have` (verified), `active` (started, not verified), `stuck`, and every node the download asks: `state` `In`, `Joining`, `Out` (asked again every 2 s) or `Banned` (not asked again in this download), and for `In` only: unchoked, interested, rate, open requests, snubbed, how many pieces it has. **Stuck:** no peer in the swarm both unchokes this node and has a piece it lacks. An upload (`UploadInfo`, one per peer with swarm streams to this node): streams, interested, unchoked, optimistic. The unchoked set and the optimistic unchoke are these flags; there is no separate list. Rates are bytes per second over the last 5 s, as whole numbers. Peers carry a `label`.

`Picker::held`, `Picker::active`, `Picker::have_count` and `Choker::optimistic` are new getters. |

**Snapshots, always published (2026-10-22):** an observer must not change what the download loop or the serving path does, or when. So the writers publish a snapshot whether anything reads it or not, and a reader never takes a lock a writer takes:

* **Downloads:** the download state is local to `Client::download`. Once a second at most (`SNAPSHOT_EVERY`), at the top of its loop, the download builds its view (peers, `have` and `active` from its picker, `stuck`) and stores it in its `Swarms` entry, an `arc_swap::ArcSwap`. The reader loads the last view (`load_full`): it never blocks the loop. The reader doesn't read the `have` watch, whose borrow would block `send_modify` when a piece is verified.
* **Uploads:** the serving path keeps the uploads mutex (the choker needs it). The choker's task builds the upload rows under that mutex once a second, always, and stores them in an `ArcSwap`; the reader loads them. The mutex is then taken the same number of times with or without a page.
* The `Swarms` map keeps its mutex: the reader holds it only to clone the entries, and only a download's start and end and a handshake take it, not the loop.
* Data is up to 1 s old, plus the 2 s `TOPIC_INTERVAL`. The fixed cost on every node, page or not: one view per download and one upload list per second (microseconds). What a page still costs is CPU on the same runtime (the topic read, JSON, the socket), limited by `TOPIC_INTERVAL` and outside the loop.
* **Rejected: the reader asks the loop** (a request channel as a `select!` arm, answered with a fresh view). No mutex, but the loop then runs a branch only when observed; the new arm is polled on every iteration, also with no page; the reader waits until the loop reaches `select!` (it may be in a QUIC send or a hash check); a blocked loop can't answer, so the view goes blind just when there is a fault; `UiBackend::transfers` would turn async; and the uploads have no loop to ask.

**Actions:**

| Route | Body | Answer |
|---|---|---|
| `POST /api/group` | `{}` | 200 `{ "group": <id> }`. Creates a group (`Node::create_group`). 409 if in a group. |
| `POST /api/join` | `{ "ticket": <invite ticket or member ID> }` | 200 `{ "group": <id>, "founder": <id> }`. Waits until the join is done (`Node::join`), at most 60 s (then 504; the join goes on). 409 if in a group. |
| `POST /api/invites` | `{ "hours": <n, default 24>, "name": <optional> }` | 200 `{ "ticket": <text> }`, as `invite create`. |
| `POST /api/members` | `{ "id": <node ID>, "name": <optional> }` | 202 `{ "queued": true, "within_secs": 5 }`. Validated first (`check_name`, in a group), then queued with `member::queue_add`, as `member add`. |
| `POST /api/members/remove` | `{ "id": <node ID> }` | 202, as above. Validated first (in a group, `check_removable`: not the founder, not this node; a member in this node's view), then queued with `member::queue_remove`, as `member remove` (ADR 0006). |
| `POST /api/group/name` | `{ "name": <name> }` | 202, as above. Validated first (`check_name`, in a group), then queued with `member::queue_group_name`, as `group name`. |
| `POST /api/folders/{name}/enabled` | `{ "enabled": <bool> }` | 202, as above. Validated first (in a group, a folder this node keeps), then written to the `disabled-folders` file with `folder::set_enabled`, as `folder enable|disable`, for the running node to apply. |
| `POST /api/folders/{name}/remove` | `{}` | 202, as above. Validated first (in a group, a folder this node keeps), then queued with `folder::queue_remove`, as `folder remove`. The running node stops the folder's tasks, deletes this node's `folder-nodes` key for it (membership is untouched), leaves its doc (kept on disk, with this node's entries), forgets it locally, and removes only its own files from the directory (the marker, staging files, the journal). |
| `POST /api/folders` | `{ "name": <folder name>, "path": <directory on this node> }` | 202 `{ "queued": true, "within_secs": 5 }`. Validated first (`check_folder_name`, path absolute (unlike the CLI: the node's working directory means nothing on the page), canonicalized and a directory, in a group), then queued with `folder::queue_add`, as `folder add`. |

Queued actions apply within one presence interval (5 s). The page says so; the UI topics show the result.

**As built (plan step 6):** the handlers are in `ui/actions.rs`. The in-group check reads the backend's state (not the group file); a `GroupError` from the node (`AlreadyInGroup`, `NotInGroup`) also gives 409 with its text. A body that isn't the request's JSON gives 400 with axum's text. Invites carry only this node's ID, as `invite create`. `POST /api/group` and `POST /api/join` run their action as a task (`Topics::spawn`) that finishes even if the request goes (a closed tab, the 60 s limit), since stopped half-way it could leave the node in a group without its monitor; at Ctrl-C, `Topics::stop` ends it (the request gets 503 `the node is stopping`), and the next `run` resumes the group.

**UI topics** (ADR 0005): the page doesn't poll. It opens one WebSocket and subscribes to the UI topics of the shown view.

* `GET /api/ws` upgrades to the WebSocket. Besides the `Host` check (§3), the upgrade requires an `Origin` of the UI address: `http://<addr>:<port>`, or `http://localhost:<port>` for a loopback address. Else 403, also when `Origin` is missing.
* A UI topic is named as its read route without `/api/`: `status`, `folders`, `folders/<name>`, `transfers`.
* The page sends `{ "subscribe": "<topic>" }` and `{ "unsubscribe": "<topic>" }`.
* The server sends `{ "topic": "<topic>", "update": <update> }`. The update is `{ "kind": "Status", "data": <StatusAnswer> }`, `{ "kind": "Folders", "data": [<FolderInfo>] }`, `{ "kind": "Folder", "data": <FolderViewAnswer> }`, `{ "kind": "Transfers", "data": <TransfersAnswer> }` or `{ "kind": "Error", "data": { "error": <text> } }`, the same data or error text as the read route. An unknown topic gets an `Error` update once.
* On subscribe, the server sends the topic's latest data at once, then whenever it changes.
* **One timer per UI topic, only while subscribed.** The first subscription to a topic, on any connection, starts its timer; when the last one goes (unsubscribe or closed socket), the timer stops. With no subscription, there is no timer and no UI work. Each tick (`TOPIC_INTERVAL`, 2 s) reads the data as the read route does, releases every lock, serializes, and sends to every subscription only if the JSON differs from the last send. A slow connection skips to the latest data; old data is never queued.
* Actions stay `POST` (above).

**Types:** the API's request and answer structs, and the WebSocket messages, derive `ts-rs` `TS` with `#[cfg_attr(test, derive(ts_rs::TS))]`, so `ts-rs` stays a dev dependency. The lib types they hold (`LinkKind`, `LinkState`, `Traffic`) do the same. The unit test `ui::tests::export_api_types` writes one file per type to `ui/src/api/` (`just ui-types`; `just check` also runs it). `u64` becomes `number`, not `bigint`. Only generated files go in `ui/src/api/`: the test deletes every `.ts` file there first. `just ui-check` writes the types to a temp dir and fails when they differ from `ui/src/api/`. The page reads through `ui/src/lib/socket.ts` only (`live(topic, kind)` for a view, `once(topic, kind)` for the first view's choice); the `POST` actions go through `ui/src/lib/api.ts` (§5).

#### 5. The backend seam

The handlers take a `UiBackend` trait object, not a `Node`:

* `id()`, `status()`, `folders()`, `folder(name)`, `transfers()`, `label(id)` (name or short ID, `Node::display`), `create_group()`, `join(ticket)`. `create_group` and `join` return a boxed future (`ui::BoxFuture`), since the trait is used as an object.
* The running node implements it (`impl UiBackend for Node`). The tests implement a fake.
* The page's `POST` client is `ui/src/lib/api.ts`; `lib/action.ts` keeps an action's state (running, answer, error) for a view.
* Invites and queued writes use the config dir only (`invite::create`, `member::queue_add`, `folder::queue_add`), so the router gets the config dir too. In tests, a temp dir.
* The router is built by one function, `ui::router(backend, config)`, so tests call it with `tower::ServiceExt::oneshot`: no port, no node.
* The UI topics have their own type, `ui::topics::Topics`, shared by every connection of one router, with an interface that doesn't need a socket: `subscribe(name)` gives a `Subscription` (or the `Error` message for an unknown topic), whose `next()` gives the latest message and every change after it; dropping it unsubscribes. A message is serialized once and shared by every subscription. The socket handler only moves messages between it and the WebSocket. The tests use it directly.
* `ui::router(backend, config)` returns the router and its `Topics`; `run` calls `Topics::stop` before the server's graceful shutdown (§2, Stopping). The timers and socket loops run in a `TaskTracker`, and `stop` waits for them (`tokio-util`, already in the tree through iroh).

#### 6. Views

One page: a sticky header (app icon and `sync-node`) and a rail on the left to move between the views. The rail collapses to its icons and expands to icons and labels (kept per browser in `localStorage`). Changed 2026-10-24 from four views in a nav bar.

1. **Dashboard** (`/`): in no group, a link to Setup. In a group: stat tiles (members online, members connected and how many direct or relay, bytes sent and received in total), then the group's card: name (rename in a dialog), this node and the group ID, and its sections:
    * **Members:** name or short ID, online, connection (direct or relay), traffic in total (the direct and relay split on hover), added by. Rename and remove each open a dialog; removing names the members that go with it (ADR 0006).
    * **Removed**, if any, and **Links:** each pair's link.
    * **Folders:** the folders this node keeps (open one at `/folders/<name>` for its merged view) and the group's folders it doesn't keep yet (`available` in `/api/status`). "Keep here" turns an available folder's row into a directory input; "New folder" adds a row with a name and a directory. Both queue `POST /api/folders`. A switch in each kept folder's row enables or disables it on this node (`POST /api/folders/{name}/enabled`, as `folder enable|disable`); it shows the asked-for state until the node applies it. A remove button opens a dialog and queues `POST /api/folders/{name}/remove` (as `folder remove`): this node stops keeping the folder, as before it was added; its files stay.
2. **Transfers:** the developer view of §4 `GET /api/transfers` (UI topic `transfers`). One card per swarm with a Progress bar of verified pieces and a table of its peers; one table of uploads.
3. **Setup:** in no group: create a group, or join with a ticket or member ID. In a group: make an invite (hours, name) and show the ticket to copy; add a member (ID, name).

**Live data:** each view subscribes to its UI topics while shown, and unsubscribes when it goes. While the socket is down, the view keeps its last data and shows that the node can't be reached; the page connects again every 2 s and subscribes again.

**Navigation:** TanStack Router (`@tanstack/solid-router`) moves between the views. Each view has its own path, so a view can have a bookmark and the browser's back button works. The server's `index.html` fallback (§4) lets a reload on any view path work.

#### 7. Front end and build

* Source in `ui/` at the repo root, outside the cargo workspace: SolidJS, TypeScript, Vite (`vite-plugin-solid`), TanStack Router (`@tanstack/solid-router`), Tailwind v4 (`@tailwindcss/vite`). Linter: ESLint with `typescript-eslint` and `eslint-plugin-solid`. TypeScript stays on 6 until `typescript-eslint` supports 7. **bun** as package manager and runner, `ui/bun.lock` committed. `ui/node_modules` and `ui/dist` are git-ignored.
* **Components: shadcn-solid only.** Every component on the page is a [shadcn-solid](https://github.com/hngngn/shadcn-solid) component (the community SolidJS port of shadcn/ui, on Kobalte; the official shadcn/ui is React-only). No hand-made components: views put shadcn-solid components together, with plain elements and Tailwind classes for layout and text only. A link that looks like a button uses `buttonVariants`, as in shadcn.
    * There is no CLI: copy a component from `apps/docs/src/registry/ui/` when a view first needs it, into `ui/src/components/ui/`, and change `@/registry/lib/` to `@/lib/`. Helpers from `registry/lib/` go to `ui/src/lib/`. Keep the copied files as they are (their own code style); changes go in the view.
    * Copied from commit `4c75b89` (2026-02-13). The theme in `ui/src/index.css` is the port's installation style (Tailwind v4, `tw-animate-css`, dark mode by `data-kb-theme="dark"`).
    * Packages: `@kobalte/core`, `cva` (`1.0.0-beta.4`, as the port uses), `tailwind-merge`, `tw-animate-css`. `@/` is `ui/src/` (tsconfig `paths` and a Vite alias).
* Icons: `lucide-solid`, one import per icon (`lucide-solid/icons/<name>`), so the build holds only those used. Added 2026-10-24.
* **rxjs** for the UI topics: one `webSocket` subject from `rxjs/webSocket` for the page, one `multiplex` per topic, `retry` with a 2 s delay to connect again; `live` turns a topic into a store (the last data, and the error since, if any), updated with `reconcile` by `id` so rows keep their identity and their state across updates, and it unsubscribes when the view goes. A clean close (the node stopping, code 1001) completes the stream, a failure errors it; both connect again after 2 s.
* The dev container gets bun as a devcontainer feature (the developer rebuilds the container).
* `just ui`: `bun install` and `vite build` into `ui/dist/`.
* `just ui-dev`: the Vite dev server, with a proxy from `/api` to a running `sync-node run --ui-addr …` (the proxy rewrites `Host`, `changeOrigin`; for the WebSocket, `ws: true` and `Origin` set to the node's UI address). The usual workflow in the dev container.
* `just ui-check`: `tsc --noEmit`, the linter, and the ts-rs freshness check. `just check` stays Rust-only and needs no bun.
* `just armv7`: runs `just ui` first, builds the node in Docker, and copies it to `~/sync-node` and `ui/dist/` to `~/ui/` on the Pi (`rsync --delete`, so old asset files go). `~/ui` is the default UI dir there: `run --ui-addr …` needs no `--ui-dir`. `Dockerfile.armv7` doesn't change.
* New Rust crates: `axum` (with the `ws` feature, which brings `tokio-tungstenite`), `serde_json`, `tower`, `tower-http`, `ts-rs` (dev only), `arc-swap` (already in the tree through `iroh-dns`). They can affect the armv7 build: the developer checks it.

#### 8. Tests

* Rust, `tests/ui.rs`: the router with a fake `UiBackend` and a temp config dir, through `oneshot`. Every route: answer code and JSON; the errors (400, 404, 409, 415, 403); a queued member and folder land in the queue files; `UI not found` when the UI dir is missing.
* UI topics without a socket, with the fake backend and paused tokio time: no reads with no subscription; two subscriptions share one timer (one read per tick); the timer stops after the last subscription goes; an unchanged answer isn't sent again; a new subscription gets the latest data at once; an unknown topic and a failing read (a folder this node doesn't keep) give an `Error` update. The messages' JSON forms. The `/api/ws` `Origin` check through `oneshot` (403 for a missing or foreign `Origin`).
* The socket loop is not tested in Rust: the developer checks it in the browser.
* No test starts a node or opens a port. No front-end component tests in the first version.

#### 9. Not in this plan

* Auth (parked), and so a recommended LAN bind.
* History, graphs, SSE.
* Change events from the node for the UI topics (each topic is read on a timer for now; ADR 0005).
* The CLI using the API (`join` and `create` while `run` runs). The CLI stays as it is.
* ADR 0003 (own index exchange): its own plan, after this one.
