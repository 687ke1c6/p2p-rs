# Progress Report (2026-10-22)

Handoff for the next session. Read this first, then [plan.md](plan.md) and [spec.md](spec.md).

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

## Decisions 2026-10-22

* Components: shadcn-solid only (copied, no CLI). Rejected: React with the official shadcn/ui; the solid-ui port (Tailwind v3 docs).

## Notes for step 3

* The API structs are in `ui.rs` and hold IDs as strings. Three lib types go into the JSON as they are: `LinkKind`, `LinkState` (serialized as `"Direct"`, `"NotConnected"`, …) and `Traffic`. They need `TS` too.
* `ts-rs` as a dev dependency can't derive on lib types for `tests/`: integration tests build the lib without `cfg(test)`. Options: a cargo feature (`ts`) that the type test turns on, or `ts-rs` as a normal dependency.
* `run` holds the node in an `Arc` for the server. At Ctrl-C the server stops with graceful shutdown, then `Arc::try_unwrap` gets the node back for `shutdown`. A long request (the 60 s join of step 5) delays the stop.
* With `cargo run`, the default UI dir is `target/debug/ui`. Use `--ui-dir ui/dist` or `just ui-dev`.

## Developer preferences

* ASD-STE100 in chat (CLAUDE.md); never touch the git index; never run the armv7 build.
* Write down how a thing must work before building it; review the spec first.
