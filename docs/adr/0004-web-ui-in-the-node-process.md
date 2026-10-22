---
status: accepted
---

# The web UI runs in the sync-node process

The web UI is served by `sync-node run` itself: an axum server on the UI address (`--ui-addr`, off by default) with a JSON API under `/api` and the built page (SolidJS) from the UI dir. The browser is the only other process. There is no second binary.

Decided 2026-10-21, with the developer, before the UI plan ([plans/ui/](../../plans/ui/plan.md)).

## Why

- `run` holds the stores open, so no other process can read them (the reason `status` is a file today). A separate front-end process would still need an API in the node. Building that API is the work in both designs.
- In the process, the API reads live state (`Status`, peers, swarms, choker) on request, not a snapshot written once each presence interval.
- One binary to deploy to the Pi, and no version contract between two programs. If a separate client is needed later, it calls the same JSON API.
- `hyper`, `http` and `tower` are already dependencies through iroh; axum adds little.

## Considered Options

- **In the process** (this ADR).
- **Separate process** that serves the page and calls an API in the node. Needs the same API, plus a second binary to build for armv7 and keep compatible.
- **Files only**: the node writes `status.json` each interval, a separate tool reads it. No server in the node, but data only as fresh as the last interval, no details on request, and no actions.

## Consequences

- The UI shares the node's tokio runtime. The load tests (`tests/starved.rs`) show the doc layer is sensitive to a busy runtime, so handlers copy a snapshot, release the lock, and only then serialize. No handler holds a lock across an `.await`.
- The UI takes actions (join, create a group, invites, members, folders), not only reads. Auth is parked for now: the UI address is loopback by default, and the server refuses an unknown `Host` and a write without `Content-Type: application/json`. A LAN bind needs the auth decision first.
- The page is not embedded in the binary; it is served from the UI dir, so the page and the binary can be of different builds. They are always built together (`just armv7` builds both).
