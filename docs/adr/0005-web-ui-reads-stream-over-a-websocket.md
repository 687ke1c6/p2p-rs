---
status: accepted
---

# The web UI's reads stream over one WebSocket, by UI topic

The page doesn't poll the read routes. It opens one WebSocket (`/api/ws`) and subscribes to UI topics, named as the read routes (`status`, `folders`, `folders/<name>`, later `transfers`). The server sends a topic's data when it changes. The node has no change events for this data yet, so the server reads it on a timer, but only while some page subscribes to the topic, and one timer per topic serves every connection. Actions stay `POST` requests, and the `GET` read routes stay.

Decided 2026-10-22, with the developer, after plan step 4 (the polling views) was validated. Replaces "the page polls" from the UI spec.

## Why

- One connection per page instead of one request per view every 2 s, and nothing on the wire while the data doesn't change (folders, folder views).
- The Transfers view (swarms, pieces, rates) is a stream by nature.
- The protocol doesn't change when a topic gets real change events later: only the server's source for that topic does.
- With no page open, the node does no UI work at all; with many open, the work is the same as with one.

## Considered Options

- **Polling the `GET` routes** (plan steps 2 to 4): simple, and `curl` works. Each view asks every 2 s whether or not anything changed, and each open page multiplies the work.
- **Server-sent events**: one stream per subscription, or a fixed set per page; the page can't add or drop topics on one stream without a second channel for that.
- **One WebSocket with UI topics** (this ADR).

## Consequences

- A WebSocket isn't covered by CORS: a page on any site can open `ws://127.0.0.1:<port>/api/ws`, and its `Host` header passes the `Host` check. So the upgrade also requires an `Origin` of the UI address (`http://<addr>:<port>`, or `http://localhost:<port>` for a loopback address). The dev server's proxy sets it.
- The poll timers live in the node's tokio runtime (ADR 0004): a timer reads the same snapshot as its `GET` route, releases every lock, serializes, and sends only if the JSON differs from the last. A slow page gets the latest data, never a queue of old data.
- The socket loop isn't tested by the Rust tests (no test opens a port). The tests cover the UI topics without a socket, the messages, and the `Origin` check; the developer checks the socket in the browser.
- `rxjs` (`rxjs/webSocket`, `multiplex`) on the page; the axum `ws` feature on the server (a new crate for the armv7 build).
