> **Superseded (2026-10-15).** We are not building our own QUIC layer. The transport moves to iroh: see the **Concluded** section of [prototype-plan.md](prototype-plan.md) and [sync-plan.md](sync-plan.md). This plan is kept for its reasoning. Its identity analysis (the rendezvous server knows the token, so trust must be pinned outside it) still applies to how peers are paired.

### Phase 2: QUIC Over the Punched Socket

* **Builds on:** [prototype-plan.md](prototype-plan.md) (complete). Rendezvous, STUN, NAT detection and direct-only punching stay as they are.
* **Objective:** Replace the prototype's hand-rolled datagram protocol (`crates/client/src/session.rs`) with QUIC running over the punched UDP socket. This gives encrypted, authenticated, congestion-controlled connections with streams, so payloads are no longer limited to one 1024-byte datagram.
* **Unchanged constraint:** Direct only. QUIC runs over the socket we punched and nothing else. The rendezvous server still sees signaling only.

### Why QUIC

The prototype protocol has no encryption and no peer authentication: after punching, any datagram from the peer's address carrying the peer's id is accepted. It is also stop-and-wait with one payload in flight and no congestion control. Adding these by hand would rebuild a large part of QUIC. QUIC also manages packet sizes itself (starting at 1200 bytes, with path MTU discovery), which removes the single-datagram limit.

### Library Choice

* **`quinn`** for QUIC, **`rustls`** for TLS 1.3 and **`rcgen`** for self-signed certificates. Pin exact versions, as with `rust-p2p-core`.
* **Crypto provider: `ring`, not `aws-lc-rs`.** The client is cross-compiled for armv7 (`just client-armv7`), and `ring` cross-compiles without the CMake/NASM toolchain `aws-lc-rs` needs. Check that the armv7 build still passes after adding the dependencies.

### Design Decisions

#### 1. Socket: a quinn adapter over `UdpTunnel`

`rust-p2p-core` owns the punched socket and exposes no way to take it out. It does expose `send_to` and `recv_from` on `UdpTunnel`. Quinn accepts a custom socket through its `AsyncUdpSocket` trait (`Endpoint::new_with_abstract_socket`), so we implement that trait on top of the tunnel.

* Once the punch round trip succeeds, the endpoint's socket belongs to quinn. The prototype's `PeerMessage` datagrams other than `PunchRequest`/`PunchResponse` are retired.
* Stray datagrams (late STUN responses, leftover punch packets) reach quinn too. Quinn drops anything that doesn't parse as QUIC, so no demultiplexing is needed. Note that the `rp2p` magic can't be used to tell our datagrams from QUIC's: its first byte (`0x72`) is a valid QUIC short-header first byte.
* Rejected: binding a second socket to the same port (`SO_REUSEPORT`), which makes delivery between the two sockets unpredictable, and replacing `rust-p2p-core`'s socket with our own, which would also mean replacing the `Puncher`.

#### 2. Identity: device keys, pinned by fingerprint

The token is shared with the rendezvous server (it is how peers register), so anything derived from it can't protect against a malicious or compromised server. Peer identity therefore has to be pinned outside the rendezvous path.

* Each device generates a persistent key pair and self-signed certificate on first run, stored in its config directory. Its **device ID** is the SHA-256 of the certificate, printed in a readable form.
* The user tells each device which device IDs to trust (`--peer <device-id>` or a trusted-peers file). This is the same model as Syncthing's device IDs.
* Both sides authenticate (mutual TLS). A custom rustls verifier on each side accepts only a certificate whose hash matches a trusted device ID, and ignores certificate chains and hostnames.
* A connection to an untrusted device fails the TLS handshake and is reported as such. It never falls back to an unauthenticated connection.

#### 3. Roles

QUIC needs a client and a server. Reuse the existing ordering: the newer peer (higher id), which sends `PunchStart`, dials; the older peer accepts. Both sides already know which they are after the exchange.

#### 4. Keepalive and timeouts

Quinn's `keep_alive_interval` (10s) replaces the `Keepalive` datagram, keeping NAT mappings open. Its `max_idle_timeout` (45s) replaces the "peer went silent" check. Rendezvous `Ping`s continue as before.

---

### Ordered Implementation Steps

Same rules as the prototype: work in order, one commit per step, tick a box only when its "Done when" holds.

#### Step 1: Quinn Socket Adapter (Decision 1)

* [ ] Add `quinn`, `rustls` (with `ring`) and `rcgen`, pinned. Confirm `just client-armv7` still builds.
* [ ] Implement `AsyncUdpSocket` over `UdpTunnel`. `recv_from` takes `&mut self`, so a task likely owns the receiving half and feeds quinn through a channel. Record any datagrams dropped there in debug logs.
* [ ] Test: two endpoints on loopback, each wrapping a `UdpTunnel`, open a QUIC connection and echo a bidirectional stream. A temporary accept-any certificate verifier is fine for this step only, behind `#[cfg(test)]`.
* **Done when:** the loopback stream test passes and a STUN-shaped datagram sent to the socket mid-connection doesn't break it.

#### Step 2: Device Identity & Pinned Mutual TLS (Decision 2)

* [ ] Generate and persist the device key and certificate on first run. Add `p2p-client id` to print the device ID.
* [ ] Add `--peer <device-id>` / a trusted-peers file, and the rustls client and server verifiers that check the certificate hash.
* [ ] Tests: trusted peers connect; a peer with an unknown certificate is refused on both sides; a missing or unreadable key file produces a clear error rather than silently generating a new identity.
* **Done when:** connections succeed only between devices that trust each other's IDs, in both directions.

#### Step 3: QUIC After Punching (Decisions 3, 4)

* [ ] After the punch round trip, hand the endpoint to quinn: the newer peer connects, the older accepts, with the peer's punched address and pinned ID.
* [ ] Configure keepalive and idle timeout. Remove `Payload`, `PayloadAck`, `Keepalive` and `Bye` from `PeerMessage`, and retire the datagram session code.
* [ ] Integration test: two clients exchange through an in-process rendezvous server with fake STUN servers, punch, and complete a QUIC handshake.
* **Done when:** `p2p-client connect` reports an authenticated QUIC connection between two instances on one machine, and through the Railway rendezvous server.

#### Step 4: Messages & Files Over Streams

* [ ] Keep the CLI (`listen`, `send --message | --file`). Each transfer is one stream: a small header (kind, name, size, SHA-256) then the body. The receiver checks the whole-file hash and answers on the same stream.
* [ ] Keep the prototype's receive rules: plain file names only, never overwrite an existing file. Write to a temporary file and rename it into place only once the hash matches.
* [ ] Tests: a message and a multi-megabyte file round trip; a hash mismatch is reported and leaves no file behind; an unsafe name is refused; the listener serves peers one after another.
* **Done when:** a file of several megabytes arrives intact (matching SHA-256) between two instances on one machine.

#### Step 5: Cross-Network Verification

* [ ] Repeat the prototype's step 9 setup (dev container and Raspberry Pi on different networks, Railway rendezvous, public STUN) with QUIC.
* [ ] Transfer a large file (e.g. 100 MB) and log throughput and any path-MTU problems. Consider KCP only if QUIC can't get packets through.
* **Done when:** the large file arrives intact across the two networks over an authenticated direct connection.
