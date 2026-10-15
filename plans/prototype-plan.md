### Project Outline & Intention

> **Status: complete (2026-10-15).** Two clients on different networks, each behind its own NAT, connected directly through the Railway rendezvous server and public STUN, and exchanged test payloads. Testing on a real hostile network (symmetric NAT) was left out of scope; the abort paths are covered by tests. The project then moved to iroh; see **Concluded** at the end, and [sync-plan.md](sync-plan.md) for the next phase.

* **Project Name / Concept:** Direct P2P File & Folder Sync (Working Title)
* **Core Intention:** Build a lightweight, sovereign, local-first file backup and synchronization tool in Rust. The primary constraint is **strict direct peer-to-peer connectivity**—no fallback data relays. If a direct NAT hole-punch cannot be established between peers, the connection fails entirely, ensuring data never routes through third-party infrastructure.
* **Phase 1 Objective:** Create a minimal Rust prototype using `rust-p2p-core` to establish and verify direct TCP/UDP hole-punched sockets across known network environments.

### Library Choice: `rust-p2p-core`, not `rustp2p`

The `rustp2p` project publishes two crates. We use the lower-level one.

* **`rustp2p` (rejected):** A self-organising mesh. Nodes forward packets addressed to other nodes, and `send_to` silently routes through an intermediate node when no direct route exists. Relaying is a headline feature, which breaks the direct-only constraint. Turning it off would mean fighting the library: an interceptor on every node plus direct-route checks before every send. It also queries public STUN servers (Google, bilibili and others) by default.
* **`rust-p2p-core` (chosen):** Only sockets, NAT detection (`NatInfo`, which can be serialized with serde) and a `Puncher` that hole-punches toward a peer's `NatInfo`. It has no routing and no forwarding, so data can only travel over a socket we punched ourselves. Its `examples/server.rs` and `examples/client.rs` are the starting templates for our server and client.
* **Caveats:** The crate is pre-1.0 with few users, and its docs are thin and sometimes contradict the code. Pin an exact version (`=0.4.1` at the time of writing) and read the source rather than trusting the docs. Our dependency is limited to `Puncher`, `NatInfo` and the socket types, so replacing it later would be manageable.

---

### Functional & Technical Requirements for the Rust Prototype

#### 1. Signaling & Discovery (Rendezvous)

* [x] **Lightweight Signaling Server:** Implement or deploy a minimal rendezvous server responsible *only* for metadata exchange (trading IP addresses and ports), handling zero file data.
* [x] **Peer Handshake:** Allow two client instances to register with the signaling server, discover each other via unique share/device tokens, and trade connection candidates.
* [x] **Rendezvous and STUN Are Separate Services:** Even if one process provides both, the client treats rendezvous and STUN as two independent services, each with its own `host:port`. Hosting providers like Railway give every exposed port its own hostname (e.g. `shuttle.proxy.rlwy.net:15140`), so the client must never assume both services share a host or derive one address from the other.
* [x] **STUN Service (Address Reflection):** STUN tells each client its public address: the source IP and port of the client's UDP packet as seen from the internet. The client speaks standard RFC 5389 STUN Binding requests, not a custom message. It uses its own small Binding client (`crates/client/src/stun.rs`) rather than `rust-p2p-core::stun`, which probes from throwaway sockets of its own (so its mappings don't apply to the punching socket), doesn't check transaction ids, and sends the RFC 3489 `CHANGE-REQUEST` attribute. We use existing public STUN servers instead of hosting our own (see the decision below).
* [x] **NAT Type Detection:** The client sends Binding requests to at least two STUN endpoints from the same UDP socket and compares the mapped ports. Using servers at different IPs also catches NATs that change the mapping only when the destination IP changes. The same mapping means a cone NAT, different mappings mean a symmetric NAT. The result fills `NatInfo.nat_type`, which the `Puncher` uses to choose its strategy. With only one STUN endpoint configured, the client logs a warning and assumes cone NAT.
* [x] **Client Endpoint Configuration:** Each service address is set by CLI flag or environment variable. Rendezvous is required. STUN has public defaults.
  * `--rendezvous <host:port>` / `RENDEZVOUS_ADDR`: one TCP endpoint.
  * `--stun <host:port>` / `STUN_ADDRS` (comma-separated): one or more UDP endpoints. Defaults to a small list of public servers from different providers, `stun.l.google.com:19302`, `stun.cloudflare.com:3478` and `global.stun.twilio.com:3478`. (`stun1.l.google.com` was dropped: it resolves to the same IP as `stun.l.google.com`, so it can't reveal per-destination remapping.) Setting the option replaces the defaults instead of adding to them.
  * Each hostname is resolved independently at startup, and every resolved address is logged. A failure to resolve or reach one service is reported with that service's name.
* [x] **Explicit STUN List:** The client contacts only its own STUN list, either the defaults above or the user's override. It does not use `rust-p2p-core`'s built-in default list, which includes servers we haven't chosen.

> **Hosting constraint (Railway):** Railway does not accept inbound UDP, and its TCP proxy hides the client's source IP. The rendezvous server can run on Railway over TCP, because signaling only needs to pass messages. **STUN cannot run on Railway**, because it must receive UDP and see the client's real source address. **Decision: use public STUN servers** for the prototype and probably beyond, rather than host infrastructure that already exists. Trade-off accepted: the STUN providers see each client's public IP and port when it starts up. They never see the peer, the token or any file data, so the "data never routes through third-party infrastructure" constraint still holds. Because the client keeps STUN separate, self-hosting later would only be a configuration change.

#### 2. Network Transport & Connection Constraint

* [x] **Direct-Only Enforcement:** Use the `rust-p2p-core` `Puncher` with the peer's exchanged `NatInfo` to attempt a direct peer-to-peer connection. A connection counts as established only after a punch request/response round trip over the punched socket succeeds. The server never forwards anything except signaling messages, so there is no path for data to be relayed.
* [x] **Early Abort:** If both peers report symmetric NAT, fail immediately with a clear "direct connection not possible" error instead of waiting for punch attempts to time out.
* [x] **Strict Failure Handling:** If the NAT traversal fails (e.g., hostile symmetric NATs), the app must gracefully abort the connection attempt rather than routing through a fallback relay.
* [x] **Connectivity Verification:** Log and verify successful raw socket connection establishment between machines across different local networks.

#### 3. Prototyping Scope & Next Steps

* [x] **CLI Harness:** Build a simple command-line interface for the prototype to trigger the connection handshake, accept incoming peer requests, and send a test payload (e.g., a simple text message or small file chunk) over the established P2P socket.
* [x] **Repository Setup:** Initialize a Git repository to house the prototype code, structuring it cleanly for future expansion into a local background daemon and file watcher.

#### 4. Development Environment

* [x] **Rust Dev Container:** All development happens inside a Rust dev container (`.devcontainer/devcontainer.json`, based on the official Rust dev container image or a Dockerfile pinning the Rust toolchain). It should include `rustfmt`, `clippy`, and `rust-analyzer`, so builds and tooling are reproducible across machines. Commit the dev container config to the repository.
* [x] **Host Networking:** The dev container runs in host network mode (`"runArgs": ["--network=host"]` in `devcontainer.json`), so it shares the host's network stack. This avoids an extra Docker NAT layer that would distort NAT traversal and hole-punching tests. Note that on Docker Desktop (including WSL2 setups using it) host mode behaves differently from native Linux, so verify the container sees the real host interfaces before trusting test results.

#### 5. Running the Rendezvous Server

The rendezvous server is deployed with Docker. It handles metadata only (addresses and ports) and never touches file data. It is TCP-only, so it can run behind Railway's TCP proxy. STUN uses public servers, so there is nothing to deploy for it.

* [x] **Dockerfile:** Create a `Dockerfile` for the rendezvous server in the repository. Use a multi-stage build: a Rust builder stage that compiles the server binary in release mode, and a minimal runtime stage based on `alpine` that copies in only the binary. Alpine uses musl libc, so build in a `rust:alpine` builder stage (or target `x86_64-unknown-linux-musl`) so the binary links against musl and runs in the runtime image. Fall back to `debian:bookworm-slim` only if a dependency doesn't build against musl. Run as a non-root user and expose the signaling port.
* [x] **`.dockerignore`:** Exclude `target/`, `.git/` and `.devcontainer/` to keep the build context small.
* [x] **Build the Image:** `docker build -t rendezvous-server .`
* [x] **Run the Container:** `docker run -d --name rendezvous --restart unless-stopped -p <PORT>:<PORT> rendezvous-server`. Configure the listen address and port through environment variables or CLI flags.
* [x] **Ports:** The server listens on one TCP port, read from `PORT` (which Railway sets) or a `--listen` flag.
* [x] **Railway Deployment:** Deploy from the Dockerfile and enable a TCP proxy on the listen port. Railway assigns a hostname and port such as `shuttle.proxy.rlwy.net:15140`, which becomes the client's `--rendezvous` value. Because the proxy hides client source IPs, the server must never use the connection's remote address as a candidate. Clients report their own candidates from STUN.
* [x] **Verify:** Check the container logs, then confirm that two clients can register and exchange candidates through the server.

---

### Ordered Implementation Steps

Work through these in order. Each step is a self-contained chunk that ends in a verifiable result, ideally one commit per step. Tick a box only when the step's "Done when" condition holds. Section numbers refer to the requirements above.

#### Step 1: Repository & Dev Container (Sections 3, 4)

* [x] `git init`, add `.gitignore` (`target/`) and a Cargo workspace with placeholder crates for the client, the rendezvous server and shared protocol types.
* [x] Add `.devcontainer/devcontainer.json` with the Rust image, `rustfmt`, `clippy`, `rust-analyzer` and `--network=host`.
* **Done when:** the project reopens in the container, `cargo build`, `cargo fmt --check` and `cargo clippy` pass, and the container sees the host's network interfaces.

#### Step 2: Shared Protocol Types

* [x] Add `rust-p2p-core` (pinned to an exact version) as a dependency of the protocol crate.
* [x] Define the signaling messages in the shared crate, using `rust-p2p-core`'s `examples/server.rs` as a reference:
  * Messages are typed by direction (`ClientMessage`, `ServerMessage`, `PeerMessage`), so the server can only decode the client-to-server allowlist.
  * `Register { token }` → `Registered { peer_id }` (ids are assigned by the server), then `PeerList { peers }` whenever the group changes.
  * `PunchStart { to, nat_info }` / `PunchReply { to, nat_info }`: the server delivers them to the target peer as `{ from, nat_info }`.
  * `Ping` / `Pong`: a keepalive, because proxies close idle connections.
  * `PunchRequest` / `PunchResponse`: sent directly peer-to-peer over UDP to confirm the hole is open.
  * `Error { reason }`.
* [x] Use a typed serde encoding (e.g. `postcard` or `bincode`) with length-prefixed framing over TCP, instead of the example's raw `u32` header plus JSON. Unknown or malformed messages are rejected, not forwarded.
* [x] STUN is not part of this protocol. The client uses standard STUN Binding requests (Section 1).
* **Done when:** messages round-trip through serialization in unit tests.

#### Step 3: Rendezvous Server (Section 1)

Start from `rust-p2p-core`'s `examples/server.rs`. Keep its peer registration and punch-message passing, but drop its address reflection and UDP listener.

* [x] **Listener:** One TCP port, read from `PORT` or `--listen`.
* [x] **Registration:** Keep a token-to-peers table in memory and remove peers whose connections close or stop sending heartbeats.
* [x] **Candidate exchange:** Forward `PunchStart` (which carries the sender's own `NatInfo`) to the target peer only when both peers are registered under the same token. This is the only message the server passes between peers. The server never adds the connection's remote address to candidates, because behind Railway's proxy that address belongs to the proxy.
* [x] **Allowlist:** Drop every other message type. The server has no code path that forwards arbitrary payloads, so it cannot relay file data.
* [x] **Logging:** Log registrations and punch coordination, using `log` + `env_logger` as the examples do.
* **Done when:** an integration test shows two local test clients registering under the same token and receiving each other's `NatInfo`, and a non-allowlisted message is dropped.

#### Step 4: Rendezvous Server Container & Railway Deployment (Section 5)

* [x] Write the Alpine-based multi-stage `Dockerfile` and `.dockerignore`, then build and run the image locally.
* [x] Add `just` recipes for building and running the image.
* [x] Let the step 3 integration test target a running server via `RENDEZVOUS_TEST_ADDR` (`just test-remote <host:port>`).
* [x] Deploy to Railway with a TCP proxy and note the assigned `host:port`.
* **Done when:** the container runs locally and on Railway, logs show it listening, and the step 3 integration test passes against both.

#### Step 5: Public STUN Selection (Section 1)

* [x] Choose the default STUN list: at least two providers and servers at different IPs.
* [x] Add a `just stun-check` recipe (or a client subcommand) that sends a Binding request to each configured server and prints the mapped address.
* [x] STUN tests: fake-server tests on loopback in `just check` (mapping, retransmit, timeout, noise rejection, per-server failures, cone/symmetric), and live tests against the defaults in `just test-network`.
* **Done when:** every default server returns the machine's real public `IP:port`, checked against a known external IP, and an unreachable server is reported by name without stopping the others. *(Verified 2026-10-15: all three defaults returned the same public IP that ipinfo.io reports.)*

#### Step 6: Client Handshake & NAT Detection (Section 1)

* [x] Client configuration: a required `--rendezvous <host:port>` and optional `--stun <host:port>` overrides of the public defaults (or `RENDEZVOUS_ADDR` / `STUN_ADDRS`). Resolve each hostname independently and report failures per service. The token comes from `--token` / `P2P_TOKEN`.
* [x] Send STUN Binding requests from the socket that will be used for punching, build `NatInfo` (public address, local addresses, cone or symmetric) and log it. The mapping is only valid for that socket. STUN runs over `rust-p2p-core`'s main UDP tunnel (`Endpoint` in `crates/client/src/endpoint.rs`), so the `Puncher` sends from the socket the mapping describes. Detection reruns just before candidates are sent, so a long wait for the peer doesn't hand over a stale mapping.
* [x] Client connects to the rendezvous endpoint over TCP, registers with a token and receives the peer's `NatInfo`. The newer peer (higher id) sends `PunchStart`, the older answers with `PunchReply`, so both never start at once. `Ping` every 20s keeps the connection inside the server's 60s idle timeout.
* [x] Integration test (`crates/client/tests/exchange.rs`): two clients exchange candidates through an in-process server with fake STUN servers.
* **Done when:** two client instances on one machine, pointed at the rendezvous server and the public STUN defaults, complete the candidate exchange. *(Verified 2026-10-15: locally, and through the Railway rendezvous server with the public STUN defaults.)*

> **Decision: UDP only.** The endpoint opens no TCP sockets and sends no TCP candidates. TCP hole punching is hard to get right and unreliable, and it would need the public TCP port, which STUN over UDP can't report. The intended transport over the punched UDP socket is QUIC, with KCP as the fallback if MTU limits become a problem. That is probably the only fallback needed.
>
> **WSL2 adds a NAT layer.** In the dev container on WSL2, `--network=host` reaches the WSL2 VM, not Windows: the reported local address is the VM's (e.g. `172.27.x.x`), and the public port differs from the local port. For step 9, either run the client natively on Windows/Linux, or switch WSL2 to mirrored networking (`networkingMode=mirrored` in `.wslconfig`) and re-check.

#### Step 7: Direct-Only Connection (Section 2)

* [x] Both peers call `Puncher::punch` with the other's `NatInfo` at roughly the same time, then confirm with a `PunchRequest`/`PunchResponse` round trip over the punched socket. (`crates/client/src/punch.rs`. The older peer starts punching as soon as it sends `PunchReply`, the newer one as soon as it receives it. A connection needs the round trip to work in *both* directions: the peer answered our request, and we answered one of theirs. Otherwise one side could stop while the other still waits.)
* [x] Abort immediately when both peers report symmetric NAT.
* [x] Add a timeout. On failure, abort with a clear error and no relay fallback. (15s by default.)
* [x] Peer datagrams carry a magic prefix (`PEER_MAGIC`) and a per-attempt nonce that the response must echo. Without the prefix, a late STUN response on the punching socket decodes as `PunchResponse { from: 1 }`. The nonce means stray or spoofed packets can't fake a round trip.
* [x] Tests (`crates/client/tests/punch.rs`): two clients connect end to end on loopback; both-symmetric aborts without sending anything; a silent peer times out, and the punches it receives come from the endpoint's socket; a response with the wrong nonce doesn't count.
* **Done when:** a connection succeeds on a permissive network and aborts cleanly when traversal is blocked. *(2026-10-15: two clients on one machine connect in a few ms through the public STUN defaults. Because they share a host they connect over the LAN candidate, so this proves the handshake, not NAT traversal. The blocked case is covered by the tests. Real traversal and a real blocked network are step 9.)*

#### Step 8: CLI Harness (Section 3)

* [x] Add commands to initiate a connection, listen for and accept incoming peers, and send a test payload (text message or small file chunk). `p2p-client listen [--save-dir DIR]` serves peers one at a time and prints messages or reports (and optionally saves) files. `p2p-client send --message TEXT | --file PATH` delivers one payload and exits. `p2p-client connect` only punches. Both sides run the same exchange and punch, so either can start first.
* [x] Payloads (`crates/client/src/session.rs`) are a single datagram of up to 1024 bytes, carrying a SHA-256. The receiver checks the hash and acknowledges every copy, delivering each `seq` once. The sender retransmits until it gets an acknowledgement. Bigger transfers are for QUIC. While connected, a `Keepalive` goes out every 10s to keep NAT mappings open, and a peer silent for 45s counts as gone. The listener keeps pinging the rendezvous server so it stays registered for the next peer. Received file names must be plain names, and existing files are never overwritten.
* [x] Tests (`crates/client/tests/session.rs`): a round trip between two sessions (a 1024-byte file and a message, then `Bye`); retransmission after a lost payload reuses its `seq`; no acknowledgement fails after every attempt; an acknowledgement reporting corruption is an error; oversized payloads are refused before sending; the receiver delivers duplicates once, acknowledges corrupted payloads as not intact, and ignores datagrams from the wrong address or peer id; a silent peer gets keepalives and then counts as gone.
* **Done when:** the payload sent by one instance arrives intact at the other. *(Verified 2026-10-15 on one machine: a message and a 700-byte random file arrived intact (matching SHA-256, `cmp` identical), and the listener served both senders in turn.)*

#### Step 9: Cross-Network Verification (Section 2)

* [x] With rendezvous on Railway and public STUN, run the clients on two different networks and log the connection results. *(2026-10-15: the dev container (WSL2, behind its home NAT) and a Raspberry Pi running the armv7 build (`just client-armv7`), on a different network behind its own NAT, connected directly with the same token. `stun-check` reported cone NAT on both.)*
* [x] Check that the STUN-reported public addresses match each network's real public IP. If they don't, check whether the client sits behind extra NAT layers, such as a VPN or Docker without host networking.
* [ ] ~~Try at least one hostile case (e.g. symmetric NAT) to confirm the strict-failure behaviour.~~ *Out of scope for the prototype. The abort paths (both peers symmetric, punch timeout) are covered by the step 7 tests but haven't been tried on a real hostile network.*
* **Done when:** a direct connection is logged across two networks and the failure case aborts without relaying. *(2026-10-15: the cross-network connection is verified. The real-network failure case is out of scope, see above.)*

---

### Concluded (2026-10-15)

The prototype met its goal, and the work stops here. The transport moves to [iroh](https://www.iroh.computer/) (1.x), and the next phase, [sync-plan.md](sync-plan.md), focuses on file sync instead. [quic-plan.md](quic-plan.md) is superseded.

#### What we learned

* **Direct connections work between our networks.** Two cone NATs on different networks punched through using only a rendezvous server and public STUN, connecting in milliseconds.
* **Most of the stack ended up being ours.** We wrote the rendezvous server, the signaling protocol, the STUN client, NAT type detection, punch coordination, nonce-checked confirmation and the payload session. From `rust-p2p-core` we used only a UDP socket wrapper, the `NatInfo` structs (half their fields unused) and `Puncher::punch_now`. Its only substantial logic, port guessing for symmetric NATs, was never tried on a real network.
* **Owning the transport means owning a lot more.** The datagram protocol has no encryption or peer authentication. Making it fit for real use meant QUIC, device identity with pinned certificates, and a socket adapter, because `rust-p2p-core` won't release its socket. iroh already provides all of that, plus more mature NAT traversal.
* **A rendezvous server can't see the network.** It drops out once peers connect, so it can't track connections or group size. That state has to come from the peers themselves (e.g. gossip), and that's equally true with iroh's relays.
* **Platform notes:** WSL2 adds its own NAT layer, so the dev container's local address and port aren't the host's. Railway proxies TCP only and hides client IPs, which rules out hosting STUN there.

#### Why we stopped

* iroh is a robust, maintained implementation of what this prototype was building, and from 1.x each connection reports its paths (`Path::is_ip()` / `is_relay()` / `is_selected()`), so the app can see and enforce whether it's direct.
* The hard, product-defining problems are distributed state and file synchronisation, not the transport. Handing the connection to iroh puts the effort there.

#### Decision: relayed traffic becomes a policy

The original constraint was "direct only, never relayed". Sync is a background task that can afford slow, bandwidth-limited relays, so relayed traffic is no longer ruled out entirely. Instead it becomes a **policy**, set per peer or per folder:

* **Direct only:** no application data is sent until the selected path is direct (`is_ip()`), and the connection is closed if it falls back to a relay. The QUIC handshake and hole-punching coordination still pass through the relay, end-to-end encrypted.
* **Relay allowed:** data may flow through a relay, end-to-end encrypted.

Running our own `iroh-relay` keeps even relayed traffic off third-party infrastructure.

#### The prototype code

The code is frozen as it stands (tag the final prototype commit). It isn't maintained as a second transport. If a network appears where iroh's traversal fails and ours wouldn't, start from iroh's `unstable-custom-transports` feature rather than running two stacks side by side. Any revival should first drop `rust-p2p-core` in favour of a plain `UdpSocket`, our own `NatInfo` and a ported punching routine.
