### Project Outline & Intention

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

* [ ] **Lightweight Signaling Server:** Implement or deploy a minimal rendezvous server responsible *only* for metadata exchange (trading IP addresses and ports), handling zero file data.
* [ ] **Peer Handshake:** Allow two client instances to register with the signaling server, discover each other via unique share/device tokens, and trade connection candidates.
* [ ] **Rendezvous and STUN Are Separate Services:** Even if one process provides both, the client treats rendezvous and STUN as two independent services, each with its own `host:port`. Hosting providers like Railway give every exposed port its own hostname (e.g. `shuttle.proxy.rlwy.net:15140`), so the client must never assume both services share a host or derive one address from the other.
* [ ] **STUN Service (Address Reflection):** STUN tells each client its public address: the source IP and port of the client's UDP packet as seen from the internet. The client speaks standard RFC 5389 STUN Binding requests (`rust-p2p-core::stun` already implements them), not a custom message. We use existing public STUN servers instead of hosting our own (see the decision below).
* [ ] **NAT Type Detection:** The client sends Binding requests to at least two STUN endpoints from the same UDP socket and compares the mapped ports. Using servers at different IPs also catches NATs that change the mapping only when the destination IP changes. The same mapping means a cone NAT, different mappings mean a symmetric NAT. The result fills `NatInfo.nat_type`, which the `Puncher` uses to choose its strategy. With only one STUN endpoint configured, the client logs a warning and assumes cone NAT.
* [ ] **Client Endpoint Configuration:** Each service address is set by CLI flag or environment variable. Rendezvous is required. STUN has public defaults.
  * `--rendezvous <host:port>` / `RENDEZVOUS_ADDR`: one TCP endpoint.
  * `--stun <host:port>` / `STUN_ADDRS` (comma-separated): one or more UDP endpoints. Defaults to a small list of public servers from different providers, e.g. `stun.l.google.com:19302`, `stun1.l.google.com:19302` and `stun.cloudflare.com:3478`. Setting the option replaces the defaults instead of adding to them.
  * Each hostname is resolved independently at startup, and every resolved address is logged. A failure to resolve or reach one service is reported with that service's name.
* [ ] **Explicit STUN List:** The client contacts only its own STUN list, either the defaults above or the user's override. It does not use `rust-p2p-core`'s built-in default list, which includes servers we haven't chosen.

> **Hosting constraint (Railway):** Railway does not accept inbound UDP, and its TCP proxy hides the client's source IP. The rendezvous server can run on Railway over TCP, because signaling only needs to pass messages. **STUN cannot run on Railway**, because it must receive UDP and see the client's real source address. **Decision: use public STUN servers** for the prototype and probably beyond, rather than host infrastructure that already exists. Trade-off accepted: the STUN providers see each client's public IP and port when it starts up. They never see the peer, the token or any file data, so the "data never routes through third-party infrastructure" constraint still holds. Because the client keeps STUN separate, self-hosting later would only be a configuration change.

#### 2. Network Transport & Connection Constraint

* [ ] **Direct-Only Enforcement:** Use the `rust-p2p-core` `Puncher` with the peer's exchanged `NatInfo` to attempt a direct peer-to-peer connection. A connection counts as established only after a punch request/response round trip over the punched socket succeeds. The server never forwards anything except signaling messages, so there is no path for data to be relayed.
* [ ] **Early Abort:** If both peers report symmetric NAT, fail immediately with a clear "direct connection not possible" error instead of waiting for punch attempts to time out.
* [ ] **Strict Failure Handling:** If the NAT traversal fails (e.g., hostile symmetric NATs), the app must gracefully abort the connection attempt rather than routing through a fallback relay.
* [ ] **Connectivity Verification:** Log and verify successful raw socket connection establishment between machines across different local networks.

#### 3. Prototyping Scope & Next Steps

* [ ] **CLI Harness:** Build a simple command-line interface for the prototype to trigger the connection handshake, accept incoming peer requests, and send a test payload (e.g., a simple text message or small file chunk) over the established P2P socket.
* [ ] **Repository Setup:** Initialize a Git repository to house the prototype code, structuring it cleanly for future expansion into a local background daemon and file watcher.

#### 4. Development Environment

* [ ] **Rust Dev Container:** All development happens inside a Rust dev container (`.devcontainer/devcontainer.json`, based on the official Rust dev container image or a Dockerfile pinning the Rust toolchain). It should include `rustfmt`, `clippy`, and `rust-analyzer`, so builds and tooling are reproducible across machines. Commit the dev container config to the repository.
* [ ] **Host Networking:** The dev container runs in host network mode (`"runArgs": ["--network=host"]` in `devcontainer.json`), so it shares the host's network stack. This avoids an extra Docker NAT layer that would distort NAT traversal and hole-punching tests. Note that on Docker Desktop (including WSL2 setups using it) host mode behaves differently from native Linux, so verify the container sees the real host interfaces before trusting test results.

#### 5. Running the Rendezvous Server

The rendezvous server is deployed with Docker. It handles metadata only (addresses and ports) and never touches file data. It is TCP-only, so it can run behind Railway's TCP proxy. STUN uses public servers, so there is nothing to deploy for it.

* [ ] **Dockerfile:** Create a `Dockerfile` for the rendezvous server in the repository. Use a multi-stage build: a Rust builder stage that compiles the server binary in release mode, and a minimal runtime stage based on `alpine` that copies in only the binary. Alpine uses musl libc, so build in a `rust:alpine` builder stage (or target `x86_64-unknown-linux-musl`) so the binary links against musl and runs in the runtime image. Fall back to `debian:bookworm-slim` only if a dependency doesn't build against musl. Run as a non-root user and expose the signaling port.
* [ ] **`.dockerignore`:** Exclude `target/`, `.git/` and `.devcontainer/` to keep the build context small.
* [ ] **Build the Image:** `docker build -t rendezvous-server .`
* [ ] **Run the Container:** `docker run -d --name rendezvous --restart unless-stopped -p <PORT>:<PORT> rendezvous-server`. Configure the listen address and port through environment variables or CLI flags.
* [ ] **Ports:** The server listens on one TCP port, read from `PORT` (which Railway sets) or a `--listen` flag.
* [ ] **Railway Deployment:** Deploy from the Dockerfile and enable a TCP proxy on the listen port. Railway assigns a hostname and port such as `shuttle.proxy.rlwy.net:15140`, which becomes the client's `--rendezvous` value. Because the proxy hides client source IPs, the server must never use the connection's remote address as a candidate. Clients report their own candidates from STUN.
* [ ] **Verify:** Check the container logs, then confirm that two clients can register and exchange candidates through the server.

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
* [ ] Deploy to Railway with a TCP proxy and note the assigned `host:port`.
* **Done when:** the container runs locally and on Railway, logs show it listening, and the step 3 integration test passes against both.

#### Step 5: Public STUN Selection (Section 1)

* [ ] Choose the default STUN list: at least two providers and servers at different IPs.
* [ ] Add a `just stun-check` recipe (or a client subcommand) that sends a Binding request to each configured server and prints the mapped address.
* **Done when:** every default server returns the machine's real public `IP:port`, checked against a known external IP, and an unreachable server is reported by name without stopping the others.

#### Step 6: Client Handshake & NAT Detection (Section 1)

* [ ] Client configuration: a required `--rendezvous <host:port>` and optional `--stun <host:port>` overrides of the public defaults (or `RENDEZVOUS_ADDR` / `STUN_ADDRS`). Resolve each hostname independently and report failures per service.
* [ ] Send STUN Binding requests from the socket that will be used for punching, build `NatInfo` (public address, local addresses, cone or symmetric) and log it. The mapping is only valid for that socket.
* [ ] Client connects to the rendezvous endpoint over TCP, registers with a token and receives the peer's `NatInfo`.
* **Done when:** two client instances on one machine, pointed at the rendezvous server and the public STUN defaults, complete the candidate exchange.

#### Step 7: Direct-Only Connection (Section 2)

* [ ] Both peers call `Puncher::punch` with the other's `NatInfo` at roughly the same time, then confirm with a `PunchRequest`/`PunchResponse` round trip over the punched socket.
* [ ] Abort immediately when both peers report symmetric NAT.
* [ ] Add a timeout. On failure, abort with a clear error and no relay fallback.
* **Done when:** a connection succeeds on a permissive network and aborts cleanly when traversal is blocked.

#### Step 8: CLI Harness (Section 3)

* [ ] Add commands to initiate a connection, listen for and accept incoming peers, and send a test payload (text message or small file chunk).
* **Done when:** the payload sent by one instance arrives intact at the other.

#### Step 9: Cross-Network Verification (Section 2)

* [ ] With rendezvous on Railway and public STUN, run the clients on two different networks and log the connection results.
* [ ] Check that the STUN-reported public addresses match each network's real public IP. If they don't, check whether the client sits behind extra NAT layers, such as a VPN or Docker without host networking.
* [ ] Try at least one hostile case (e.g. symmetric NAT) to confirm the strict-failure behaviour.
* **Done when:** a direct connection is logged across two networks and the failure case aborts without relaying.
