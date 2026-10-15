# List available recipes
default:
    @just --list

# Format check, clippy (warnings as errors) and tests across the workspace
check:
    cargo fmt --all --check
    cargo clippy --workspace --all-targets -- -D warnings
    cargo test --workspace

# Run the rendezvous server, passing any extra args through
server *args:
    cargo run -p rendezvous-server -- {{args}}

# Run the P2P client, passing any extra args through
client *args:
    cargo run -p p2p-client -- {{args}}

# Build the rendezvous server image
docker-build:
    docker build -t rendezvous-server .

# Run the rendezvous server image in the background (replaces a running one)
docker-run port="7000":
    -docker rm -f rendezvous
    docker run -d --name rendezvous --restart unless-stopped -e PORT={{port}} -p {{port}}:{{port}} rendezvous-server

# Follow the running container's logs
docker-logs:
    docker logs -f rendezvous

# Stop and remove the running container
docker-stop:
    docker rm -f rendezvous

# Run the rendezvous integration tests against a deployed server, e.g. a Railway TCP proxy host:port
test-remote addr:
    RENDEZVOUS_TEST_ADDR={{addr}} cargo test -p rendezvous-server --test rendezvous

# Query each STUN server (defaults, or --stun a,b) and report mapped addresses and NAT type
stun-check *args:
    cargo run -q -p p2p-client -- stun-check {{args}}

# Run the tests that need the internet (public STUN servers)
test-network:
    cargo test -p p2p-client --test stun_live -- --ignored --nocapture

# Cross-compile the client for armv7 (Debian bookworm armhf) into target/release/armv7
client-armv7:
    docker build -f Dockerfile.client-armv7 --output type=local,dest=target/release/armv7 .
