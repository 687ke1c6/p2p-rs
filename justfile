# List available recipes
default:
    @just --list

# Format check, clippy (warnings as errors) and tests across the workspace
check:
    cargo fmt --all --check
    cargo clippy --workspace --all-targets -- -D warnings
    cargo test --workspace

# Load and contention tests (slow; ignored by `check`)
stress:
    cargo test --workspace -- --ignored

# Run the sync node, passing any extra args through
node *args:
    cargo run -p sync-node -- {{args}}

# Cross-compile the node for armv7 (Debian bookworm armhf) into target/release/armv7
armv7:
    docker build -f Dockerfile.armv7 --output type=local,dest=target/release/armv7 . && scp target/release/armv7/sync-node g@raspberrypi:~ && rsync -avP ui/dist g@raspberrypi:~

# Build the web UI into ui/dist/
ui:
    cd ui && bun install --frozen-lockfile && bun run build

# Vite dev server with hot reload; proxies /api to a `run --ui-addr <addr>` node
ui-dev addr="127.0.0.1:7777":
    cd ui && bun install --frozen-lockfile && SYNC_NODE_UI_ADDR={{addr}} bun run dev

# Type check and lint the web UI
ui-check:
    cd ui && bun install --frozen-lockfile && bun run check
