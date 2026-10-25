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

# Cross-compile the node for armv7 (Debian bookworm armhf) into target/release/armv7,
# build the web UI, and copy both to the Pi: ~/sync-node and ~/ui (its default UI dir)
armv7: ui
    docker build -f Dockerfile.armv7 --output type=local,dest=target/release/armv7 .

# Build the web UI into ui/dist/
ui:
    cd ui && bun install --frozen-lockfile && bun run build

# Vite dev server with hot reload; proxies /api to a `run --ui-addr <addr>` node
ui-dev addr="127.0.0.1:7777":
    cd ui && bun install --frozen-lockfile && SYNC_NODE_UI_ADDR={{addr}} bun run dev

# Write the API types from the Rust structs (ts-rs) to ui/src/api/
ui-types:
    cargo test -q -p sync-node --lib ui::tests::export_api_types

# Check the API types are fresh, then type check and lint the web UI
ui-check:
    #!/usr/bin/env bash
    set -euo pipefail
    fresh=$(mktemp -d)
    trap 'rm -rf "$fresh"' EXIT
    SYNC_NODE_UI_TYPES_DIR="$fresh" cargo test -q -p sync-node --lib ui::tests::export_api_types
    if ! diff -r "$fresh" ui/src/api; then
        echo "ui/src/api differs from the Rust API structs: run \`just ui-types\`" >&2
        exit 1
    fi
    cd ui && bun install --frozen-lockfile && bun run check
