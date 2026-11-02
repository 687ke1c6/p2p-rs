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

# Release build of the node for the host (target/release/sync-node), with the web UI embedded
release: ui
    cargo build --release --locked -p sync-node

# Cross-compile the node for armv7 (Debian bookworm armhf), with the web UI embedded,
# into target/docker/bookworm-armv7
armv7: (build "bookworm-armv7")

# Build the node in Docker for each target (default: all) into target/docker/<target>/
build *targets:
    build/dist.sh build {{targets}}

# Upload each target's build (default: all) to drop-docs (settings in .env); no build
publish *targets:
    build/dist.sh publish {{targets}}

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
