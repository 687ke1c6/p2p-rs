# List available recipes
default:
    @just --list

# Format check, clippy (warnings as errors) and tests across the workspace
check:
    cargo fmt --all --check
    cargo clippy --workspace --all-targets -- -D warnings
    cargo test --workspace

# Run the sync node, passing any extra args through
node *args:
    cargo run -p sync-node -- {{args}}

# Cross-compile the node for armv7 (Debian bookworm armhf) into target/release/armv7
armv7:
    docker build -f Dockerfile.armv7 --output type=local,dest=target/release/armv7 . && scp target/release/armv7/sync-node g@raspberrypi:~
