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
