# sync-node

A local-first tool that keeps folders in sync between a small group of trusted
machines over direct peer-to-peer connections. See [GLOSSARY.md](GLOSSARY.md)
for the terms and [docs/adr/](docs/adr/) for the design decisions.

## Develop

Run `just` to list the recipes. The main ones:

| Recipe | Does |
|---|---|
| `just check` | Format check, clippy and tests |
| `just node <args>` | Runs the node |
| `just ui-dev` | Web UI dev server with hot reload |
| `just release` | Release build for the host, with the web UI embedded |

## Build for other targets

`just build [target...]` builds the node in Docker, with the web UI embedded.
Each binary goes to `target/docker/<target>/sync-node`. With no targets, it
builds all of them.

| Target | For | Dockerfile |
|---|---|---|
| `bookworm-armv7` | 32-bit Raspberry Pi OS | `build/bookworm.Dockerfile` |
| `bookworm-arm64` | 64-bit Raspberry Pi OS, e.g. a Pi 4 Model B | `build/bookworm.Dockerfile` |
| `bookworm-x86_64` | Debian/Ubuntu desktops and WSL | `build/bookworm.Dockerfile` |
| `alpine-armv7` | Alpine on 32-bit ARM (static musl) | `build/musl.Dockerfile` |
| `alpine-arm64` | Alpine on 64-bit ARM (static musl) | `build/musl.Dockerfile` |
| `alpine-x86_64` | Alpine on x86_64 (static musl) | `build/musl.Dockerfile` |

`just publish [target...]` uploads those builds to drop-docs, with the version
from `Cargo.toml`. It does not build. Copy [.env.example](.env.example) to
`.env` and fill in the settings first.

## Docker image

The root [Dockerfile](Dockerfile) makes a runnable Alpine image. Its header
has the `docker build` and `docker run` commands.
