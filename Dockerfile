# sync-node on Alpine, with the web UI embedded.
#
#   docker build -t sync-node .
#   docker run -d --name sync-node --network host \
#       -v sync-node-config:/config -v /srv/sync:/sync \
#       -e SYNC_NODE_UI_ADDR=127.0.0.1:7777 sync-node
#
# Host networking: iroh's hole punching works best on the host's own
# addresses, and the UI stays on the host's loopback (there is no auth, see
# ADR 0004). Synced folders are paths inside the container, e.g.
#   docker exec sync-node sync-node folder add <name> /sync/<name>

FROM oven/bun:1.4-alpine AS ui
WORKDIR /build/ui
COPY ui ./
RUN bun install --frozen-lockfile && bun run build

FROM rust:1.99-alpine AS builder
# A C toolchain: ring compiles C.
RUN apk add --no-cache build-base
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY patches patches
COPY crates crates
# The web UI, embedded in the binary (`ui::EmbeddedPage`).
COPY --from=ui /build/ui/dist ui/dist
RUN cargo build --release --locked -p sync-node

FROM alpine:3.22
COPY --from=builder /build/target/release/sync-node /usr/local/bin/sync-node
ENV SYNC_NODE_CONFIG_DIR=/config
VOLUME /config
# `run` stops cleanly on Ctrl-C (SIGINT) only; `docker stop` sends SIGTERM.
STOPSIGNAL SIGINT
ENTRYPOINT ["sync-node"]
CMD ["run"]
