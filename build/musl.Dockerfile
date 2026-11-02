# sync-node for one musl Rust target, linked statically, so it runs on Alpine
# (and any other Linux) with no libraries. The web UI is embedded: run
# `just ui` first.
#
#   docker build -f build/musl.Dockerfile --output type=local,dest=<dir> \
#       --build-arg TARGET=aarch64-unknown-linux-musl .
#
# cargo-zigbuild links with zig, which is also the C cross compiler for ring,
# so one image builds every architecture. build/publish.sh has the targets.
# The build context is the repo root.
# The final stage is `scratch` holding only the binary: it exists to be
# exported with `docker build --output`, not run.

# Cross-compile on the build machine's native platform; no emulation.
FROM --platform=$BUILDPLATFORM rust:1.99-alpine AS builder
# Before the TARGET arg, so every target shares this layer.
RUN apk add --no-cache musl-dev zig \
    && cargo install --locked cargo-zigbuild
ARG TARGET
RUN rustup target add $TARGET
WORKDIR /build

# Build dependencies against stub sources first, so editing our own code
# doesn't rebuild every dependency. Every workspace member's manifest must be
# present for cargo to load the workspace.
COPY Cargo.toml Cargo.lock ./
# Patched upstream crates ([patch.crates-io] in Cargo.toml), built with the
# other dependencies.
COPY patches patches
COPY crates/node/Cargo.toml crates/node/
RUN mkdir -p crates/node/src \
    && echo 'fn main() {}' > crates/node/src/main.rs \
    && touch crates/node/src/lib.rs \
    && cargo zigbuild --release --locked -p sync-node --target $TARGET \
    && rm -r crates/*/src

# The web UI, embedded in the binary (`ui::EmbeddedPage`). `just ui` builds
# it on the host first; bun isn't in this image.
COPY ui/dist ui/dist
COPY crates crates
# COPY keeps the files' original mtimes, which can be older than the stub
# build, and cargo would then reuse the stub binary. Touch to force a rebuild.
RUN find crates -name '*.rs' -exec touch {} + \
    && cargo zigbuild --release --locked -p sync-node --target $TARGET \
    && cp target/$TARGET/release/sync-node /sync-node

FROM scratch
COPY --from=builder /sync-node /sync-node
