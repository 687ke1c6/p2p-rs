# sync-node for one Rust target, linked against Debian bookworm's glibc, so it
# runs on bookworm (and newer glibc) systems. The web UI is embedded: run
# `just ui` first.
#
#   docker build -f build/bookworm.Dockerfile --output type=local,dest=<dir> \
#       --build-arg TARGET=aarch64-unknown-linux-gnu \
#       --build-arg "PACKAGES=gcc-aarch64-linux-gnu libc6-dev-arm64-cross" \
#       --build-arg LINKER=aarch64-linux-gnu-gcc .
#
# With no PACKAGES and LINKER, the build machine's own gcc links: right for
# its own architecture. build/dist.sh has the args for each target. The
# build context is the repo root.
# The final stage is `scratch` holding only the binary: it exists to be
# exported with `docker build --output`, not run.

# Cross-compile on the build machine's native platform; no emulation.
FROM --platform=$BUILDPLATFORM rust:1.99-bookworm AS builder
ARG TARGET
ARG PACKAGES=""
ARG LINKER=""
RUN if [ -n "$PACKAGES" ]; then \
        apt-get update \
        && apt-get install -y --no-install-recommends $PACKAGES \
        && rm -rf /var/lib/apt/lists/*; \
    fi \
    && rustup target add $TARGET
WORKDIR /build
RUN if [ -n "$LINKER" ]; then \
        mkdir .cargo && printf '[target.%s]\nlinker = "%s"\n' "$TARGET" "$LINKER" > .cargo/config.toml; \
    fi

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
    && cargo build --release --locked -p sync-node --target $TARGET \
    && rm -r crates/*/src

# The web UI, embedded in the binary (`ui::EmbeddedPage`). `just ui` builds
# it on the host first; bun isn't in this image.
COPY ui/dist ui/dist
COPY crates crates
# COPY keeps the files' original mtimes, which can be older than the stub
# build, and cargo would then reuse the stub binary. Touch to force a rebuild.
RUN find crates -name '*.rs' -exec touch {} + \
    && cargo build --release --locked -p sync-node --target $TARGET \
    && cp target/$TARGET/release/sync-node /sync-node

FROM scratch
COPY --from=builder /sync-node /sync-node
