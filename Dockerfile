# Rendezvous server image. Signaling only: one TCP port, no file data.
#
#   docker build -t rendezvous-server .
#   docker run -p 7000:7000 rendezvous-server
#
# Listens on 0.0.0.0:$PORT (default 7000). Railway sets PORT itself.

FROM rust:1.99-alpine AS builder
WORKDIR /build

# Build dependencies against stub sources first, so editing our own code
# doesn't rebuild every dependency. Every workspace member's manifest must be
# present for cargo to load the workspace.
COPY Cargo.toml Cargo.lock ./
COPY crates/client/Cargo.toml crates/client/
COPY crates/protocol/Cargo.toml crates/protocol/
COPY crates/server/Cargo.toml crates/server/
RUN mkdir -p crates/client/src crates/protocol/src crates/server/src \
    && echo 'fn main() {}' > crates/client/src/main.rs \
    && echo 'fn main() {}' > crates/server/src/main.rs \
    && touch crates/protocol/src/lib.rs crates/server/src/lib.rs \
    && cargo build --release --locked -p rendezvous-server \
    && rm -r crates/*/src

COPY crates crates
# COPY keeps the files' original mtimes, which can be older than the stub
# build, and cargo would then reuse the stub binary. Touch to force a rebuild.
RUN touch crates/*/src/*.rs \
    && cargo build --release --locked -p rendezvous-server

FROM alpine:3.24
RUN adduser -D -H -u 10001 rendezvous
COPY --from=builder /build/target/release/rendezvous-server /usr/local/bin/
USER rendezvous
EXPOSE 7000
ENTRYPOINT ["rendezvous-server"]
