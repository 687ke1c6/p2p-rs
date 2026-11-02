#!/usr/bin/env bash
# Builds sync-node for other targets in Docker, and publishes the builds to
# drop-docs.
#
#   build/dist.sh build [target...]     each binary to target/docker/<target>/sync-node
#   build/dist.sh publish [target...]   uploads the binaries `build` made; no build
#
# Targets (default: all):
#   bookworm-armv7    32-bit Raspberry Pi OS (build/bookworm.Dockerfile)
#   bookworm-arm64    64-bit Raspberry Pi OS, e.g. a Pi 4 Model B
#   bookworm-x86_64   Debian/Ubuntu desktops and WSL; builds on an x86_64 machine
#   alpine-armv7      Alpine on 32-bit ARM (static musl, build/musl.Dockerfile)
#   alpine-arm64      Alpine on 64-bit ARM
#   alpine-x86_64     Alpine on x86_64
#
# `publish` uploads each binary in a tar.gz to
# $DROP_DOCS_HOST/api/upload/$DROP_DOCS_STORE/$DROP_DOCS_FEATURE-<target>/<version>,
# where <version> is the node's version in Cargo.toml. The drop-docs settings
# come from .env (see .env.example).
set -euo pipefail
trap 'echo "dist.sh: failed at line $LINENO: $BASH_COMMAND" >&2' ERR
cd "$(dirname "$0")/.."

all_targets=(
    bookworm-armv7 bookworm-arm64 bookworm-x86_64
    alpine-armv7 alpine-arm64 alpine-x86_64
)

# The `docker build` args for target $1, into the array `args`.
docker_args() {
    case $1 in
        bookworm-armv7)
            args=(-f build/bookworm.Dockerfile
                --build-arg TARGET=armv7-unknown-linux-gnueabihf
                --build-arg "PACKAGES=gcc-arm-linux-gnueabihf libc6-dev-armhf-cross"
                --build-arg LINKER=arm-linux-gnueabihf-gcc)
            ;;
        bookworm-arm64)
            args=(-f build/bookworm.Dockerfile
                --build-arg TARGET=aarch64-unknown-linux-gnu
                --build-arg "PACKAGES=gcc-aarch64-linux-gnu libc6-dev-arm64-cross"
                --build-arg LINKER=aarch64-linux-gnu-gcc)
            ;;
        bookworm-x86_64) args=(-f build/bookworm.Dockerfile --build-arg TARGET=x86_64-unknown-linux-gnu) ;;
        alpine-armv7) args=(-f build/musl.Dockerfile --build-arg TARGET=armv7-unknown-linux-musleabihf) ;;
        alpine-arm64) args=(-f build/musl.Dockerfile --build-arg TARGET=aarch64-unknown-linux-musl) ;;
        alpine-x86_64) args=(-f build/musl.Dockerfile --build-arg TARGET=x86_64-unknown-linux-musl) ;;
        *)
            echo "unknown target: $1 (known: ${all_targets[*]})" >&2
            exit 1
            ;;
    esac
}

build() {
    echo "==> building the web UI"
    just ui
    for target in "${targets[@]}"; do
        echo "==> building $target"
        docker_args "$target"
        out="target/docker/$target"
        rm -rf "$out"
        docker build "${args[@]}" --output "type=local,dest=$out" .
        echo "==> $out/sync-node"
    done
}

# POSTs to drop-docs path $1 with the curl args after it. Prints the answer's
# body; fails with the status and body unless the status is 2xx.
post() {
    local url="$DROP_DOCS_HOST$1" body status
    shift
    body=$(mktemp)
    status=$(curl -sS -o "$body" -w '%{http_code}' -X POST "$url" "$@") || {
        echo "POST $url: no answer (curl failed)" >&2
        rm -f "$body"
        return 1
    }
    if [[ $status != 2* ]]; then
        echo "POST $url: HTTP $status" >&2
        cat "$body" >&2
        echo >&2
        rm -f "$body"
        return 1
    fi
    cat "$body"
    rm -f "$body"
}

publish() {
    if [ -f .env ]; then
        set -a
        # shellcheck source=/dev/null
        . ./.env
        set +a
    fi
    for var in DROP_DOCS_HOST DROP_DOCS_USER DROP_DOCS_PASSWORD DROP_DOCS_FEATURE DROP_DOCS_STORE; do
        if [ -z "${!var:-}" ]; then
            echo "$var is not set (put it in .env)" >&2
            exit 1
        fi
    done
    # Fail on a missing build before any upload starts.
    for target in "${targets[@]}"; do
        if [ ! -f "target/docker/$target/sync-node" ]; then
            echo "no build for $target (run \`just build $target\`)" >&2
            exit 1
        fi
    done
    # The node's version (the workspace's, in Cargo.toml).
    version=$(cargo metadata --no-deps --format-version 1 \
        | jq -r '.packages[] | select(.name == "sync-node") | .version')
    if [ -z "$version" ]; then
        echo "found no sync-node version in cargo metadata" >&2
        exit 1
    fi

    echo "==> getting a token from $DROP_DOCS_HOST as $DROP_DOCS_USER"
    answer=$(post /api/token \
        --data-urlencode "username=$DROP_DOCS_USER" \
        --data-urlencode "password=$DROP_DOCS_PASSWORD")
    token=$(jq -r '.access_token // empty' <<<"$answer" 2>/dev/null || true)
    if [ -z "$token" ]; then
        echo "drop-docs gave no access_token. Its answer:" >&2
        echo "$answer" >&2
        exit 1
    fi

    tmp=$(mktemp -d)
    trap 'rm -rf "$tmp"' EXIT
    for target in "${targets[@]}"; do
        feature="$DROP_DOCS_FEATURE-$target"
        # Uploaded as `package.tar.gz` whatever the target; the feature names it.
        tar -czf "$tmp/package.tar.gz" -C "target/docker/$target" sync-node
        echo "==> uploading $target to $DROP_DOCS_STORE/$feature/$version"
        post "/api/upload/$DROP_DOCS_STORE/$feature/$version" \
            -H "Authorization: Bearer $token" \
            -F "file=@$tmp/package.tar.gz;filename=package.tar.gz"
        echo
    done
    echo "==> uploaded ${#targets[@]} build(s)"
}

command=${1:-}
case $command in
    build | publish) shift ;;
    *)
        echo "usage: build/dist.sh build|publish [target...]" >&2
        exit 1
        ;;
esac
targets=("$@")
if [ ${#targets[@]} -eq 0 ]; then
    targets=("${all_targets[@]}")
fi
# Fail on an unknown target before any work starts.
for target in "${targets[@]}"; do
    docker_args "$target"
done
"$command"
