#!/usr/bin/env bash
# Builds sync-node for each target in Docker, packs each binary in a tar.gz,
# and uploads it to drop-docs as feature "$DROP_DOCS_FEATURE-<target>", version
# the node's version from Cargo.toml.
#
#   build/publish.sh [--no-publish] [target...]
#
# Targets (default: all):
#   bookworm-armv7    32-bit Raspberry Pi OS (build/bookworm.Dockerfile)
#   bookworm-arm64    64-bit Raspberry Pi OS, e.g. a Pi 4 Model B
#   bookworm-x86_64   Debian/Ubuntu desktops and WSL; builds on an x86_64 machine
#   alpine-armv7      Alpine on 32-bit ARM (static musl, build/musl.Dockerfile)
#   alpine-arm64      Alpine on 64-bit ARM
#   alpine-x86_64     Alpine on x86_64
#
# Each binary goes to target/docker/<target>/, each pack to target/dist/.
# drop-docs settings come from .env (see .env.example): DROP_DOCS_HOST,
# DROP_DOCS_USER, DROP_DOCS_PASSWORD, DROP_DOCS_FEATURE, DROP_DOCS_STORE.
set -euo pipefail
trap 'echo "publish.sh: failed at line $LINENO: $BASH_COMMAND" >&2' ERR
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

publish=1
targets=()
for arg in "$@"; do
    case $arg in
        --no-publish) publish=0 ;;
        *) targets+=("$arg") ;;
    esac
done
if [ ${#targets[@]} -eq 0 ]; then
    targets=("${all_targets[@]}")
fi
# Fail on an unknown target before any build starts.
for target in "${targets[@]}"; do
    docker_args "$target"
done

if [ $publish -eq 1 ]; then
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
    # The node's version (the workspace's, in Cargo.toml) names the upload.
    version=$(cargo metadata --no-deps --format-version 1 \
        | jq -r '.packages[] | select(.name == "sync-node") | .version')
    if [ -z "$version" ]; then
        echo "found no sync-node version in cargo metadata" >&2
        exit 1
    fi
fi

# The commit, with -dirty for uncommitted or untracked changes, so a pack says
# what it is.
rev=$(git rev-parse --short HEAD)
if [ -n "$(git status --porcelain)" ]; then
    rev="$rev-dirty"
fi

echo "==> building the web UI"
just ui

dist=target/dist
mkdir -p "$dist"
packs=()
for target in "${targets[@]}"; do
    echo "==> building $target"
    docker_args "$target"
    out="target/docker/$target"
    rm -rf "$out"
    docker build "${args[@]}" --output "type=local,dest=$out" .
    pack="$dist/sync-node-$target-$rev.tar.gz"
    tar -czf "$pack" -C "$out" sync-node
    packs+=("$target=$pack")
    echo "==> $pack"
done

if [ $publish -eq 0 ]; then
    exit 0
fi

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

for entry in "${packs[@]}"; do
    target=${entry%%=*}
    pack=${entry#*=}
    feature="$DROP_DOCS_FEATURE-$target"
    # Uploaded as `package.<ext>` whatever the target; the feature names it.
    name=${pack##*/}
    upload="package.${name#*.}"
    echo "==> uploading $pack to $DROP_DOCS_STORE as $feature/$version/$upload"
    post "/api/upload/$DROP_DOCS_STORE/$feature/$version" \
        -H "Authorization: Bearer $token" \
        -F "file=@$pack;filename=$upload"
    echo
done
echo "==> uploaded ${#packs[@]} pack(s)"
