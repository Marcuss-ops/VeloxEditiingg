#!/usr/bin/env bash
set -Eeuo pipefail

ENGINE_SOURCE="${VELOX_VIDEO_ENGINE_SRC:-/app/native/video-engine-cpp}"
ENGINE_BINARY="${VELOX_VIDEO_ENGINE_CPP_BIN:-/usr/local/bin/velox_video_engine}"
BUILD_ROOT="${VELOX_VIDEO_ENGINE_BUILD_DIR:-/tmp/velox-video-engine-build}"

log() {
    printf '[worker-entrypoint] %s\n' "$*"
}

fail() {
    log "FATAL: $*"
    exit 1
}

log "Starting deterministic C++ engine build"

# Non usare mai build/ ricevuto dal bundle.
rm -rf \
    "$ENGINE_SOURCE/build" \
    "$ENGINE_SOURCE/CMakeFiles" \
    "$ENGINE_SOURCE/CMakeCache.txt" \
    "$ENGINE_SOURCE/cmake_install.cmake"

rm -rf "$BUILD_ROOT"
mkdir -p "$BUILD_ROOT"

export VELOX_VIDEO_ENGINE_SRC="$ENGINE_SOURCE"
export VELOX_VIDEO_ENGINE_BUILD_DIR="$BUILD_ROOT"
export VELOX_VIDEO_ENGINE_OUT="$ENGINE_BINARY"

/usr/local/bin/build-video-engine.sh

test -x "$ENGINE_BINARY" ||
    fail "Engine binary missing or not executable: $ENGINE_BINARY"

log "Runtime GLIBC:"
getconf GNU_LIBC_VERSION

log "Engine dependencies:"
LDD_OUTPUT="$(ldd "$ENGINE_BINARY" 2>&1)"
printf '%s\n' "$LDD_OUTPUT"

if printf '%s\n' "$LDD_OUTPUT" | grep -q "not found"; then
    fail "Engine contains unresolved shared libraries"
fi

RUNTIME_GLIBC="$(getconf GNU_LIBC_VERSION | awk '{print $2}')"

REQUIRED_GLIBC="$(
    strings "$ENGINE_BINARY" |
        grep -oE 'GLIBC_[0-9]+(\.[0-9]+)*' |
        sed 's/^GLIBC_//' |
        sort -Vu |
        tail -n1
)"

if [[ -n "$REQUIRED_GLIBC" ]]; then
    HIGHEST="$(
        printf '%s\n%s\n' "$RUNTIME_GLIBC" "$REQUIRED_GLIBC" |
            sort -V |
            tail -n1
    )"

    if [[ "$HIGHEST" != "$RUNTIME_GLIBC" ]]; then
        fail "Engine requires GLIBC $REQUIRED_GLIBC, runtime provides $RUNTIME_GLIBC"
    fi
fi

log "C++ engine validation passed"
log "Starting worker agent"

exec /usr/local/bin/velox-worker-agent "$@"
