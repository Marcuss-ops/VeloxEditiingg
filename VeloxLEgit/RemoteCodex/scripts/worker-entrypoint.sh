#!/usr/bin/env bash
# worker-entrypoint.sh — slim runtime launcher for the Velox worker container.
#
# Contract enforced by the multistage Dockerfile:
#   * C++ engine binary is at $VELOX_VIDEO_ENGINE_CPP_BIN (default
#     /usr/local/bin/velox_video_engine), produced FROM the SAME
#     debian:bookworm-slim base as the runtime image via cpp-builder
#     stage 1. There is NO rebuild path in the runtime image — the
#     compiler toolchain is intentionally absent.
#   * Go worker binary is at $VELOX_GO_WORKER_BIN (default
#     /usr/local/bin/velox-worker-agent), pre-built by `make agent`
#     and COPYed directly into the image.
#
# What this script does:
#   1. Crash hard if either binary is missing or not executable — the
#      container is not designed to mint its own binaries at runtime.
#   2. `ldd`-grep for "not found" so the dynamic linker error surfaces
#      as a clear FATAL line instead of a confusing kernel-level
#      ENOEXEC on first use.
#   3. exec the Go worker with the original argv.
#
# Removed from previous version:
#   * `strings` + GLIBC scrape: required `binutils`, which the slim
#     runtime image intentionally does NOT ship (smaller image, smaller
#     attack surface). Multistage guarantees GLIBC parity by construction.
#   * Source-tree fallback rebuild (re-running build-video-engine.sh if
#     the binary was missing). Same reason: no compiler toolchain in
#     runtime. Operators that need source-tree rebuilds should use the
#     `make docker` target locally.
set -Eeuo pipefail

ENGINE_BINARY="${VELOX_VIDEO_ENGINE_CPP_BIN:-/usr/local/bin/velox_video_engine}"
GO_BINARY="${VELOX_GO_WORKER_BIN:-/usr/local/bin/velox-worker-agent}"

log() {
    printf '[worker-entrypoint] %s\n' "$*"
}
fail() {
    log "FATAL: $*"
    exit 1
}

log "Runtime glibc: $(getconf GNU_LIBC_VERSION 2>/dev/null || echo unknown)"

if [[ ! -x "$ENGINE_BINARY" ]]; then
    fail "C++ engine binary missing or not executable at $ENGINE_BINARY. Slim runtime image has no compiler toolchain; rebuild via 'make -C RemoteCodex/native/worker-agent-go docker'."
fi

LDD_OUTPUT="$(ldd "$ENGINE_BINARY" 2>&1 || true)"
printf '%s\n' "$LDD_OUTPUT"
if printf '%s\n' "$LDD_OUTPUT" | grep -q 'not found'; then
    fail "Engine $ENGINE_BINARY references unresolved shared libraries. The slim runtime image is missing one or more dependencies."
fi

if [[ ! -x "$GO_BINARY" ]]; then
    fail "Go worker binary missing or not executable at $GO_BINARY. Pre-build it with 'make -C RemoteCodex/native/worker-agent-go agent'."
fi

log "C++ engine validation passed"
log "Starting worker agent"

exec "$GO_BINARY" "$@"
