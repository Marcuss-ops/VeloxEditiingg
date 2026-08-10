#!/usr/bin/env bash
# Sourced by scripts/pilot.sh; definitions only.
# shellcheck disable=SC2164

cmd_build() {
  banner "BUILD: master + worker + engine"

  if [[ "${SKIP_BUILD:-0}" == "1" ]]; then
    for bin in "$MASTER_BIN" "$WORKER_BIN" "$ENGINE_BIN"; do
      [[ -x "$bin" ]] || die "SKIP_BUILD=1 but $bin is missing" 3
    done
    log "build: skipped (SKIP_BUILD=1)"
    return 0
  fi

  mkdir -p "$(dirname "$MASTER_BIN")"

  # ── Master (DataServer Go) ──────────────────────────────────────────────
  local MASTER_SRC="${REPO_ROOT}/DataServer/cmd/server"
  if [[ -d "$MASTER_SRC" ]]; then
    log "  → building velox-server"
    cd "${REPO_ROOT}/DataServer"
    go build -o "$MASTER_BIN" -ldflags "-s -w -X main.Version=${VERSION}" ./cmd/server 2>&1
    ok "  velox-server → ${MASTER_BIN}"
  else
    die "master source not found at ${MASTER_SRC}" 2
  fi

  # ── Worker agent (Go) ───────────────────────────────────────────────────
  local WORKER_SRC="${REPO_ROOT}/RemoteCodex/native/worker-agent-go"
  if [[ -d "$WORKER_SRC" ]]; then
    log "  → building velox-worker-agent"
    cd "$WORKER_SRC"
    make VERSION_FILE="../../../VERSION.txt" agent 2>&1
    cp -v "${WORKER_SRC}/bin/velox-worker-agent" "$WORKER_BIN"
    ok "  velox-worker-agent → ${WORKER_BIN}"
  else
    die "worker source not found at ${WORKER_SRC}" 2
  fi

  # ── Video engine (C++ cmake) ────────────────────────────────────────────
  local ENGINE_SRC="${REPO_ROOT}/RemoteCodex/native/video-engine-cpp"
  if [[ -d "$ENGINE_SRC" ]]; then
    log "  → building velox_video_engine"
    local BUILD_DIR="/tmp/velox-engine-pilot-build"
    mkdir -p "$BUILD_DIR"
    cd "$ENGINE_SRC"
    cmake -B "$BUILD_DIR" -DCMAKE_BUILD_TYPE=Release 2>&1
    cmake --build "$BUILD_DIR" --parallel 2>&1
    local ENGINE_BINARY
    ENGINE_BINARY="${BUILD_DIR}/velox_video_engine"
    if [[ ! -x "$ENGINE_BINARY" ]]; then
      warn "cmake build output listing:"
      ls -la "$BUILD_DIR" || true
      die "engine binary not found after cmake build" 2
    fi
    cp -v "$ENGINE_BINARY" "$ENGINE_BIN"
    rm -rf "$BUILD_DIR"
    ok "  velox_video_engine → ${ENGINE_BIN}"
  else
    warn "engine source not found at ${ENGINE_SRC} — skipping (engine tasks will fail)"
  fi

  ok "build complete"
}
