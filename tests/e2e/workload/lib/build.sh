# shellcheck shell=bash
# build.sh — Phase 1: build master, worker, and video engine binaries.

phase_build() {
  info "Phase 1: building binaries"
  mkdir -p "$BIN_DIR"

  ENGINE_BIN="$WORKDIR/engine-build/velox_video_engine"
  if [[ -x "$MASTER_BIN" && -x "$WORKER_BIN" && -x "$ENGINE_BIN" ]]; then
    info "binaries already built — skipping"
    return 0
  fi

  info "  → velox-server"
  (cd "$REPO_ROOT/DataServer" && go build -o "$MASTER_BIN" \
    -ldflags "-s -w -X main.Version=$VERSION" ./cmd/server) || {
    fail "master build failed"; exit 2; }

  info "  → velox-worker-agent"
  (cd "$REPO_ROOT/RemoteCodex/native/worker-agent-go" && \
    go build -o "$WORKER_BIN" -ldflags "-s -w" ./cmd/velox-worker-agent) || {
    fail "worker build failed"; exit 2; }

  info "  → velox_video_engine"
  cmake -S "$REPO_ROOT/RemoteCodex/native/video-engine-cpp" \
    -B "$WORKDIR/engine-build" -DCMAKE_BUILD_TYPE=Release >/dev/null
  cmake --build "$WORKDIR/engine-build" --parallel >/dev/null
  [[ -x "$ENGINE_BIN" ]] || { fail "engine build did not produce $ENGINE_BIN"; exit 2; }

  pass "build complete"
}
