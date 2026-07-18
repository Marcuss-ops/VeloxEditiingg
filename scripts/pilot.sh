#!/usr/bin/env bash
# scripts/pilot.sh
#
# Velox pilot launcher — one-command reproduce of the 4-of-5 links pipeline
# on any sandbox. Encapsulates the dev-bypass environment variables
# (VELOX_GRPC_ALLOW_INSECURE_DEV / VELOX_ALLOW_INSECURE_GRPC_DEV /
# VELOX_ASSET_REWRITE_DEV_BYPASS) so the next operator does not inherit
# 5 rounds of iterative patch history.
#
# Usage:
#   ./scripts/pilot.sh [command]
#
# Commands:
#   all           Full pipeline: build → start → submit → work → poll (default)
#   build         Build master + worker + engine binaries
#   start         Start master (with all dev bypasses)
#   submit        Generate test fixtures + submit an images.v1 job
#   work          Start worker (with all dev bypasses)
#   stop          Stop master + worker processes
#   status        Show running processes + master health
#   log           Tail master log
#
# Environment:
#   PILOT_DIR     Working directory (default: /tmp/velox-pilot)
#   SKIP_BUILD    If set, skip building binaries (reuse existing)
#   SKIP_CLEANUP  If set, do not stop processes on exit
#
# Exit codes:
#   0   Success
#   1   General failure
#   2   Build failure
#   3   Environment/deps missing
#   126 Timeout
#
# ─── WARNING: Dev bypasses ────────────────────────────────────────────────────
# This script sets THREE dev-bypass environment variables that are
# PRODUCTION-UNSAFE. They exist so the pilot can run end-to-end without
# mTLS certs or production asset wiring on a throwaway sandbox:
#
#   VELOX_GRPC_ALLOW_INSECURE_DEV=true   (master side: plaintext gRPC)
#   VELOX_ALLOW_INSECURE_GRPC_DEV=true   (worker side: plaintext gRPC)
#   VELOX_ASSET_REWRITE_DEV_BYPASS=true  (master: allow any file:// path)
#
# NEVER use this script against a production database or a reachable
# network. These env vars are deliberately separate from the production
# deployment paths (mTLS, VELOX_GRPC_TLS_*, allowedRoots) so they
# cannot be set by accident in production configs.
# ──────────────────────────────────────────────────────────────────────────────

set -euo pipefail

# ─── Repo root (always works regardless of CWD) ──────────────────────────────
readonly REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly SCRIPT_NAME="$(basename "$0")"

# ─── Configuration ───────────────────────────────────────────────────────────
PILOT_DIR="${PILOT_DIR:-/tmp/velox-pilot-$(date +%s)-$$}"
readonly LOGDIR="${PILOT_DIR}/logs"
readonly PID_DIR="${PILOT_DIR}"
readonly DATA_DIR="${PILOT_DIR}/data"
readonly STAGING_DIR="${PILOT_DIR}/staging"
readonly STORAGE_DIR="${PILOT_DIR}/storage"

pick_free_port() {
  python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()'
}

readonly MASTER_PORT="${PILOT_MASTER_PORT:-$(pick_free_port)}"
readonly GRPC_PORT="${PILOT_GRPC_PORT:-$(pick_free_port)}"
readonly WORKER_HEALTH_PORT="${PILOT_WORKER_HEALTH_PORT:-$(pick_free_port)}"
readonly ADMIN_TOKEN="test-admin-token"
readonly WORKER_ID="pilot-worker-1"
readonly DESTINATION_ID="e2e-local"

# Binaries (built from repo)
readonly MASTER_BIN="${PILOT_DIR}/bin/velox-server"
readonly WORKER_BIN="${PILOT_DIR}/bin/velox-worker-agent"
readonly ENGINE_BIN="${PILOT_DIR}/bin/velox_video_engine"

# Paths
readonly MASTER_LOG="${LOGDIR}/master.log"
readonly WORKER_LOG="${LOGDIR}/worker.log"
readonly MASTER_PIDFILE="${PID_DIR}/master.pid"
readonly WORKER_PIDFILE="${PID_DIR}/worker.pid"
readonly MASTER_ENV="${PID_DIR}/master.env"
readonly WORKER_CONFIG="${PID_DIR}/worker.json"
readonly JOB_FILE="${PID_DIR}/job.json"

# Version from canonical source
VERSION="$(tr -d '[:space:]' < "${REPO_ROOT}/VERSION.txt" 2>/dev/null || echo "dev")"

# ─── Dev bypasses (pilot-only; see WARNING above) ────────────────────────────
# Scoped exports: only the cmd_* functions that need them set the bypass
# variables, NOT script top. Prevents `./scripts/pilot.sh status` or
# `./scripts/pilot.sh stop` from leaking plaintext-gRPC + allow-any-path
# env vars into the calling shell on invocation.
# Worker-side bypass is set explicitly in cmd_work() via env prefix
# (VELOX_ALLOW_INSECURE_GRPC_DEV is a separate var enforced by the worker's
# transport_factory.go).

# ─── Terminal helpers ────────────────────────────────────────────────────────
log()    { printf '\e[36m[pilot]\e[0m %s\n' "$*"; }
ok()     { printf '\e[32m[pilot]\e[0m %s\n' "$*"; }
warn()   { printf '\e[33m[pilot][WARN]\e[0m %s\n' "$*" >&2; }
die()    { printf '\e[31m[pilot][FAIL]\e[0m %s\n' "$*" >&2; exit "${2:-1}"; }
banner() { echo; echo "──────────────────────────────────────────────────────"; echo "  $*"; echo "──────────────────────────────────────────────────────"; }

# ─── Cleanup trap ────────────────────────────────────────────────────────────
cleanup() {
  if [[ "${SKIP_CLEANUP:-0}" != "1" ]]; then
    log "cleanup: stopping processes"
    [[ -f "$MASTER_PIDFILE" ]] && kill -- "-$(cat "$MASTER_PIDFILE")" 2>/dev/null || true
    [[ -f "$WORKER_PIDFILE" ]] && kill -- "-$(cat "$WORKER_PIDFILE")" 2>/dev/null || true
    wait 2>/dev/null || true
    # Remove pid files so subsequent cmd_status reports correctly.
    rm -f "$MASTER_PIDFILE" "$WORKER_PIDFILE"
  fi
}
trap cleanup EXIT

# ═══════════════════════════════════════════════════════════════════════════════
# COMMAND: build
# ═══════════════════════════════════════════════════════════════════════════════
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

# ═══════════════════════════════════════════════════════════════════════════════
# COMMAND: start
# ═══════════════════════════════════════════════════════════════════════════════
assert_port_free() {
  local port="$1"
  if ss -ltn "sport = :${port}" | grep -q LISTEN; then
    die "required pilot port ${port} is occupied; set PILOT_*_PORT or stop its owner" 3
  fi
}

init_database() {
  [[ ! -e "${DATA_DIR}/velox.db" ]] ||
    die "refusing to reuse existing database ${DATA_DIR}/velox.db; choose a new PILOT_DIR or remove it explicitly" 3
  mkdir -p "$DATA_DIR"
  log "  → applying canonical SQLite migrations"
  (cd "$REPO_ROOT/DataServer" && go run ./cmd/seed-velox-db-fixture "${DATA_DIR}/velox.db")
  sqlite3 "${DATA_DIR}/velox.db" \
    "INSERT INTO delivery_destinations (destination_id, provider, name, enabled, configuration_json, created_at, updated_at) VALUES ('${DESTINATION_ID}', 'google_drive', 'Local E2E', 1, '{}', datetime('now'), datetime('now'));"
}

cmd_start() {
  banner "START: master"

  assert_port_free "$MASTER_PORT"
  assert_port_free "$GRPC_PORT"
  init_database

  # Build if binaries don't exist
  if [[ ! -x "$MASTER_BIN" ]]; then
    warn "master binary not found — building first"
    cmd_build
  fi

  # Ensure clean state
  mkdir -p "$LOGDIR" "$DATA_DIR" "$STAGING_DIR" "$STORAGE_DIR"
  rm -f "$MASTER_LOG" "$MASTER_PIDFILE"

  # Write master env file (dev bypasses are auto-set at script top)
  cat > "$MASTER_ENV" <<ENV
VELOX_MASTER_PORT=${MASTER_PORT}
VELOX_GRPC_PORT=${GRPC_PORT}
VELOX_DB_PATH=${DATA_DIR}/velox.db
VELOX_DATA_DIR=${DATA_DIR}
VELOX_STAGING_DIR=${STAGING_DIR}
VELOX_STORAGE_DIR=${STORAGE_DIR}
VELOX_ADMIN_TOKEN=${ADMIN_TOKEN}
VELOX_ALLOWED_WORKERS=${WORKER_ID}
VELOX_CODE_VERSION=${VERSION}
GIN_MODE=release
ENV

  # Source the env file so the master inherits VELOX_MASTER_PORT, VELOX_DB_PATH, etc.
  set -a; source "$MASTER_ENV"; set +a

  # Dev bypasses are scoped to this function so they do NOT leak into the
  # calling shell on `./scripts/pilot.sh status` / `./scripts/pilot.sh stop`.
  export VELOX_GRPC_ALLOW_INSECURE_DEV=true
  export VELOX_ASSET_REWRITE_DEV_BYPASS=true

  cd "$PILOT_DIR"
  setsid "$MASTER_BIN" serve </dev/null >"$MASTER_LOG" 2>&1 &
  local MPID=$!
  echo "$MPID" > "$MASTER_PIDFILE"
  disown "$MPID" 2>/dev/null
  log "master PID=${MPID}"

  # Wait for healthy
  for i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15; do
    if curl -fsS -o /dev/null "http://127.0.0.1:${MASTER_PORT}/health" 2>/dev/null; then
      kill -0 "$MPID" 2>/dev/null || die "health answered after master PID exited" 1
      ss -ltnp | grep -q "pid=${MPID}," || die "port ${MASTER_PORT} is not owned by master PID ${MPID}" 1
      grep -q "Velox master listening on :${MASTER_PORT}" "$MASTER_LOG" || die "master listener identity missing" 1
      ok "master healthy (${i}s)"
      return 0
    fi
    sleep 1
  done

  tail -40 "$MASTER_LOG" 2>/dev/null || true
  die "master did not become healthy within 15s" 1
}

# ═══════════════════════════════════════════════════════════════════════════════
# COMMAND: submit
# ═══════════════════════════════════════════════════════════════════════════════
cmd_submit() {
  banner "SUBMIT: fixtures + job"

  mkdir -p "$STAGING_DIR"

  # Generate test fixtures
  log "  → silent.aac (2s silent)"
  ffmpeg -hide_banner -loglevel error -y \
    -f lavfi -i anullsrc=r=48000:cl=mono -t 2 \
    -c:a aac -b:a 64k "${STAGING_DIR}/silent.aac" 2>/dev/null || true

  log "  → scene.png (teal 320x180)"
  ffmpeg -hide_banner -loglevel error -y \
    -f lavfi -i color=c=0x008080:s=320x180:d=0.1 -frames:v 1 \
    -vcodec png "${STAGING_DIR}/scene.png" 2>/dev/null || true

  # Verify fixtures
  ls -la "${STAGING_DIR}/silent.aac" "${STAGING_DIR}/scene.png" 2>/dev/null || \
    warn "fixture files may be missing (ffmpeg might not support libmp3lame)"

  "${REPO_ROOT}/scripts/e2e/write-local-workload-fixture.sh" "$JOB_FILE" "$STAGING_DIR" "$DESTINATION_ID"

  # Submit
  local SUBMIT_OUT
  SUBMIT_OUT="$(curl -sS -m 15 -X POST \
    -H "Authorization: Bearer ${ADMIN_TOKEN}" \
    -H "Content-Type: application/json" \
    --data-binary @"$JOB_FILE" \
    "http://127.0.0.1:${MASTER_PORT}/api/v1/script/generate-with-images" 2>&1)" || true

  echo "$SUBMIT_OUT" | python3 -m json.tool 2>/dev/null || echo "$SUBMIT_OUT"

  local JOB_ID
  JOB_ID="$(echo "$SUBMIT_OUT" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('job_id',''))" 2>/dev/null || true)"

  if [[ -z "$JOB_ID" ]]; then
    die "job submission failed — could not extract job_id" 1
  fi

  log "job_id=${JOB_ID}"

  # Show current jobs from DB
  banner "JOBS in DB"
  sqlite3 "${DATA_DIR}/velox.db" \
    "SELECT job_id, status, video_name, created_at FROM jobs ORDER BY created_at DESC LIMIT 5;" \
    2>/dev/null || true

  ok "job submitted (PENDING)"
}

# ═══════════════════════════════════════════════════════════════════════════════
# COMMAND: work
# ═══════════════════════════════════════════════════════════════════════════════
cmd_work() {
  banner "WORK: start worker"

  if [[ ! -x "$WORKER_BIN" ]]; then
    warn "worker binary not found — building first"
    cmd_build
  fi

  mkdir -p "$LOGDIR"
  rm -f "$WORKER_LOG" "$WORKER_PIDFILE"

  # Write worker config (dev bypass: allow_insecure_grpc_dev: true)
  local BUNDLE_HASH
  BUNDLE_HASH="$("${REPO_ROOT}/scripts/e2e/write-local-bundle-identity.sh" "$PILOT_DIR" "$WORKER_BIN" "$ENGINE_BIN")"
  mkdir -p "${PILOT_DIR}/tests/fixtures"
  cp "${REPO_ROOT}/RemoteCodex/native/worker-agent-go/tests/fixtures/engine_selftest_baseline.sha256" \
    "${PILOT_DIR}/tests/fixtures/engine_selftest_baseline.sha256"
  cat > "$WORKER_CONFIG" <<JSON
{
  "master_url": "http://127.0.0.1:${MASTER_PORT}",
  "admin_token": "${ADMIN_TOKEN}",
  "worker_id": "${WORKER_ID}",
  "work_dir": "${PILOT_DIR}",
  "control_grpc_url": "127.0.0.1:${GRPC_PORT}",
  "job_delivery": "push",
  "allow_insecure_grpc_dev": true,
  "bundle_hash": "${BUNDLE_HASH}",
  "video_engine_cpp_bin": "${ENGINE_BIN}",
  "output_dir": "${PILOT_DIR}/runtime-output",
  "temp_dir": "${PILOT_DIR}/runtime-temp",
  "data_dir": "${PILOT_DIR}",
  "state_dir": "${PILOT_DIR}/state",
  "max_active_jobs": 1,
  "health_port": ${WORKER_HEALTH_PORT},
  "protocol_version": "v3"
}
JSON

  # Worker has its OWN separate env var (VELOX_ALLOW_INSECURE_GRPC_DEV) that
  # transport_factory.go enforces — it's NOT the same var as the master's
  # VELOX_GRPC_ALLOW_INSECURE_DEV. Must pass it explicitly. Scoped to this
  # function so it does not leak into the calling shell on other subcommands.
  cd "$PILOT_DIR"
  local WORKER_TOKEN
  WORKER_TOKEN="$(curl -fsS -m 10 -X POST \
    -H "Content-Type: application/json" \
    --data "{\"worker_id\":\"${WORKER_ID}\",\"worker_name\":\"pilot-worker\",\"protocol_version\":\"v3\",\"bundle_hash\":\"${BUNDLE_HASH}\"}" \
    "http://127.0.0.1:${MASTER_PORT}/api/v1/workers/register" \
    | python3 -c 'import json,sys; print(json.load(sys.stdin)["session_id"])')" \
    || die "worker HTTP registration/token bootstrap failed" 1
  [[ -n "$WORKER_TOKEN" ]] || die "worker HTTP registration returned an empty token" 1
  setsid env \
    VELOX_ENV=dev \
    VELOX_ALLOW_INSECURE_GRPC_DEV=true \
    WORKER_TOKEN="$WORKER_TOKEN" \
    "$WORKER_BIN" -config "$WORKER_CONFIG" \
    </dev/null >"$WORKER_LOG" 2>&1 &
  local WPID=$!
  echo "$WPID" > "$WORKER_PIDFILE"
  disown "$WPID" 2>/dev/null
  log "worker PID=${WPID}"

  # Wait for registration signal in master log
  for i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15; do
    if grep -qE "${WORKER_ID}.*(hello_ack|HelloAck)|Worker ${WORKER_ID} connected" "$MASTER_LOG" 2>/dev/null \
      || grep -q "Registration successful" "$WORKER_LOG" 2>/dev/null; then
      ok "worker registered (${i}s)"
      return 0
    fi
    if ! kill -0 "$WPID" 2>/dev/null; then
      warn "worker process died — dumping worker log"
      tail -60 "$WORKER_LOG" 2>/dev/null || true
      die "worker crashed during registration" 1
    fi
    sleep 2
  done

  tail -40 "$MASTER_LOG" 2>/dev/null || true
  tail -40 "$WORKER_LOG" 2>/dev/null || true
  die "worker did not register within 30s" 126
}

# ═══════════════════════════════════════════════════════════════════════════════
# COMMAND: status
# ═══════════════════════════════════════════════════════════════════════════════
cmd_status() {
  banner "STATUS"

  # Master
  if [[ -f "$MASTER_PIDFILE" ]]; then
    local MPID
    MPID="$(cat "$MASTER_PIDFILE")"
    if ps -p "$MPID" >/dev/null 2>&1; then
      ok "master running (PID=${MPID})"
      # Health check
      curl -fsS -m 3 -o /dev/null "http://127.0.0.1:${MASTER_PORT}/health" 2>/dev/null && \
        ok "master health: OK" || warn "master health: FAIL"
    else
      warn "master PID=${MPID} NOT running (stale PID file)"
    fi
  else
    warn "master NOT running (no PID file)"
  fi

  # Worker
  if [[ -f "$WORKER_PIDFILE" ]]; then
    local WPID
    WPID="$(cat "$WORKER_PIDFILE")"
    if ps -p "$WPID" >/dev/null 2>&1; then
      ok "worker running (PID=${WPID})"
    else
      warn "worker PID=${WPID} NOT running (stale PID file)"
    fi
  else
    warn "worker NOT running (no PID file)"
  fi

  # Jobs
  if [[ -f "${DATA_DIR}/velox.db" ]]; then
    banner "JOBS in DB"
    sqlite3 "${DATA_DIR}/velox.db" \
      "SELECT job_id, status, video_name, updated_at FROM jobs ORDER BY updated_at DESC LIMIT 5;" \
      2>/dev/null || true
  fi

  # Log tails
  banner "MASTER LOG (tail 10)"
  tail -10 "$MASTER_LOG" 2>/dev/null || true
  banner "WORKER LOG (tail 10)"
  tail -10 "$WORKER_LOG" 2>/dev/null || true
}

# ═══════════════════════════════════════════════════════════════════════════════
# COMMAND: stop
# ═══════════════════════════════════════════════════════════════════════════════
cmd_stop() {
  banner "STOP"

  # Worker first (de-register cleanly)
  if [[ -f "$WORKER_PIDFILE" ]]; then
    local WPID
    WPID="$(cat "$WORKER_PIDFILE")"
    kill -- -"$WPID" 2>/dev/null && log "worker process-group TERM sent to PGID=${WPID}" || true
    sleep 2
    kill -- -"$WPID" 2>/dev/null && log "worker process-group KILL sent" || true
    rm -f "$WORKER_PIDFILE"
  fi

  # Master
  if [[ -f "$MASTER_PIDFILE" ]]; then
    local MPID
    MPID="$(cat "$MASTER_PIDFILE")"
    kill -- -"$MPID" 2>/dev/null && log "master process-group TERM sent to PGID=${MPID}" || true
    sleep 2
    kill -- -"$MPID" 2>/dev/null && log "master process-group KILL sent" || true
    rm -f "$MASTER_PIDFILE"
  fi

  ok "processes stopped"
}

# ═══════════════════════════════════════════════════════════════════════════════
# COMMAND: log
# ═══════════════════════════════════════════════════════════════════════════════
cmd_log() {
  if [[ ! -f "$MASTER_LOG" ]]; then
    die "master log not found at ${MASTER_LOG} — start master first" 1
  fi
  tail -n 200 -F "$MASTER_LOG"
}

verify_completed_job() {
  local db="$1"
  local job_id="$2"
  local video
  local video_count
  video_count="$(find "$STORAGE_DIR" -type f \( -name '*.mp4' -o -name '*.f4v' \) | wc -l)"
  [[ "$video_count" -eq 1 ]] || die "expected exactly one final video artifact, found ${video_count}" 1
  video="$(find "$STORAGE_DIR" -type f \( -name '*.mp4' -o -name '*.f4v' \) -print -quit)"
  [[ -s "$video" ]] || die "final video is empty: ${video}" 1

  local probe
  probe="$(ffprobe -v error -show_entries stream=codec_type,codec_name,width,height,r_frame_rate -show_entries format=duration,size -of json "$video" 2>&1)" \
    || die "ffprobe failed for ${video}: ${probe}" 1
  grep -q '"codec_type": "video"' <<<"$probe" \
    || die "final artifact has no video stream: ${video}" 1

  local decode_log="${PILOT_DIR}/decode-errors.log"
  ffmpeg -v error -i "$video" -f null - 2>"$decode_log" \
    || die "decode command failed for ${video}" 1
  [[ ! -s "$decode_log" ]] || { cat "$decode_log"; die "final video is not fully decodable" 1; }

  local actual_sha actual_size recorded
  actual_sha="$(sha256sum "$video" | awk '{print $1}')"
  actual_size="$(stat -c '%s' "$video")"
  recorded="$(sqlite3 "$db" "SELECT status || '|' || sha256 || '|' || size_bytes || '|' || COALESCE(verified_at,'') FROM artifacts WHERE job_id='${job_id}' ORDER BY created_at DESC LIMIT 1;" 2>/dev/null || true)"
  local recorded_status recorded_sha recorded_size recorded_verified
  IFS='|' read -r recorded_status recorded_sha recorded_size recorded_verified <<<"$recorded"
  [[ "$recorded_status" == "READY" && "$recorded_sha" == "$actual_sha" \
    && "$recorded_size" == "$actual_size" && -n "$recorded_verified" ]] \
    || die "artifact DB verification mismatch: recorded=${recorded} actual=READY|${actual_sha}|${actual_size}|verified" 1

  local task_state
  task_state="$(sqlite3 "$db" "SELECT status || '|' || COALESCE(attempt_id,'') || '|' || COALESCE(winning_attempt_id,'') FROM tasks WHERE job_id='${job_id}';" 2>/dev/null || true)"
  [[ "$task_state" == SUCCEEDED\|*\|* ]] || die "task did not succeed: ${task_state}" 1
  local task_attempt task_winner
  task_attempt="${task_state#*|}"; task_attempt="${task_attempt%%|*}"
  task_winner="${task_state##*|}"
  [[ -n "$task_winner" && "$task_winner" == "$task_attempt" ]] \
    || die "succeeded task has no matching winning attempt: ${task_state}" 1

  ok "video validated: ${video} (${actual_size} bytes, sha256=${actual_sha})"
  ok "artifact READY and winning TaskAttempt verified"
}

# ═══════════════════════════════════════════════════════════════════════════════
# COMMAND: all (default — full pipeline)
# ═══════════════════════════════════════════════════════════════════════════════
cmd_all() {
  banner "VELOX PILOT — full pipeline"
  log "version: ${VERSION}"
  log "pilot dir: ${PILOT_DIR}"
  log "dev bypasses:"
  log "  VELOX_GRPC_ALLOW_INSECURE_DEV=true  (master gRPC plaintext)"
  log "  VELOX_ALLOW_INSECURE_GRPC_DEV=true  (worker gRPC plaintext)"
  log "  VELOX_ASSET_REWRITE_DEV_BYPASS=true (asset path allow-all)"
  echo
  warn "These bypasses are PRODUCTION-UNSAFE. See WARNING at top of script."

  cmd_build
  cmd_start
  cmd_submit
  cmd_work

  # Poll for SUCCEEDED
  banner "POLL: waiting for SUCCEEDED"
  local DB="${DATA_DIR}/velox.db"
  local JOB_ID
  JOB_ID="$(sqlite3 "$DB" "SELECT job_id FROM jobs ORDER BY created_at DESC LIMIT 1;" 2>/dev/null || true)"
  if [[ -z "$JOB_ID" ]]; then
    die "no job found in DB — submission may have failed" 1
  fi
  log "polling job_id=${JOB_ID}"

  local MAX_POLLS=42  # 42 × 10s = 7 minutes
  local POLL_INTERVAL=10

  for i in $(seq 1 "$MAX_POLLS"); do
    local STATUS
    STATUS="$(sqlite3 "$DB" "SELECT status FROM jobs WHERE job_id='${JOB_ID}';" 2>/dev/null || true)"

    case "$STATUS" in
      SUCCEEDED)
        ok "job SUCCEEDED after ~$(( i * POLL_INTERVAL ))s"
        verify_completed_job "$DB" "$JOB_ID"
        return 0
        ;;
      FAILED|TIMEOUT|REJECTED|CANCELLED)
        warn "job terminal with status=${STATUS}"
        sqlite3 "$DB" "SELECT job_id, status, updated_at FROM jobs WHERE job_id='${JOB_ID}';" || true
        die "job reached terminal status ${STATUS} (expected SUCCEEDED)" 1
        ;;
      ""|PENDING|RUNNING|LEASED|RENDER_FINISHED|FINALIZING)
        if (( i % 5 == 0 )); then
          log "  poll[${i}/${MAX_POLLS}] status=${STATUS}  (elapsed=$(( i * POLL_INTERVAL ))s)"
        fi
        ;;
      *)
        warn "unknown status: ${STATUS}"
        ;;
    esac
    sleep "$POLL_INTERVAL"
  done

  die "job did not reach SUCCEEDED within $(( MAX_POLLS * POLL_INTERVAL ))s" 126
}

# ═══════════════════════════════════════════════════════════════════════════════
# Main dispatch
# ═══════════════════════════════════════════════════════════════════════════════
main() {
  local cmd="${1:-all}"

  case "$cmd" in
    all)     cmd_all ;;
    build)   cmd_build ;;
    start)   cmd_start ;;
    submit)  cmd_submit ;;
    work)    cmd_work ;;
    stop)    cmd_stop ;;
    status)  cmd_status ;;
    log)     cmd_log ;;
    --help|-h|help)
      sed -n '/^# Usage:/,/^# Exit codes:/p' "$0" | sed 's/^# //p'
      ;;
    *)
      echo "Unknown command: ${cmd}"
      echo "Usage: $0 {all|build|start|submit|work|stop|status|log}"
      exit 2
      ;;
  esac
}

main "$@"
