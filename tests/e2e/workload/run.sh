#!/usr/bin/env bash
# =============================================================================
# tests/e2e/workload/run.sh — PR 5 real workload E2E
# =============================================================================
# Runs the full Hello → artifact → SUCCEEDED pipeline with deterministic
# fixtures, strict media/SHA checks, metrics checks, worker visibility, and
# blocking database finalization assertions.
#
# Usage:
#   make e2e-workload
#   E2E_WORKDIR=/tmp/vx-wl make e2e-workload
#   bash tests/e2e/workload/run.sh
# =============================================================================

set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$ROOT/../../.." && pwd)"
WORKDIR="${E2E_WORKDIR:-/tmp/velox-e2e-workload}"

# ─── Paths ───────────────────────────────────────────────────────────────────
BIN_DIR="$WORKDIR/bin"
DATA_DIR="$WORKDIR/data"
# Keep BlobStore spool and fixture inputs under DATA_DIR; inputsecurity.ValidateFile rejects unrelated paths.
STAGING_DIR="$DATA_DIR/staging"
FIXTURE_DIR="$DATA_DIR/fixtures"
STORAGE_DIR="$WORKDIR/storage"
LOG_DIR="$WORKDIR/logs"

MASTER_BIN="$BIN_DIR/velox-server"
WORKER_BIN="$BIN_DIR/velox-worker-agent"
MASTER_LOG="$LOG_DIR/master.log"
WORKER_LOG="$LOG_DIR/worker.log"
MASTER_ENV="$WORKDIR/master.env"
WORKER_CFG="$WORKDIR/worker.json"
MASTER_PIDFILE="$WORKDIR/master.pid"
WORKER_PIDFILE="$WORKDIR/worker.pid"

MASTER_PORT="${E2E_MASTER_PORT:-8080}"
GRPC_PORT="${E2E_GRPC_PORT:-50051}"
WORKER_HEALTH_PORT="${E2E_WORKER_HEALTH_PORT:-0}"
WORKER_PROMETHEUS_PORT="${E2E_WORKER_PROMETHEUS_PORT:-0}"
ADMIN_TOKEN="e2e-workload-token"
WORKER_ID="e2e-workload-worker-1"
DESTINATION_ID="e2e-local"

VERSION="$(tr -d '[:space:]' < "$REPO_ROOT/VERSION.txt" 2>/dev/null || echo "dev")"

# ─── Colors ─────────────────────────────────────────────────────────────────
C_GREEN='\033[32m'; C_RED='\033[31m'; C_CYAN='\033[36m'; C_RST='\033[0m'
pass() { printf "${C_GREEN}PASS${C_RST}  %s\n" "$*"; }
fail() { printf "${C_RED}FAIL${C_RST}  %s\n" "$*"; return 1; }
info() { printf "${C_CYAN}.. %s${C_RST}\n" "$*"; }

# Reusable workload phases and assertions share this script's environment.
for helper in \
  "$ROOT/lib/submit.sh" \
  "$ROOT/lib/worker_wait.sh" \
  "$ROOT/lib/artifact_assertions.sh" \
  "$ROOT/lib/video_assertions.sh" \
  "$ROOT/lib/metrics_assertions.sh"; do
  # shellcheck disable=SC1090
  source "$helper"
done

# ─── Cleanup ────────────────────────────────────────────────────────────────
declare -a CHILD_PIDS=()
push_pid() { CHILD_PIDS+=("$1"); }

pick_free_port() {
  python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()'
}

if [[ -z "${E2E_MASTER_PORT:-}" ]]; then MASTER_PORT="$(pick_free_port)"; fi
if [[ -z "${E2E_GRPC_PORT:-}" ]]; then GRPC_PORT="$(pick_free_port)"; fi
if [[ "$WORKER_HEALTH_PORT" == "0" ]]; then WORKER_HEALTH_PORT="$(pick_free_port)"; fi
if [[ "$WORKER_PROMETHEUS_PORT" == "0" ]]; then WORKER_PROMETHEUS_PORT="$(pick_free_port)"; fi

assert_port_free() {
  local port="$1"
  if ss -ltn "sport = :${port}" | grep -q LISTEN; then
    fail "required E2E port ${port} is occupied; choose E2E_*_PORT explicitly"
    exit 3
  fi
}

kill_all() {
  local sig="${1:-TERM}"
  for pid in "${CHILD_PIDS[@]}"; do
    kill -0 "$pid" 2>/dev/null && kill -- -"$pid" 2>/dev/null || true
  done
  if [[ "$sig" == "TERM" ]]; then
    sleep 1
    for pid in "${CHILD_PIDS[@]}"; do
      kill -0 "$pid" 2>/dev/null && kill -- -"$pid" 2>/dev/null || true
    done
    for pid in "${CHILD_PIDS[@]}"; do wait "$pid" 2>/dev/null || true; done
  fi
}

on_exit() {
  kill_all TERM
  rm -f "$MASTER_PIDFILE" "$WORKER_PIDFILE"
}
trap on_exit EXIT
trap 'kill_all TERM; exit 130' INT
trap 'kill_all TERM; exit 143' TERM

# ═══════════════════════════════════════════════════════════════════════════════
# Phase 1: Build
# ═══════════════════════════════════════════════════════════════════════════════
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

# ═══════════════════════════════════════════════════════════════════════════════
# Phase 2: Fixtures (deterministic, self-contained)
# ═══════════════════════════════════════════════════════════════════════════════
phase_fixtures() {
  info "Phase 2: generating test fixtures"
  mkdir -p "$FIXTURE_DIR"

  command -v ffmpeg >/dev/null 2>&1 || { fail "ffmpeg not found — install ffmpeg"; exit 3; }

  # Scene image: pure teal (#008080), 1920x1080, 1 frame PNG.
  # Matches the engine's default canvas so the rendered output is
  # native rather than upscaled from a smaller source.
  info "  → scene.png (teal 1920x1080)"
  ffmpeg -hide_banner -loglevel error -y \
    -f lavfi -i "color=c=0x008080:s=1920x1080:d=0.1" -frames:v 1 \
    -vcodec png "$FIXTURE_DIR/scene.png" 2>/dev/null || {
    fail "scene.png generation failed"; exit 3; }

  # Silent audio: 2 seconds, AAC inside an MP4 container named .mp4. The
  # MP4 container is REQUIRED: inputsecurity sniffs the file header, and
  # raw ADTS AAC (a bare .aac) is not sniffable by http.DetectContentType
  # (rejected with INPUT_MIME_UNSUPPORTED). The extension must ALSO be .mp4:
  # the sniffed MIME for an MP4 container is video/mp4, and an .m4a name
  # declares audio/mp4 → INPUT_MIME_MISMATCH. video/mp4 is accepted for the
  # voiceover/audio role by allowedMIME, and ffprobe validates the stream.
  info "  → silent.mp4 (2s, AAC in MP4 container)"
  ffmpeg -hide_banner -loglevel error -y \
    -f lavfi -i "anullsrc=r=48000:cl=mono" -t 2 \
    -c:a aac -b:a 64k "$FIXTURE_DIR/silent.mp4" 2>/dev/null || {
    # Try MP3 fallback (ID3 tag → audio/mpeg, also accepted)
    ffmpeg -hide_banner -loglevel error -y \
      -f lavfi -i "anullsrc=r=44100:cl=mono" -t 2 \
      -c:a libmp3lame -b:a 64k "$FIXTURE_DIR/silent.mp3" 2>/dev/null || {
      fail "audio fixture generation failed"; exit 3; }
    info "  → silent.mp3 (2s, MP3 fallback)"
  }

  local scene_path="$FIXTURE_DIR/scene.png"
  local audio_path="$FIXTURE_DIR/silent.mp4"
  [[ -f "$audio_path" ]] || audio_path="$FIXTURE_DIR/silent.mp3"

  ls -la "$scene_path" "$audio_path" 2>/dev/null
  pass "fixtures ready"
}

# ═══════════════════════════════════════════════════════════════════════════════
# Phase 3: Start master
# ═══════════════════════════════════════════════════════════════════════════════
phase_master_start() {
  info "Phase 3: starting master"
  mkdir -p "$DATA_DIR" "$STORAGE_DIR" "$LOG_DIR"

  assert_port_free "$MASTER_PORT"
  assert_port_free "$GRPC_PORT"
  [[ ! -e "$DATA_DIR/velox.db" ]] || {
    fail "refusing to reuse existing database $DATA_DIR/velox.db; choose a fresh E2E_WORKDIR"
    exit 3
  }
  (cd "$REPO_ROOT/DataServer" && go run ./cmd/seed-velox-db-fixture "$DATA_DIR/velox.db") >/dev/null
  sqlite3 "$DATA_DIR/velox.db" \
    "INSERT INTO delivery_destinations (destination_id, provider, name, enabled, configuration_json, created_at, updated_at) VALUES ('$DESTINATION_ID', 'google_drive', 'Local E2E', 1, '{}', datetime('now'), datetime('now'));"

  # Control-plane endpoints required by validateBootstrapEndpoints (REST public + gRPC control).
  cat > "$MASTER_ENV" <<ENV
VELOX_MASTER_PORT=$MASTER_PORT
VELOX_GRPC_PORT=$GRPC_PORT
VELOX_CONTROL_PLANE_REST_PUBLIC_URL=http://127.0.0.1:${MASTER_PORT}
VELOX_CONTROL_PLANE_GRPC_URL=127.0.0.1:${GRPC_PORT}
VELOX_DB_PATH=$DATA_DIR/velox.db
VELOX_DATA_DIR=$DATA_DIR
VELOX_STAGING_DIR=$STAGING_DIR
VELOX_STORAGE_DIR=$STORAGE_DIR
VELOX_ADMIN_TOKEN=$ADMIN_TOKEN
# Required by the typed artifact publication protocol (artifact.commit.v1):
# without VELOX_COMMIT_HMAC_KEY the completion coordinator is disabled at
# bootstrap, TaskOutputDeclared is rejected, and the worker waits forever
# for an ArtifactUploadPlan (ARTIFACT_UPLOAD_PLAN_WAIT_FAILED after ~4min).
# 64 hex chars = 32 raw bytes (validator minimum). Same value golden-e2e.sh
# pins, so the workload run exercises the identical wire path.
VELOX_COMMIT_HMAC_KEY=00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff
VELOX_ALLOWED_WORKERS=$WORKER_ID
VELOX_CODE_VERSION=$VERSION
VELOX_GRPC_ALLOW_INSECURE_DEV=true
VELOX_ASSET_REWRITE_DEV_BYPASS=true
# Fail-closed Level-D smoke (2026-08-10 capability audit): production wiring
# requires a real Drive service + asset resolver, neither of which exists on
# this dev sandbox. Development mode wires the documented local fakes
# (LocalShellWorker / LocalFileDriveUploader / StubAssetResolver) so the
# master boots without mTLS certs or production asset wiring.
VELOX_ENVIRONMENT=development
VELOX_SMOKE_MODE=development
GIN_MODE=release
ENV

  set -a
  # shellcheck disable=SC1090
  source "$MASTER_ENV"
  set +a
  rm -f "$MASTER_LOG"

  setsid "$MASTER_BIN" serve >"$MASTER_LOG" 2>&1 &
  local pid=$!
  echo "$pid" > "$MASTER_PIDFILE"
  push_pid "$pid"
  info "master PID=$pid"

  # Wait for healthy
  for i in $(seq 1 20); do
    if curl -fsS -o /dev/null "http://127.0.0.1:${MASTER_PORT}/health" 2>/dev/null; then
      kill -0 "$pid" 2>/dev/null || { fail "health answered after master PID exited"; exit 1; }
      ss -ltnp | grep -q "pid=${pid}," || { fail "health port is not owned by master PID=$pid"; exit 1; }
      grep -q "Velox master listening on :${MASTER_PORT}" "$MASTER_LOG" || { fail "master listener identity missing"; exit 1; }
      pass "master healthy after ${i}s"
      return 0
    fi
    sleep 1
  done
  fail "master did not become healthy"
  tail -40 "$MASTER_LOG" 2>/dev/null || true
  exit 1
}

# ═══════════════════════════════════════════════════════════════════════════════
# Phase 4: Submit job
# ═══════════════════════════════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════════════════════════════
# Phase 5: Start worker
# ═══════════════════════════════════════════════════════════════════════════════
phase_worker_start() {
  info "Phase 5: starting worker"

  # Ensure worker runtime directories exist before the agent starts.
  # The C++ engine and the Go agent write scratch data here and can fail
  # fast if the directories are missing.
  mkdir -p "$WORKDIR/runtime-temp" "$WORKDIR/runtime-output" "$WORKDIR/state"

  local bundle_hash
  bundle_hash="$(scripts/e2e/write-local-bundle-identity.sh "$WORKDIR" "$WORKER_BIN" "$ENGINE_BIN")"

  cat > "$WORKER_CFG" <<JSON
{
  "master_url": "http://127.0.0.1:${MASTER_PORT}",
  "worker_id": "${WORKER_ID}",
  "work_dir": "${WORKDIR}",
  "control_grpc_url": "127.0.0.1:${GRPC_PORT}",
  "job_delivery": "push",
  "environment": "dev",
  "allow_insecure_grpc_dev": true,
  "bundle_hash": "${bundle_hash}",
  "video_engine_cpp_bin": "${ENGINE_BIN}",
  "output_dir": "${WORKDIR}/runtime-output",
  "temp_dir": "${WORKDIR}/runtime-temp",
  "data_dir": "${WORKDIR}",
  "state_dir": "${WORKDIR}/state",
  "max_active_jobs": 1,
  "health_port": ${WORKER_HEALTH_PORT},
  "prometheus_port": ${WORKER_PROMETHEUS_PORT},
  "protocol_version": "v3"
}
JSON

  mkdir -p "$WORKDIR/tests/fixtures"
  cp "$REPO_ROOT/RemoteCodex/native/worker-agent-go/tests/fixtures/engine_selftest_baseline.sha256" \
    "$WORKDIR/tests/fixtures/engine_selftest_baseline.sha256"

  local worker_token
  worker_token="$(curl -fsS -m 10 -X POST \
    -H "Content-Type: application/json" \
    --data "{\"worker_id\":\"${WORKER_ID}\",\"worker_name\":\"e2e-worker\",\"protocol_version\":\"v3\",\"bundle_hash\":\"${bundle_hash}\"}" \
    "http://127.0.0.1:${MASTER_PORT}/api/v1/agent/register" \
    | python3 -c 'import json,sys; print(json.load(sys.stdin)["session_id"])')" \
    || { fail "worker HTTP registration/token bootstrap failed"; exit 1; }
  [[ -n "$worker_token" ]] || { fail "worker HTTP registration returned an empty token"; exit 1; }

  rm -f "$WORKER_LOG"
  # Isolate the C++ engine's temp directory so that a stale /tmp path
  # owned by another user (e.g. /tmp/velox_video_engine_plan) cannot
  # make the self-render bootstrap fail.
  setsid env TMPDIR="$WORKDIR/runtime-temp" VELOX_ENV=dev VELOX_ALLOW_INSECURE_GRPC_DEV=true WORKER_TOKEN="$worker_token" "$WORKER_BIN" --config "$WORKER_CFG" \
    >"$WORKER_LOG" 2>&1 &
  local pid=$!
  echo "$pid" > "$WORKER_PIDFILE"
  push_pid "$pid"
  info "worker PID=$pid"

  wait_for_worker_registration "$pid"
}

# ═══════════════════════════════════════════════════════════════════════════════
# Phase 6: Poll + verify
# ═══════════════════════════════════════════════════════════════════════════════
phase_poll_and_verify() {
  info "Phase 6: polling for SUCCEEDED (max 5 min)"
  local db="$DATA_DIR/velox.db"
  local status=""

  for i in $(seq 1 60); do
    status="$(sqlite3 "$db" "SELECT status FROM jobs WHERE job_id='${JOB_ID}';" 2>/dev/null || true)"
    case "$status" in
      SUCCEEDED)
        pass "job SUCCEEDED after ~$(( i * 5 ))s"
        break
        ;;
      FAILED|TIMEOUT|REJECTED|CANCELLED)
        fail "job reached terminal status=$status (expected SUCCEEDED)"
        exit 1
        ;;
    esac
    if (( i % 6 == 0 )); then
      info "  poll[$i/60] status=$status (elapsed=$(( i * 5 ))s)"
    fi
    sleep 5
  done

  if [[ "$status" != "SUCCEEDED" ]]; then
    fail "job did not reach SUCCEEDED within 5 min"
    sqlite3 "$db" "SELECT job_id, status, updated_at FROM jobs WHERE job_id='${JOB_ID}';" || true
    exit 1
  fi

  assert_artifact_exists
  assert_video_properties
  assert_artifact_sha256

  info "Verification 4: GET /api/v1/workers"
  local workers_json
  workers_json="$(curl -sS -m 5 -H "Authorization: Bearer ${ADMIN_TOKEN}"     "http://127.0.0.1:${MASTER_PORT}/api/v1/workers" 2>/dev/null || true)"
  if echo "$workers_json" | grep -qF "$WORKER_ID"; then
    pass "worker '$WORKER_ID' visible in /api/v1/workers"
  else
    fail "worker '$WORKER_ID' NOT in /api/v1/workers"
    info "response: $workers_json"
    exit 1
  fi

  assert_master_metrics
  assert_database_state
  assert_worker_metrics
}

# ═══════════════════════════════════════════════════════════════════════════════
# Phase 6a: live attempt-milestone projection (STEP A)
# ═══════════════════════════════════════════════════════════════════════════════
# While the job is RUNNING, the worker publishes the monotonic milestone
# timeline (execution.started → assets.requested → assets.all_ready → ...)
# in every heartbeat, and the master folds it into the volatile
# worker_task_runtime projection. This phase polls the /live admin endpoint
# every second and requires at least one non-empty attempt_milestones sample
# to appear BEFORE the job reaches a terminal status — proving the milestone
# timeline is visible live, not only after the durable report lands.
phase_live_milestones() {
  info "Phase 6a: capturing attempt_milestones from the LIVE projection"
  local db="$DATA_DIR/velox.db"
  local status=""
  local milestones=""
  for i in $(seq 1 90); do
    status="$(sqlite3 "$db" "SELECT status FROM jobs WHERE job_id='${JOB_ID}';" 2>/dev/null || true)"
    case "$status" in
      SUCCEEDED|FAILED|TIMEOUT|REJECTED|CANCELLED) break ;;
    esac
    local live
    live="$(curl -sS -m 5 -H "Authorization: Bearer ${ADMIN_TOKEN}" \
      "http://127.0.0.1:${MASTER_PORT}/api/v1/admin/jobs/${JOB_ID}/live" 2>/dev/null || true)"
    if echo "$live" | grep -q '"attempt_milestones"'; then
      milestones="$(echo "$live" | python3 -c '
import sys, json
d = json.load(sys.stdin)
ms = (d.get("execution") or {}).get("attempt_milestones") or []
for m in ms:
    print("{} @ {}ms".format(m.get("name"), m.get("elapsed_ms")))
' 2>/dev/null || true)"
      if [[ -n "$milestones" ]]; then
        break
      fi
    fi
    sleep 1
  done

  if [[ -z "$milestones" ]]; then
    fail "attempt_milestones never appeared in the /live projection while RUNNING (final status=$status)"
    tail -30 "$WORKER_LOG" 2>/dev/null || true
    exit 1
  fi
  pass "live attempt_milestones captured while job status=$status"
  info "milestone timeline (elapsed_ms since attempt start):"
  while IFS= read -r line; do
    [[ -n "$line" ]] && info "  $line"
  done <<< "$milestones"
}

main() {
  echo ""
  echo "══════════════════════════════════════════════════════════════"
  echo "  PR 5 — Velox Real Workload E2E"
  echo "══════════════════════════════════════════════════════════════"
  echo ""
  info "workdir  = $WORKDIR"
  info "version  = $VERSION"
  info "binaries = $MASTER_BIN / $WORKER_BIN"
  echo ""

  # Phase 0 — pre-flight dependency checks.
  local missing=0
  for dep in go ffmpeg sqlite3 python3; do
    command -v "$dep" >/dev/null 2>&1 || {
      fail "$dep not found — install before running make e2e-workload"; missing=1; }
  done
  (( missing == 0 )) || exit 3

  phase_build
  phase_fixtures
  phase_master_start
  phase_submit
  phase_worker_start
  phase_live_milestones
  phase_poll_and_verify

  echo ""
  echo "══════════════════════════════════════════════════════════════"
  pass "ALL VERIFICATIONS PASSED — Velox E2E workload complete"
  echo "══════════════════════════════════════════════════════════════"
  echo ""
}

main "$@"
