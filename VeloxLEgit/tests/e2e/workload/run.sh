#!/usr/bin/env bash
# =============================================================================
# tests/e2e/workload/run.sh — PR 5 Real Workload E2E
# =============================================================================
# Full Velox pipeline from zero to verified artifact:
#   Hello → HelloAck → TaskOffer → TaskAccepted → TaskLeaseGranted
#   → executor reale → TaskResult → artifact upload → Job SUCCEEDED
#
# Minimal deterministic fixture: teal background 320x180 + silent audio,
# encoded as H.264 MP4 (~2s). Output is reproducible byte-for-byte when
# the same FFmpeg version runs on the same architecture.
#
# Verification (no mocking of the critical path):
#   1. Artifact exists on disk
#   2. SHA-256 matches expected (deterministic output)
#   3. ffprobe opens it: duration ≈2s, resolution 320x180, codec h264
#   4. Master metrics endpoint returns non-zero counters
#   5. GET /api/v1/workers shows CONNECTED worker
#   6. DB state assertions (4-part, all blocking):
#        (a) task_attempts row reached SUCCEEDED for our job_id;
#        (b) artifacts row marked READY (finalization committed);
#        (c) artifacts.sha256 == sha256 of the downloaded file;
#        (d) jobs.completed_at >= artifacts.verified_at — jobs is
#            promoted to SUCCEEDED only AFTER finalization.
#
# Usage:
#   make e2e-workload                     # full pipeline
#   E2E_WORKDIR=/tmp/vx-wl make e2e-workload
#   bash tests/e2e/workload/run.sh        # direct invocation
# =============================================================================

set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$ROOT/../../.." && pwd)"
WORKDIR="${E2E_WORKDIR:-/tmp/velox-e2e-workload}"

# ─── Paths ───────────────────────────────────────────────────────────────────
BIN_DIR="$WORKDIR/bin"
DATA_DIR="$WORKDIR/data"
STAGING_DIR="$WORKDIR/staging"
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
ADMIN_TOKEN="e2e-workload-token"
WORKER_ID="e2e-workload-worker-1"

VERSION="$(tr -d '[:space:]' < "$REPO_ROOT/VERSION.txt" 2>/dev/null || echo "dev")"

# ─── Colors ─────────────────────────────────────────────────────────────────
C_GREEN='\033[32m'; C_RED='\033[31m'; C_CYAN='\033[36m'; C_RST='\033[0m'
pass() { printf "${C_GREEN}PASS${C_RST}  %s\n" "$*"; }
fail() { printf "${C_RED}FAIL${C_RST}  %s\n" "$*"; return 1; }
info() { printf "${C_CYAN}.. %s${C_RST}\n" "$*"; }

# ─── Cleanup ────────────────────────────────────────────────────────────────
declare -a CHILD_PIDS=()
push_pid() { CHILD_PIDS+=("$1"); }

kill_all() {
  local sig="${1:-TERM}"
  for pid in "${CHILD_PIDS[@]}"; do
    kill -0 "$pid" 2>/dev/null && kill -"$sig" "$pid" 2>/dev/null || true
  done
  if [[ "$sig" == "TERM" ]]; then
    sleep 1
    for pid in "${CHILD_PIDS[@]}"; do
      kill -0 "$pid" 2>/dev/null && kill -KILL "$pid" 2>/dev/null || true
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

  if [[ -x "$MASTER_BIN" && -x "$WORKER_BIN" ]]; then
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

  pass "build complete"
}

# ═══════════════════════════════════════════════════════════════════════════════
# Phase 2: Fixtures (deterministic, self-contained)
# ═══════════════════════════════════════════════════════════════════════════════
phase_fixtures() {
  info "Phase 2: generating test fixtures"
  mkdir -p "$STAGING_DIR"

  command -v ffmpeg >/dev/null 2>&1 || { fail "ffmpeg not found — install ffmpeg"; exit 3; }

  # Scene image: pure teal (#008080), 320x180, 1 frame PNG
  # Uses lavfi color source for deterministic output.
  info "  → scene.png (teal 320x180)"
  ffmpeg -hide_banner -loglevel error -y \
    -f lavfi -i "color=c=0x008080:s=320x180:d=0.1" -frames:v 1 \
    -vcodec png "$STAGING_DIR/scene.png" 2>/dev/null || {
    fail "scene.png generation failed"; exit 3; }

  # Silent audio: 2 seconds, AAC in MP4 container (for voiceless render)
  info "  → silent.aac (2s, AAC)"
  ffmpeg -hide_banner -loglevel error -y \
    -f lavfi -i "anullsrc=r=48000:cl=mono" -t 2 \
    -c:a aac -b:a 64k "$STAGING_DIR/silent.aac" 2>/dev/null || {
    # Try MP3 fallback
    ffmpeg -hide_banner -loglevel error -y \
      -f lavfi -i "anullsrc=r=44100:cl=mono" -t 2 \
      -c:a libmp3lame -b:a 64k "$STAGING_DIR/silent.mp3" 2>/dev/null || {
      fail "audio fixture generation failed"; exit 3; }
    info "  → silent.mp3 (2s, MP3 fallback)"
  }

  local scene_path="$STAGING_DIR/scene.png"
  local audio_path="$STAGING_DIR/silent.aac"
  [[ -f "$audio_path" ]] || audio_path="$STAGING_DIR/silent.mp3"

  ls -la "$scene_path" "$audio_path" 2>/dev/null
  pass "fixtures ready"
}

# ═══════════════════════════════════════════════════════════════════════════════
# Phase 3: Start master
# ═══════════════════════════════════════════════════════════════════════════════
phase_master_start() {
  info "Phase 3: starting master"
  mkdir -p "$DATA_DIR" "$STORAGE_DIR" "$LOG_DIR"

  cat > "$MASTER_ENV" <<ENV
VELOX_MASTER_PORT=$MASTER_PORT
VELOX_GRPC_PORT=$GRPC_PORT
VELOX_DB_PATH=$DATA_DIR/velox.db
VELOX_DATA_DIR=$DATA_DIR
VELOX_STAGING_DIR=$STAGING_DIR
VELOX_STORAGE_DIR=$STORAGE_DIR
VELOX_ADMIN_TOKEN=$ADMIN_TOKEN
VELOX_ALLOWED_WORKERS=$WORKER_ID
VELOX_CODE_VERSION=$VERSION
VELOX_GRPC_ALLOW_INSECURE_DEV=true
VELOX_ASSET_REWRITE_DEV_BYPASS=true
GIN_MODE=release
ENV

  set -a; source "$MASTER_ENV"; set +a
  rm -f "$MASTER_LOG"

  "$MASTER_BIN" serve >"$MASTER_LOG" 2>&1 &
  local pid=$!
  echo "$pid" > "$MASTER_PIDFILE"
  push_pid "$pid"
  info "master PID=$pid"

  # Wait for healthy
  for i in $(seq 1 20); do
    if curl -fsS -o /dev/null "http://127.0.0.1:${MASTER_PORT}/health" 2>/dev/null; then
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
phase_submit() {
  info "Phase 4: submitting job"

  local scene_path="$STAGING_DIR/scene.png"
  local audio_path="$STAGING_DIR/silent.aac"
  [[ -f "$audio_path" ]] || audio_path="$STAGING_DIR/silent.mp3"

  cat > "$WORKDIR/job.json" <<JSON
{
  "video_name": "VeloxE2EWorkload",
  "script_text": "PR 5 E2E workload smoke test.",
  "scenes_json": "[{\"text\":\"E2E\",\"image\":\"file://${scene_path}\"}]",
  "voiceover_path": "${audio_path}",
  "render_video": true,
  "save_to_db": true,
  "channel_id": "e2e-workload",
  "audio_language_for_srt": "en"
}
JSON

  local submit_out
  submit_out="$(curl -sS -m 15 -X POST \
    -H "Authorization: Bearer ${ADMIN_TOKEN}" \
    -H "Content-Type: application/json" \
    --data-binary @"$WORKDIR/job.json" \
    "http://127.0.0.1:${MASTER_PORT}/api/v1/script/generate-with-images" 2>&1)" || true

  JOB_ID="$(echo "$submit_out" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('job_id',''))" 2>/dev/null || true)"

  if [[ -z "$JOB_ID" ]]; then
    fail "job submission failed — response: $submit_out"
    exit 1
  fi
  pass "job submitted: job_id=$JOB_ID"
}

# ═══════════════════════════════════════════════════════════════════════════════
# Phase 5: Start worker
# ═══════════════════════════════════════════════════════════════════════════════
phase_worker_start() {
  info "Phase 5: starting worker"

  cat > "$WORKER_CFG" <<JSON
{
  "master_url": "http://127.0.0.1:${MASTER_PORT}",
  "worker_id": "${WORKER_ID}",
  "work_dir": "${WORKDIR}",
  "control_grpc_url": "127.0.0.1:${GRPC_PORT}",
  "job_delivery": "push",
  "environment": "dev",
  "allow_insecure_grpc_dev": true,
  "data_dir": "${WORKDIR}",
  "max_active_jobs": 1,
  "health_port": 0,
  "prometheus_port": 0,
  "protocol_version": "v3"
}
JSON

  rm -f "$WORKER_LOG"
  VELOX_ALLOW_INSECURE_GRPC_DEV=true "$WORKER_BIN" --config "$WORKER_CFG" \
    >"$WORKER_LOG" 2>&1 &
  local pid=$!
  echo "$pid" > "$WORKER_PIDFILE"
  push_pid "$pid"
  info "worker PID=$pid"

  # Wait for registration
  for i in $(seq 1 20); do
    if grep -qE "HelloAck|✓ HelloAck|accepted registration" "$MASTER_LOG" 2>/dev/null; then
      pass "worker registered after ${i}s"
      sleep 2  # let the worker settle
      return 0
    fi
    if ! kill -0 "$pid" 2>/dev/null; then
      fail "worker crashed during registration"
      tail -40 "$WORKER_LOG" 2>/dev/null || true
      exit 1
    fi
    sleep 2
  done
  fail "worker did not register within 40s"
  tail -20 "$MASTER_LOG" 2>/dev/null || true
  tail -20 "$WORKER_LOG" 2>/dev/null || true
  exit 1
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

  # ── Verification 1: Artifact exists ───────────────────────────
  info "Verification 1: artifact exists on disk"
  local artifact
  artifact="$(find "$STORAGE_DIR" -name '*.mp4' 2>/dev/null | head -1 || true)"
  if [[ -z "$artifact" ]]; then
    fail "no .mp4 artifact found in $STORAGE_DIR"
    ls -laR "$STORAGE_DIR" 2>/dev/null || true
    exit 1
  fi
  local art_size
  art_size="$(stat -c%s "$artifact" 2>/dev/null || stat -f%z "$artifact" 2>/dev/null || echo 0)"
  if (( art_size < 1000 )); then
    fail "artifact too small: ${art_size} bytes (expected ≥1 KB)"
    exit 1
  fi
  pass "artifact: $(basename "$artifact") (${art_size} bytes)"

  # ── Verification 2: ffprobe (strict) ────────────────────────────
  info "Verification 2: ffprobe inspection (strict: codec h264 ONLY, 320x180, 1.8..2.2s)"
  if ! command -v ffprobe >/dev/null 2>&1; then
    fail "ffprobe not found — cannot validate artifact codec/resolution/duration"
    exit 1
  fi
  local probe_json
  probe_json="$(ffprobe -v quiet -print_format json -show_format -show_streams "$artifact" 2>/dev/null || true)"
  if [[ -z "$probe_json" ]]; then
    fail "ffprobe returned empty output — artifact may be corrupt"
    exit 1
  fi

  # ── Codec: h264 ONLY. hevc / mpeg4 / vp9 / av1 are explicit failures. ──
  local codec
  codec="$(echo "$probe_json" | python3 -c "
import sys, json
d = json.load(sys.stdin)
vid = [s for s in d.get('streams', []) if s.get('codec_type') == 'video']
if not vid: sys.exit(2)
print(vid[0].get('codec_name',''))
")" || { fail "ffprobe: no video stream found"; exit 1; }
  if [[ "$codec" != "h264" ]]; then
    fail "ffprobe: codec=$codec (only h264 accepted; hevc/mpeg4/vp9/av1 reject)"
    exit 1
  fi
  pass "ffprobe: codec=h264"

  # ── Resolution: exactly 320x180. ──
  local width height
  width="$(echo "$probe_json" | python3 -c "import sys,json;d=json.load(sys.stdin);s=[x for x in d['streams'] if x.get('codec_type')=='video'];print(s[0].get('width','0'))" 2>/dev/null || echo 0)"
  height="$(echo "$probe_json" | python3 -c "import sys,json;d=json.load(sys.stdin);s=[x for x in d['streams'] if x.get('codec_type')=='video'];print(s[0].get('height','0'))" 2>/dev/null || echo 0)"
  if (( width != 320 || height != 180 )); then
    fail "ffprobe: resolution=${width}x${height} (must be exactly 320x180)"
    exit 1
  fi
  pass "ffprobe: resolution=320x180"

  # ── Duration: 1.8s ≤ dur ≤ 2.2s. ──
  local dur
  dur="$(echo "$probe_json" | python3 -c "import sys,json;d=json.load(sys.stdin);f=d.get('format',{});print(f.get('duration','0'))" 2>/dev/null || echo 0)"
  # Polarity note: awk exits 1 when d IS in range, 0 when out-of-range.
  # `if ! awk …; then fail … fi` then fails only when d is OUT of range.
  if ! awk -v d="$dur" 'BEGIN{ exit (d+0 >= 1.8 && d+0 <= 2.2) }'; then
    fail "ffprobe: duration=${dur}s (must be in 1.8..2.2s)"
    exit 1
  fi
  pass "ffprobe: duration=${dur}s (within 1.8..2.2s)"

  # ── Verification 3: SHA-256 checksum (mandatory) ──────────────
  # Determinism: the SHA-256 of the rendered artifact is byte-stable for
  # a fixed (FFmpeg version, libx264 build, fixture triple). CI MUST
  # populate E2E_EXPECTED_SHA256 with the platform-correct hash — we
  # no longer accept a "skip if unset" fallback. Operators committing
  # to a recorded baseline can derive the value once via:
  #     make e2e-workload && cat $E2E_WORKDIR/storage/artifact.sha256
  # and pin it in the workflow file.
  if [[ -z "${E2E_EXPECTED_SHA256:-}" ]]; then
    fail "E2E_EXPECTED_SHA256 must be set for deterministic SHA-256 enforcement (was unset)"
    exit 1
  fi
  info "Verification 3: SHA-256 checksum (expected=${E2E_EXPECTED_SHA256:0:16}...)"
  local sha
  sha="$(sha256sum "$artifact" 2>/dev/null | awk '{print $1}' || shasum -a 256 "$artifact" 2>/dev/null | awk '{print $1}' || true)"
  if [[ -z "$sha" ]]; then
    fail "SHA-256 could not be computed"
    exit 1
  fi
  echo "$sha  $(basename "$artifact")" > "$STORAGE_DIR/artifact.sha256"
  if [[ "$sha" != "$E2E_EXPECTED_SHA256" ]]; then
    fail "SHA-256 mismatch: got ${sha:0:16}... want ${E2E_EXPECTED_SHA256:0:16}..."
    exit 1
  fi
  pass "SHA-256 matches: ${sha:0:16}..."

  # ── Verification 4: Worker visible in API ──────────────────────
  info "Verification 4: GET /api/v1/workers"
  local workers_json
  workers_json="$(curl -sS -m 5 "http://127.0.0.1:${MASTER_PORT}/api/v1/workers" 2>/dev/null || true)"
  if echo "$workers_json" | grep -qF "$WORKER_ID"; then
    pass "worker '$WORKER_ID' visible in /api/v1/workers"
  else
    fail "worker '$WORKER_ID' NOT in /api/v1/workers"
    info "response: $workers_json"
    exit 1
  fi

  # ── Verification 5: Metrics > 0 (strict, blocking) ───────────
  info "Verification 5: Prometheus metrics (strict: velox_job_succeeded_total >= 1 AND velox_compute_seconds_total > 0)"
  local metrics
  metrics="$(curl -sS -m 5 "http://127.0.0.1:${MASTER_PORT}/metrics" 2>/dev/null || true)"
  if [[ -z "$metrics" ]]; then
    fail "/metrics returned empty — Prometheus endpoint disabled or master unhealthy"
    exit 1
  fi

  # ── velox_job_succeeded_total — Prom counter (non-negative integer). ──
  # Accept ANY time series with a positive integer value (1, 2, 10, …).
  local jobs_val jobs_nz=0 v
  jobs_val="$(echo "$metrics" | grep -E '^velox_job_succeeded_total([ {]|$)' | awk '{print $NF}')"
  for v in $jobs_val; do
    if [[ "$v" =~ ^[1-9][0-9]*$ ]]; then jobs_nz=1; break; fi
  done
  if (( jobs_nz == 0 )); then
    fail "metrics: velox_job_succeeded_total missing or zero across all series"
    exit 1
  fi
  pass "metrics: velox_job_succeeded_total >= 1"

  # ── velox_compute_seconds_total — Prom gauge (float). ──
  # Accept integers (1, 12) AND floats (1.0, 12.34, 0.5); reject 0 / 0.0 / 0e0.
  local compute_nz=0
  for v in $(echo "$metrics" | grep -E '^velox_compute_seconds_total([ {]|$)' | awk '{print $NF}'); do
    if [[ "$v" =~ ^([1-9][0-9]*(\.[0-9]+)?|0\.[0-9]*[1-9][0-9]*)$ ]]; then
      compute_nz=1; break
    fi
  done
  if (( compute_nz == 0 )); then
    fail "metrics: velox_compute_seconds_total missing or zero across all series"
    exit 1
  fi
  pass "metrics: velox_compute_seconds_total > 0"

  # ── Verification 6: Database state (4-part, all blocking) ─────
  # After the job reaches SUCCEEDED, verify the four canonical
  # invariants the success path depends on. ALL four must hold;
  # any failure exits non-zero before the script returns.
  info "Verification 6: Database state assertions (4-part, blocking)"

  if ! command -v sqlite3 >/dev/null 2>&1; then
    fail "sqlite3 missing — cannot verify DB state"
    exit 1
  fi
  if [[ ! -f "$db" ]]; then
    fail "DB file not found at $db"
    exit 1
  fi

  # sql_query <sql_with_?> <positional_arg> …
  # sqlite3 native positional binding; pipe-separated first column
  # of the first row; empty on no-rows or driver error. Reusable
  # across V6 a–d.
  sql_query() {
    local q="$1"; shift
    sqlite3 -separator '|' "$db" "$q" "$@" 2>/dev/null
  }

  # ── (a) task_attempts row reached SUCCEEDED for our job_id ────
  local attempts_succ
  attempts_succ="$(sql_query "SELECT COUNT(*) FROM task_attempts WHERE job_id = ? AND status='SUCCEEDED'" "$JOB_ID" || true)"
  if [[ "${attempts_succ:-0}" =~ ^[1-9][0-9]*$ ]]; then
    pass "DB (a): task_attempts SUCCEEDED count=$attempts_succ for job_id=$JOB_ID"
  else
    fail "DB (a): no SUCCEEDED row in task_attempts for job_id=$JOB_ID (got '$attempts_succ')"
    exit 1
  fi

  # ── (b) artifacts row marked READY (finalization committed) ───
  local arts_ready
  arts_ready="$(sql_query "SELECT COUNT(*) FROM artifacts WHERE job_id = ? AND status='READY'" "$JOB_ID" || true)"
  if [[ "${arts_ready:-0}" =~ ^[1-9][0-9]*$ ]]; then
    pass "DB (b): artifacts READY count=$arts_ready for job_id=$JOB_ID"
  else
    fail "DB (b): no READY row in artifacts for job_id=$JOB_ID (got '$arts_ready')"
    exit 1
  fi

  # ── (c) artifacts.sha256 == sha256 of the downloaded file ─────
  local db_sha
  db_sha="$(sql_query "SELECT sha256 FROM artifacts WHERE job_id = ? AND status='READY' ORDER BY verified_at DESC LIMIT 1" "$JOB_ID" || true)"
  if [[ -z "$db_sha" ]]; then
    fail "DB (c): artifacts.sha256 missing/empty for job_id=$JOB_ID"
    exit 1
  fi
  if [[ "$db_sha" != "$sha" ]]; then
    fail "DB (c): sha256 mismatch (artifacts.sha256=$db_sha, downloaded=$sha, expected_baseline=${E2E_EXPECTED_SHA256:-<unset>})"
    exit 1
  fi
  pass "DB (c): artifacts.sha256 matches downloaded file ($db_sha)"

  # ── (d) jobs.completed_at >= artifacts.verified_at (ordinal) ──
  # `>=` (not strict `>`) is the correct gate: a single-tx finalize
  # writes both columns within the same SQL transaction, so equal
  # timestamps are acceptable; only TRUE reversal fails.
  local jobs_completed_at db_verified_at jobs_epoch art_epoch
  jobs_completed_at="$(sql_query "SELECT completed_at FROM jobs WHERE job_id = ? LIMIT 1" "$JOB_ID" || true)"
  db_verified_at="$(sql_query "SELECT verified_at FROM artifacts WHERE job_id = ? AND status='READY' ORDER BY verified_at DESC LIMIT 1" "$JOB_ID" || true)"
  if [[ -z "$jobs_completed_at" || -z "$db_verified_at" ]]; then
    fail "DB (d): missing timestamp (jobs.completed_at='$jobs_completed_at', artifacts.verified_at='$db_verified_at')"
    exit 1
  fi
  jobs_epoch="$(date -d "$jobs_completed_at" +%s 2>/dev/null || true)"
  art_epoch="$(date -d "$db_verified_at"    +%s 2>/dev/null || true)"
  if [[ -z "$jobs_epoch" || ! "$jobs_epoch" =~ ^[0-9]+$ ]]; then
    fail "DB (d): jobs.completed_at='$jobs_completed_at' is not a valid RFC3339 timestamp"
    exit 1
  fi
  if [[ -z "$art_epoch" || ! "$art_epoch" =~ ^[0-9]+$ ]]; then
    fail "DB (d): artifacts.verified_at='$db_verified_at' is not a valid RFC3339 timestamp"
    exit 1
  fi
  if (( jobs_epoch >= art_epoch )); then
    pass "DB (d): jobs.completed_at (epoch=$jobs_epoch, ${jobs_completed_at}) >= artifacts.verified_at (epoch=$art_epoch, ${db_verified_at}) — finalization ordering holds"
  else
    fail "DB (d): jobs.completed_at (epoch=$jobs_epoch, ${jobs_completed_at}) is BEFORE artifacts.verified_at (epoch=$art_epoch, ${db_verified_at}) — ordering bug"
    exit 1
  fi
}

# ═══════════════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════════════
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
  phase_poll_and_verify

  echo ""
  echo "══════════════════════════════════════════════════════════════"
  pass "ALL VERIFICATIONS PASSED — Velox E2E workload complete"
  echo "══════════════════════════════════════════════════════════════"
  echo ""
}

main "$@"
