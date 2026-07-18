#!/usr/bin/env bash
# =============================================================================
# tests/e2e/workload-mtls/run.sh — PR 7 mTLS Workload E2E (staging)
# =============================================================================
# Same end-to-end workload as tests/e2e/workload/run.sh (PR 5), BUT the
# gRPC control plane tunnels through TLS using a real cert+key pair from
# scripts/gen-worker-certs.sh. Worker environment = "staging" (NOT dev,
# NOT production). Fail-closed: missing/invalid certs ⇒ exit non-zero.
# NO insecure fallback is accepted at any layer.
#
# Fail-closed contracts (DO NOT WEAKEN):
#   1. Master env does NOT set VELOX_GRPC_ALLOW_INSECURE_DEV. Combined
#      with VELOX_RELEASE_CHANNEL=staging, this triggers log.Fatalf in
#      DataServer/cmd/server/bootstrap.go:301 (PR-5 P0 guard).
#   2. Worker config has tls_cert_file/tls_key_file/tls_ca_file WITH
#      environment=staging AND NO allow_insecure_grpc_dev. The worker
#      transport factory at RemoteCodex/.../pkg/config/config.go:351
#      rejects any other combination on a non-dev channel.
#   3. phase_certs refuses to continue if any of ca.crt / server.crt /
#      server.key / worker.crt / worker.key is missing/empty, AND if
#      worker.crt's CN does not equal WORKER_ID (the cert contract
#      that hooks into PR 4's CN-as-worker-id binding).
#
# Verification (same V1-V6 as PR 5; ALL blocking):
#   1. Artifact exists on disk
#   2. SHA-256 matches E2E_EXPECTED_SHA256 (mandatory)
#   3. ffprobe (strict: h264 ONLY, 320x180, 1.8..2.2s)
#   4. /api/v1/workers shows the worker registered (over mTLS)
#   5. Prometheus metrics: velox_job_succeeded_total >= 1
#      AND velox_compute_seconds_total > 0
#   6. DB state 4-part: attempts SUCCEEDED, artifacts READY,
#      sha256 == downloaded, jobs.completed_at >= artifacts.verified_at
#
# Usage:
#   make e2e-workload-mtls                          # full pipeline
#   E2E_WORKDIR=/tmp/vx-wl-mtls make e2e-workload-mtls
#   E2E_EXPECTED_SHA256=… make e2e-workload-mtls    # CI pinning
# =============================================================================

set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$ROOT/../../.." && pwd)"
WORKDIR="${E2E_WORKDIR:-/tmp/velox-e2e-workload-mtls}"
GEN_CERTS="$REPO_ROOT/scripts/gen-worker-certs.sh"

# ─── Paths ───────────────────────────────────────────────────────────────────
BIN_DIR="$WORKDIR/bin"
DATA_DIR="$WORKDIR/data"
STAGING_DIR="$WORKDIR/staging"
STORAGE_DIR="$WORKDIR/storage"
LOG_DIR="$WORKDIR/logs"
CERTS_DIR="$WORKDIR/certs"

MASTER_BIN="$BIN_DIR/velox-server"
WORKER_BIN="$BIN_DIR/velox-worker-agent"
MASTER_LOG="$LOG_DIR/master.log"
WORKER_LOG="$LOG_DIR/worker.log"
MASTER_ENV="$WORKDIR/master.env"
WORKER_CFG="$WORKDIR/worker.json"
MASTER_PIDFILE="$WORKDIR/master.pid"
WORKER_PIDFILE="$WORKDIR/worker.pid"

CA_CRT="$CERTS_DIR/ca.crt"
SERVER_CRT="$CERTS_DIR/server.crt"
SERVER_KEY="$CERTS_DIR/server.key"
WORKER_CRT="$CERTS_DIR/worker.crt"
WORKER_KEY="$CERTS_DIR/worker.key"

MASTER_PORT="${E2E_MASTER_PORT:-8080}"
GRPC_PORT="${E2E_GRPC_PORT:-50051}"
ADMIN_TOKEN="e2e-workload-mtls-token"
WORKER_ID="e2e-workload-mtls-worker-1"
DATASERVER_ROOT="${DATASERVER_ROOT:-$REPO_ROOT/DataServer}"
WORKERAGENT_ROOT="${WORKERAGENT_ROOT:-$REPO_ROOT/RemoteCodex/native/worker-agent-go}"

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
# Phase 0 — mTLS certs (NEW; replaces plaintext path)
# ═══════════════════════════════════════════════════════════════════════════════
phase_certs() {
  info "Phase 0: generating mTLS test certs (scripts/gen-worker-certs.sh)"
  mkdir -p "$CERTS_DIR"

  if [[ ! -x "$GEN_CERTS" ]]; then
    fail "scripts/gen-worker-certs.sh missing or not executable at $GEN_CERTS"
    exit 8
  fi
  if ! "$GEN_CERTS" "$CERTS_DIR" "$WORKER_ID"; then
    fail "gen-worker-certs.sh failed (cert generation)"
    exit 8
  fi

  # Fail-closed: every expected file must exist AND be non-empty.
  local f
  for f in "$CA_CRT" "$SERVER_CRT" "$SERVER_KEY" "$WORKER_CRT" "$WORKER_KEY"; do
    if [[ ! -s "$f" ]]; then
      fail "$f missing or empty after cert generation"
      exit 8
    fi
  done

  # Cert contract: worker.crt CN MUST equal WORKER_ID (PR 4 uses CN as
  # worker-id during handshake). Refuse to proceed on mismatch.
  local cn
  cn="$(openssl x509 -in "$WORKER_CRT" -subject -noout 2>/dev/null | sed -n 's/.*CN *= *//p' || true)"
  if [[ -z "$cn" ]]; then
    fail "could not extract CN from $WORKER_CRT — cert malformed"
    exit 8
  fi
  if [[ "$cn" != "$WORKER_ID" ]]; then
    fail "worker.crt CN='$cn' != WORKER_ID='$WORKER_ID' (cert contract requires CN=worker_id)"
    exit 8
  fi

  pass "Phase 0: certs present and valid (CA=$(basename "$CA_CRT"), server.crt CN=localhost, worker.crt CN=$cn)"
}

# ═══════════════════════════════════════════════════════════════════════════════
# Phase 1 — Build (same as PR 5)
# ═══════════════════════════════════════════════════════════════════════════════
phase_build() {
  info "Phase 1: building binaries"
  mkdir -p "$BIN_DIR"

  if [[ -x "$MASTER_BIN" && -x "$WORKER_BIN" ]]; then
    info "binaries already built — skipping"
    return 0
  fi

  info "  → velox-server"
  (cd "$DATASERVER_ROOT" && go build -o "$MASTER_BIN" \
    -ldflags "-s -w -X main.Version=$VERSION" ./cmd/server) || {
    fail "master build failed"; exit 2; }

  info "  → velox-worker-agent"
  (cd "$WORKERAGENT_ROOT" && \
    go build -o "$WORKER_BIN" -ldflags "-s -w" ./cmd/velox-worker-agent) || {
    fail "worker build failed"; exit 2; }

  pass "build complete"
}

# ═══════════════════════════════════════════════════════════════════════════════
# Phase 2 — Fixtures (same as PR 5)
# ═══════════════════════════════════════════════════════════════════════════════
phase_fixtures() {
  info "Phase 2: generating test fixtures"
  mkdir -p "$STAGING_DIR"

  command -v ffmpeg >/dev/null 2>&1 || { fail "ffmpeg not found — install ffmpeg"; exit 3; }

  info "  → scene.png (teal 320x180)"
  ffmpeg -hide_banner -loglevel error -y \
    -f lavfi -i "color=c=0x008080:s=320x180:d=0.1" -frames:v 1 \
    -vcodec png "$STAGING_DIR/scene.png" 2>/dev/null || {
    fail "scene.png generation failed"; exit 3; }

  info "  → silent.aac (2s, AAC)"
  ffmpeg -hide_banner -loglevel error -y \
    -f lavfi -i "anullsrc=r=48000:cl=mono" -t 2 \
    -c:a aac -b:a 64k "$STAGING_DIR/silent.aac" 2>/dev/null || {
    ffmpeg -hide_banner -loglevel error -y \
      -f lavfi -i "anullsrc=r=44100:cl=mono" -t 2 \
      -c:a libmp3lame -b:a 64k "$STAGING_DIR/silent.mp3" 2>/dev/null || {
      fail "audio fixture generation failed"; exit 3; }
    info "  → silent.mp3 (2s, MP3 fallback)"
  }

  pass "fixtures ready"
}

# ═══════════════════════════════════════════════════════════════════════════════
# Phase 3 — Start master WITH mTLS env (no insecure escape hatch)
# ═══════════════════════════════════════════════════════════════════════════════
phase_master_start() {
  info "Phase 3: starting master (mTLS, channel=staging)"
  mkdir -p "$DATA_DIR" "$STORAGE_DIR" "$LOG_DIR"

  # Note the DELIBERATE ABSENCE of VELOX_GRPC_ALLOW_INSECURE_DEV — this
  # is the fail-closed gate that bootstrap.go:301 enforces. Setting both
  # VELOX_RELEASE_CHANNEL=staging AND VELOX_GRPC_ALLOW_INSECURE_DEV=true
  # would log.Fatalf immediately.
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
# mTLS gRPC control plane (server side).
VELOX_GRPC_TLS_CERT_FILE=$SERVER_CRT
VELOX_GRPC_TLS_KEY_FILE=$SERVER_KEY
VELOX_GRPC_TLS_CA_FILE=$CA_CRT
# Release channel = staging (PR-5 P0 fail-closed: insecure=true implies log.Fatalf here).
VELOX_RELEASE_CHANNEL=staging
# Asset rewrite: dev-bypass is dev-only; in staging we want the prod path.
VELOX_ASSET_REWRITE_DEV_BYPASS=false
GIN_MODE=release
ENV

  set -a; source "$MASTER_ENV"; set +a
  rm -f "$MASTER_LOG"

  "$MASTER_BIN" serve >"$MASTER_LOG" 2>&1 &
  local pid=$!
  echo "$pid" > "$MASTER_PIDFILE"
  push_pid "$pid"
  info "master PID=$pid"

  for i in $(seq 1 30); do
    if curl -fsS -o /dev/null "http://127.0.0.1:${MASTER_PORT}/health" 2>/dev/null; then
      pass "master healthy after ${i}s (REST on :${MASTER_PORT}, gRPC+mTLS on :${GRPC_PORT})"
      return 0
    fi
    sleep 1
  done
  fail "master did not become healthy"
  tail -40 "$MASTER_LOG" 2>/dev/null || true
  exit 1
}

# ═══════════════════════════════════════════════════════════════════════════════
# Phase 4 — Submit job (REST API, plain HTTP — same as PR 5)
# ═══════════════════════════════════════════════════════════════════════════════
phase_submit() {
  info "Phase 4: submitting job (REST over plain HTTP)"

  local scene_path="$STAGING_DIR/scene.png"
  local audio_path="$STAGING_DIR/silent.aac"
  [[ -f "$audio_path" ]] || audio_path="$STAGING_DIR/silent.mp3"

  cat > "$WORKDIR/job.json" <<JSON
{
  "video_name": "VeloxE2EWorkloadMTLS",
  "script_text": "PR 7 mTLS E2E workload smoke test.",
  "scenes_json": "[{\"text\":\"E2E\",\"image\":\"file://${scene_path}\"}]",
  "voiceover_path": "${audio_path}",
  "render_video": true,
  "save_to_db": true,
  "channel_id": "e2e-workload-mtls",
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
  pass "job submitted (REST): job_id=$JOB_ID"
}

# ═══════════════════════════════════════════════════════════════════════════════
# Phase 5 — Start worker WITH mTLS, environment=staging (no insecure)
# ═══════════════════════════════════════════════════════════════════════════════
phase_worker_start() {
  info "Phase 5: starting worker (mTLS, environment=staging)"

  # Note: NO "allow_insecure_grpc_dev" field. Combined with
  # environment=staging + the TLS triple, this is the canonical
  # "production-grade staging" recipe (per PR 1 §Path A migration).
  cat > "$WORKER_CFG" <<JSON
{
  "master_url": "http://127.0.0.1:${MASTER_PORT}",
  "worker_id": "${WORKER_ID}",
  "work_dir": "${WORKDIR}",
  "control_grpc_url": "127.0.0.1:${GRPC_PORT}",
  "job_delivery": "push",
  "environment": "staging",
  "tls_cert_file": "${WORKER_CRT}",
  "tls_key_file":  "${WORKER_KEY}",
  "tls_ca_file":   "${CA_CRT}",
  "data_dir": "${WORKDIR}",
  "max_active_jobs": 1,
  "health_port": 0,
  "prometheus_port": 0,
  "protocol_version": "v3"
}
JSON

  rm -f "$WORKER_LOG"
  # Do NOT export VELOX_ALLOW_INSECURE_GRPC_DEV — preserves fail-closed.
  "$WORKER_BIN" --config "$WORKER_CFG" \
    >"$WORKER_LOG" 2>&1 &
  local pid=$!
  echo "$pid" > "$WORKER_PIDFILE"
  push_pid "$pid"
  info "worker PID=$pid"

  for i in $(seq 1 30); do
    if grep -qE "HelloAck|✓ HelloAck|accepted registration" "$MASTER_LOG" 2>/dev/null; then
      pass "worker registered after ${i}s via mTLS (handshake accepted by master)"
      sleep 2
      return 0
    fi
    if ! kill -0 "$pid" 2>/dev/null; then
      fail "worker crashed during registration"
      tail -40 "$WORKER_LOG" 2>/dev/null || true
      exit 1
    fi
    sleep 2
  done
  fail "worker did not register within 60s"
  tail -20 "$MASTER_LOG" 2>/dev/null || true
  tail -20 "$WORKER_LOG" 2>/dev/null || true
  exit 1
}

# ═══════════════════════════════════════════════════════════════════════════════
# Phase 6 — Poll + verify V1–V6 (same as PR 5)
# ═══════════════════════════════════════════════════════════════════════════════
phase_poll_and_verify() {
  info "Phase 6: polling for SUCCEEDED (max 5 min)"
  local db="$DATA_DIR/velox.db"
  local status=""

  for i in $(seq 1 60); do
    status="$(sqlite3 "$db" "SELECT status FROM jobs WHERE job_id='${JOB_ID}';" 2>/dev/null || true)"
    case "$status" in
      SUCCEEDED) pass "job SUCCEEDED after ~$(( i * 5 ))s"; break ;;
      FAILED|TIMEOUT|REJECTED|CANCELLED)
        fail "job reached terminal status=$status (expected SUCCEEDED)"; exit 1 ;;
    esac
    if (( i % 6 == 0 )); then info "  poll[$i/60] status=$status"; fi
    sleep 5
  done

  if [[ "$status" != "SUCCEEDED" ]]; then
    fail "job did not reach SUCCEEDED within 5 min"
    exit 1
  fi

  # ── V1 artifact exists ─────────────────────────────────────────────────
  info "Verification 1: artifact exists on disk"
  local artifact
  artifact="$(find "$STORAGE_DIR" -name '*.mp4' 2>/dev/null | head -1 || true)"
  if [[ -z "$artifact" ]]; then fail "no .mp4 artifact found in $STORAGE_DIR"; exit 1; fi
  local art_size
  art_size="$(stat -c%s "$artifact" 2>/dev/null || stat -f%z "$artifact" 2>/dev/null || echo 0)"
  if (( art_size < 1000 )); then fail "artifact too small: ${art_size} bytes"; exit 1; fi
  pass "artifact: $(basename "$artifact") (${art_size} bytes)"

  # ── V2 ffprobe (strict) ────────────────────────────────────────────────
  info "Verification 2: ffprobe (strict: h264 ONLY, 320x180, 1.8..2.2s)"
  if ! command -v ffprobe >/dev/null 2>&1; then fail "ffprobe missing"; exit 1; fi
  local probe_json
  probe_json="$(ffprobe -v quiet -print_format json -show_format -show_streams "$artifact" 2>/dev/null || true)"
  if [[ -z "$probe_json" ]]; then fail "ffprobe empty output"; exit 1; fi

  local codec
  codec="$(echo "$probe_json" | python3 -c "
import sys, json
d = json.load(sys.stdin)
vid = [s for s in d.get('streams', []) if s.get('codec_type') == 'video']
if not vid: sys.exit(2)
print(vid[0].get('codec_name',''))
")" || { fail "ffprobe: no video stream"; exit 1; }
  if [[ "$codec" != "h264" ]]; then fail "codec=$codec (only h264 accepted)"; exit 1; fi
  pass "ffprobe: codec=h264"

  local width height
  width="$(echo "$probe_json" | python3 -c "import sys,json;d=json.load(sys.stdin);s=[x for x in d['streams'] if x.get('codec_type')=='video'];print(s[0].get('width','0'))" 2>/dev/null || echo 0)"
  height="$(echo "$probe_json" | python3 -c "import sys,json;d=json.load(sys.stdin);s=[x for x in d['streams'] if x.get('codec_type')=='video'];print(s[0].get('height','0'))" 2>/dev/null || echo 0)"
  if (( width != 320 || height != 180 )); then fail "resolution=${width}x${height} (must be 320x180)"; exit 1; fi
  pass "ffprobe: resolution=320x180"

  local dur
  dur="$(echo "$probe_json" | python3 -c "import sys,json;d=json.load(sys.stdin);f=d.get('format',{});print(f.get('duration','0'))" 2>/dev/null || echo 0)"
  if ! awk -v d="$dur" 'BEGIN{ exit !(d+0 >= 1.8 && d+0 <= 2.2) }'; then
    fail "duration=${dur}s (must be in 1.8..2.2s)"; exit 1
  fi
  pass "ffprobe: duration=${dur}s (within 1.8..2.2s)"

  # ── V3 SHA-256 (mandatory) ─────────────────────────────────────────────
  if [[ -z "${E2E_EXPECTED_SHA256:-}" ]]; then
    fail "E2E_EXPECTED_SHA256 must be set for deterministic SHA-256 enforcement"
    exit 1
  fi
  info "Verification 3: SHA-256 checksum (expected=${E2E_EXPECTED_SHA256:0:16}...)"
  local sha
  sha="$(sha256sum "$artifact" 2>/dev/null | awk '{print $1}' || shasum -a 256 "$artifact" 2>/dev/null | awk '{print $1}' || true)"
  if [[ -z "$sha" ]]; then fail "SHA-256 could not be computed"; exit 1; fi
  echo "$sha  $(basename "$artifact")" > "$STORAGE_DIR/artifact.sha256"
  if [[ "$sha" != "$E2E_EXPECTED_SHA256" ]]; then
    fail "SHA-256 mismatch: got ${sha:0:16}... want ${E2E_EXPECTED_SHA256:0:16}..."
    exit 1
  fi
  pass "SHA-256 matches: ${sha:0:16}..."

  # ── V4 Worker visible in API ───────────────────────────────────────────
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

  # ── V5 Metrics strict ──────────────────────────────────────────────────
  info "Verification 5: Prometheus metrics (strict)"
  local metrics
  metrics="$(curl -sS -m 5 "http://127.0.0.1:${MASTER_PORT}/metrics" 2>/dev/null || true)"
  if [[ -z "$metrics" ]]; then fail "/metrics empty"; exit 1; fi

  local jobs_val jobs_nz=0 v
  jobs_val="$(echo "$metrics" | grep -E '^velox_job_succeeded_total([ {]|$)' | awk '{print $NF}')"
  for v in $jobs_val; do
    if [[ "$v" =~ ^[1-9][0-9]*$ ]]; then jobs_nz=1; break; fi
  done
  if (( jobs_nz == 0 )); then fail "velox_job_succeeded_total zeroed across all series"; exit 1; fi
  pass "metrics: velox_job_succeeded_total >= 1"

  local compute_nz=0
  for v in $(echo "$metrics" | grep -E '^velox_compute_seconds_total([ {]|$)' | awk '{print $NF}'); do
    if [[ "$v" =~ ^([1-9][0-9]*(\.[0-9]+)?|0\.[0-9]*[1-9][0-9]*)$ ]]; then
      compute_nz=1; break
    fi
  done
  if (( compute_nz == 0 )); then fail "velox_compute_seconds_total zeroed across all series"; exit 1; fi
  pass "metrics: velox_compute_seconds_total > 0"

  # ── V6 DB state (4-part) ──────────────────────────────────────────────
  info "Verification 6: DB state assertions (4-part, blocking)"
  if ! command -v sqlite3 >/dev/null 2>&1; then fail "sqlite3 missing"; exit 1; fi
  if [[ ! -f "$db" ]]; then fail "DB file not found at $db"; exit 1; fi

  sql_query() {
    local q="$1"; shift
    sqlite3 -separator '|' "$db" "$q" "$@" 2>/dev/null
  }

  local attempts_succ
  attempts_succ="$(sql_query "SELECT COUNT(*) FROM task_attempts WHERE job_id = ? AND status='SUCCEEDED'" "$JOB_ID" || true)"
  if [[ "${attempts_succ:-0}" =~ ^[1-9][0-9]*$ ]]; then
    pass "DB (a): task_attempts SUCCEEDED count=$attempts_succ"
  else
    fail "DB (a): no SUCCEEDED row in task_attempts (got '$attempts_succ')"; exit 1
  fi

  local arts_ready
  arts_ready="$(sql_query "SELECT COUNT(*) FROM artifacts WHERE job_id = ? AND status='READY'" "$JOB_ID" || true)"
  if [[ "${arts_ready:-0}" =~ ^[1-9][0-9]*$ ]]; then
    pass "DB (b): artifacts READY count=$arts_ready"
  else
    fail "DB (b): no READY row in artifacts (got '$arts_ready')"; exit 1
  fi

  local db_sha
  db_sha="$(sql_query "SELECT sha256 FROM artifacts WHERE job_id = ? AND status='READY' ORDER BY verified_at DESC LIMIT 1" "$JOB_ID" || true)"
  if [[ -z "$db_sha" ]]; then fail "DB (c): artifacts.sha256 empty"; exit 1; fi
  if [[ "$db_sha" != "$sha" ]]; then
    fail "DB (c): sha mismatch (db=$db_sha, downloaded=$sha, expected_baseline=${E2E_EXPECTED_SHA256:-<unset>})"
    exit 1
  fi
  pass "DB (c): artifacts.sha256 matches downloaded file"

  local jobs_completed_at db_verified_at jobs_epoch art_epoch
  jobs_completed_at="$(sql_query "SELECT completed_at FROM jobs WHERE job_id = ? LIMIT 1" "$JOB_ID" || true)"
  db_verified_at="$(sql_query "SELECT verified_at FROM artifacts WHERE job_id = ? AND status='READY' ORDER BY verified_at DESC LIMIT 1" "$JOB_ID" || true)"
  if [[ -z "$jobs_completed_at" || -z "$db_verified_at" ]]; then
    fail "DB (d): missing timestamp"; exit 1
  fi
  jobs_epoch="$(date -d "$jobs_completed_at" +%s 2>/dev/null || true)"
  art_epoch="$(date -d "$db_verified_at"    +%s 2>/dev/null || true)"
  if [[ -z "$jobs_epoch" || ! "$jobs_epoch" =~ ^[0-9]+$ ]]; then
    fail "DB (d): jobs.completed_at not RFC3339"; exit 1
  fi
  if [[ -z "$art_epoch" || ! "$art_epoch" =~ ^[0-9]+$ ]]; then
    fail "DB (d): artifacts.verified_at not RFC3339"; exit 1
  fi
  if (( jobs_epoch >= art_epoch )); then
    pass "DB (d): jobs.completed_at >= artifacts.verified_at (ord holds)"
  else
    fail "DB (d): jobs BEFORE artifacts.verified_at — ordering bug"; exit 1
  fi
}

# ═══════════════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════════════
main() {
  echo ""
  echo "══════════════════════════════════════════════════════════════"
  echo "  PR 7 — Velox Real Workload E2E (mTLS, environment=staging)"
  echo "══════════════════════════════════════════════════════════════"
  echo ""
  info "workdir  = $WORKDIR"
  info "version  = $VERSION"
  info "certs    = $CERTS_DIR"
  info "gRPC     = 127.0.0.1:${GRPC_PORT} (mTLS, channel=staging)"
  echo ""

  # Pre-flight: dependencies — strict, every one of them is required.
  local missing=0
  for dep in go ffmpeg sqlite3 python3 openssl curl; do
    command -v "$dep" >/dev/null 2>&1 || {
      fail "$dep not found — install before running make e2e-workload-mtls"; missing=1; }
  done
  (( missing == 0 )) || exit 3

  phase_certs   # Phase 0 — certs first; everything else depends on them.
  phase_build
  phase_fixtures
  phase_master_start
  phase_submit
  phase_worker_start
  phase_poll_and_verify

  echo ""
  echo "══════════════════════════════════════════════════════════════"
  pass "ALL VERIFICATIONS PASSED — Velox E2E workload-mtls (mTLS) complete"
  echo "══════════════════════════════════════════════════════════════"
  echo ""
}

main "$@"
