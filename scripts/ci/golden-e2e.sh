#!/usr/bin/env bash
# scripts/ci/golden-e2e.sh
#
# Golden e2e test for the full Velox pipeline. Follows the 19-June-2026
# deploy recipe from docs/worker-reliability-fixes.md, using real mTLS
# test certs instead of the dev-bypass flag.
#
# Targets a self-hosted fedora-40 (or any Linux with:
#   bash >= 4, coreutils, openssl, Go >= 1.22, cmake, g++, ffmpeg-dev,
#   sqlite3, python3)
#
# Environment:
#   TMPDIR         Base working directory (default: /tmp/velox-golden-e2e)
#   VERSION        Forces a version string; otherwise reads VERSION.txt
#   SKIP_BUILD     If set, skip the Go/cmake build steps (reuse existing bins)
#   SKIP_CLEANUP   If set, do not rm -rf $TMPDIR on exit
#
# Exit codes:
#   0   Pipeline OK — assert SUCCEEDED + *.mp4 found in storage
#   1   Master / worker / submission failure
#   2   Assertion failure (SUCCEEDED not reached or MP4 missing)
#   3   Environment / dependencies missing
#   126 Timeout waiting for lifecycle transition
#   127 Internal script error

set -euo pipefail

# ─── Configuration ───────────────────────────────────────────────────────────
readonly REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
readonly SCRIPT_NAME="$(basename "$0")"
TMPDIR="${TMPDIR:-/tmp/velox-golden-e2e}"
if [[ -n "${CMAKE_BIN:-}" ]]; then
  readonly CMAKE="${CMAKE_BIN}"
elif [[ -x /usr/bin/cmake ]]; then
  readonly CMAKE="/usr/bin/cmake"
else
  readonly CMAKE="$(command -v cmake)"
fi
readonly LOGDIR="${TMPDIR}/logs"
readonly CERTS_DIR="${TMPDIR}/certs"
readonly DATA_DIR="${TMPDIR}/data"
readonly STAGING_DIR="${DATA_DIR}/staging"
readonly STORAGE_DIR="${DATA_DIR}/storage"
MASTER_PORT="${MASTER_PORT:-8180}"
GRPC_PORT="${GRPC_PORT:-51851}"
WORKER_HEALTH_PORT="${WORKER_HEALTH_PORT:-8181}"
readonly ADMIN_TOKEN="e2e-test-admin-token"
readonly WORKER_ID="e2e-worker-1"
readonly WORKER_NAME="e2e-worker"
readonly WORKER_SECRET="golden-e2e-worker-secret"
BUNDLE_HASH=""

# Binaries
readonly MASTER_BIN="${TMPDIR}/bin/velox-server"
readonly WORKER_BIN="${TMPDIR}/bin/velox-worker-agent"
readonly ENGINE_BIN="${TMPDIR}/bin/velox_video_engine"

# Paths
readonly MASTER_LOG="${LOGDIR}/master.log"
readonly WORKER_LOG="${LOGDIR}/worker.log"
readonly MASTER_PIDFILE="${TMPDIR}/master.pid"
readonly WORKER_PIDFILE="${TMPDIR}/worker.pid"
readonly WORKER_CONFIG="${TMPDIR}/worker.json"
readonly JOB_FILE="${TMPDIR}/job.json"
readonly MASTER_ENV="${TMPDIR}/master.env"
readonly WORKER_CACHE_DIR="${TMPDIR}/cache"
readonly WORKER_BLOB_DIR="${TMPDIR}/blobs"

# ─── Helper functions ────────────────────────────────────────────────────────
log()   { printf '\e[36m[%s]\e[0m %s\n' "${SCRIPT_NAME}" "$*"; }
warn()  { printf '\e[33m[%s][WARN]\e[0m %s\n' "${SCRIPT_NAME}" "$*" >&2; }
die()   { printf '\e[31m[%s][FAIL]\e[0m %s\n' "${SCRIPT_NAME}" "$*" >&2; exit "${2:-1}"; }
ok()    { printf '\e[32m[%s][OK]\e[0m %s\n' "${SCRIPT_NAME}" "$*"; }

cleanup() {
  if [[ "${SKIP_CLEANUP:-0}" != "1" ]]; then
    log "cleanup: stopping master + worker"
    [[ -f "$MASTER_PIDFILE" ]] && kill "$(cat "$MASTER_PIDFILE")" 2>/dev/null || true
    [[ -f "$WORKER_PIDFILE" ]] && kill "$(cat "$WORKER_PIDFILE")" 2>/dev/null || true
    wait 2>/dev/null || true
  else
    log "cleanup: SKIP_CLEANUP=1 — leaving processes and logs in $TMPDIR"
  fi
}
trap cleanup EXIT

# ─── Phase 0: Dependency check + prep ────────────────────────────────────────
phase0_deps() {
  log "[0/8] Dependencies"
  local missing=0
  for cmd in openssl go g++ sqlite3 python3 ffmpeg curl; do
    command -v "$cmd" >/dev/null 2>&1 || { warn "missing: $cmd"; missing=1; }
  done
  if ! "$CMAKE" --version >/dev/null 2>&1; then
    warn "cmake not runnable via CMAKE=${CMAKE}"
    missing=1
  fi
  if [[ "$missing" -eq 1 ]]; then
    die "Install missing dependencies and retry" 3
  fi
  ok "dependencies: all found"

  # Version sanity — VERSION.txt
  if [[ -z "${VERSION:-}" ]]; then
    VERSION="$(tr -d '[:space:]' < "${REPO_ROOT}/VERSION.txt")"
  fi
  log "version: ${VERSION}"
  BUNDLE_HASH="golden-e2e-bundle-${VERSION}"

  mkdir -p "$LOGDIR" "$CERTS_DIR" "$DATA_DIR" "$STAGING_DIR" "$STORAGE_DIR" "$(dirname "$MASTER_BIN")" "${TMPDIR}/tests/fixtures" "$WORKER_CACHE_DIR" "$WORKER_BLOB_DIR"
  : > "${DATA_DIR}/velox.db"
  printf '%s\n' "$BUNDLE_HASH" > "${TMPDIR}/BUNDLE_HASH.txt"
  cp -f "${REPO_ROOT}/RemoteCodex/native/worker-agent-go/tests/fixtures/engine_selftest_baseline.sha256" "${TMPDIR}/tests/fixtures/engine_selftest_baseline.sha256"
}

# ─── Phase 1: Build binaries ─────────────────────────────────────────────────
phase1_build() {
  if [[ "${SKIP_BUILD:-0}" == "1" ]]; then
    for bin in "$MASTER_BIN" "$WORKER_BIN" "$ENGINE_BIN"; do
      [[ -x "$bin" ]] || die "SKIP_BUILD=1 but $bin is missing" 3
    done
    log "[1/8] Build: skipped (SKIP_BUILD=1)"
    return
  fi

  log "[1/8] Building binaries"

  # DataServer master
  log "  → velox-server"
  cd "${REPO_ROOT}/DataServer"
  go build -o "$MASTER_BIN" -ldflags "-s -w -X main.Version=${VERSION}" ./cmd/server 2>&1 | while IFS= read -r line; do warn "  go build master: $line"; done
  ok "  velox-server built"

  # Worker agent
  # NOTE: the worker-agent-go Makefile reads VERSION verbatim from VERSION_FILE;
  # the VERSION env var is NOT honored by the Makefile (it uses $(shell cat ...)
  # instead of $(VERSION) for the version string). Set VERSION_FILE if you need
  # to pin a non-canonical version file. Default resolves to ../../../VERSION.txt
  # which is the project root VERSION.txt. This is intentional single-source-of-truth.
  log "  → velox-worker-agent"
  cd "${REPO_ROOT}/RemoteCodex/native/worker-agent-go"
  make VERSION_FILE="../../../VERSION.txt" agent 2>&1 | while IFS= read -r line; do warn "  make agent: $line"; done
  cp -v "${REPO_ROOT}/RemoteCodex/native/worker-agent-go/bin/velox-worker-agent" "$WORKER_BIN"

  # Video engine
  log "  → velox_video_engine (cmake)"
  mkdir -p /tmp/velox-engine-build
  cd "${REPO_ROOT}/RemoteCodex/native/video-engine-cpp"
  "$CMAKE" -B /tmp/velox-engine-build -DCMAKE_BUILD_TYPE=Release 2>&1 | while IFS= read -r line; do warn "  cmake: $line"; done
  "$CMAKE" --build /tmp/velox-engine-build --parallel 2>&1 | while IFS= read -r line; do warn "  cmake build: $line"; done
  # Find the engine binary via glob — CMakeLists target name may differ; don't hardcode.
  ENGINE_SRC="$(find /tmp/velox-engine-build -maxdepth 1 -type f -executable -name 'velox*' 2>/dev/null | head -1 || true)"
  if [[ -z "$ENGINE_SRC" ]]; then
    warn "no velox* engine binary found in /tmp/velox-engine-build — listing contents:"
    ls -la /tmp/velox-engine-build/ || true
    die "engine binary not found after cmake build" 3
  fi
  cp -v "$ENGINE_SRC" "$ENGINE_BIN"
  rm -rf /tmp/velox-engine-build

  ok "binaries built"
}

# ─── Phase 2: Generate mTLS certs ────────────────────────────────────────────
phase2_certs() {
  log "[2/8] Generating mTLS test certs"
  bash "${REPO_ROOT}/scripts/gen-worker-certs.sh" "$CERTS_DIR" "$WORKER_ID"
  ok "mTLS certs generated (CN=${WORKER_ID})"
}

# ─── Phase 3: Bootstrap master ───────────────────────────────────────────────
phase3_master() {
  log "[3/8] Bootstrapping master"

  cat > "$MASTER_ENV" <<ENV
VELOX_MASTER_PORT=${MASTER_PORT}
VELOX_GRPC_PORT=${GRPC_PORT}
VELOX_DB_PATH=${DATA_DIR}/velox.db
VELOX_DATA_DIR=${DATA_DIR}
VELOX_STAGING_DIR=${STAGING_DIR}
VELOX_STORAGE_DIR=${STORAGE_DIR}
VELOX_ADMIN_TOKEN=${ADMIN_TOKEN}
VELOX_GRPC_TLS_CERT_FILE=${CERTS_DIR}/server.crt
VELOX_GRPC_TLS_KEY_FILE=${CERTS_DIR}/server.key
VELOX_GRPC_TLS_CA_FILE=${CERTS_DIR}/ca.crt
GIN_MODE=release
VELOX_ALLOWED_WORKERS=${WORKER_ID}
VELOX_CODE_VERSION=${VERSION}
VELOX_DELIVERY_GLOBAL_FALLBACK=true
ENV

  # Boot with setsid+nohup so the process survives the script shell.
  cd "$TMPDIR"
  set -a; source "$MASTER_ENV"; set +a
  setsid nohup "$MASTER_BIN" serve </dev/null >"$MASTER_LOG" 2>&1 &
  MPID=$!
  echo "$MPID" > "$MASTER_PIDFILE"
  disown "$MPID" 2>/dev/null
  unset VELOX_MASTER_PORT VELOX_GRPC_PORT VELOX_DB_PATH VELOX_DATA_DIR VELOX_STAGING_DIR VELOX_STORAGE_DIR
  unset VELOX_ADMIN_TOKEN VELOX_GRPC_TLS_CERT_FILE VELOX_GRPC_TLS_KEY_FILE VELOX_GRPC_TLS_CA_FILE
  unset GIN_MODE VELOX_ALLOWED_WORKERS VELOX_CODE_VERSION VELOX_DELIVERY_GLOBAL_FALLBACK
  log "master PID=$MPID"

  # Wait for /health
  for i in $(seq 1 20); do
    if curl -fsS -o /dev/null "http://127.0.0.1:${MASTER_PORT}/health" 2>/dev/null; then
      ok "master healthy (${i}s)"
      return 0
    fi
    sleep 1
  done

  # Dump master log for diagnostics
  tail -40 "$MASTER_LOG" 2>/dev/null || true
  die "master did not become healthy within 20s" 1
}

# ─── Phase 4: Bootstrap worker ───────────────────────────────────────────────
phase4_worker() {
  log "[4/8] Bootstrapping worker"

  cat > "$WORKER_CONFIG" <<JSON
{
  "master_url": "http://127.0.0.1:${MASTER_PORT}",
  "control_grpc_url": "127.0.0.1:${GRPC_PORT}",
  "worker_id": "${WORKER_ID}",
  "worker_name": "${WORKER_NAME}",
  "work_dir": "${TMPDIR}",
  "log_level": "info",
  "job_delivery": "push",
  "tls_cert_file": "${CERTS_DIR}/worker.crt",
  "tls_key_file": "${CERTS_DIR}/worker.key",
  "tls_ca_file": "${CERTS_DIR}/ca.crt",
  "video_engine_cpp_bin": "${ENGINE_BIN}",
  "bundle_hash": "${BUNDLE_HASH}",
  "max_active_jobs": 1,
  "health_port": ${WORKER_HEALTH_PORT},
  "protocol_version": "v3"
}
JSON

  # Boot worker with setsid+nohup
  cd "$TMPDIR"
  setsid nohup env WORK_DIR="${TMPDIR}" \
    VELOX_VIDEO_ENGINE_CPP_BIN="${ENGINE_BIN}" \
    VELOX_BUNDLE_HASH="${BUNDLE_HASH}" \
    VELOX_WORKER_SECRET="${WORKER_SECRET}" \
    VELOX_WORKER_CACHE_DIR="${WORKER_CACHE_DIR}" \
    VELOX_WORKER_BLOB_DIR="${WORKER_BLOB_DIR}" \
    VELOX_GRPC_TLS_CERT_FILE="${CERTS_DIR}/worker.crt" \
    VELOX_GRPC_TLS_KEY_FILE="${CERTS_DIR}/worker.key" \
    VELOX_GRPC_TLS_CA_FILE="${CERTS_DIR}/ca.crt" \
    "$WORKER_BIN" -config "$WORKER_CONFIG" </dev/null >"$WORKER_LOG" 2>&1 &
  WPID=$!
  echo "$WPID" > "$WORKER_PIDFILE"
  disown "$WPID" 2>/dev/null
  log "worker PID=$WPID"

  # Wait for real registration, not just process startup.
  for i in $(seq 1 30); do
    if grep -q "Worker ${WORKER_ID} connected (session:" "$MASTER_LOG" 2>/dev/null || \
       grep -q "Registration successful" "$WORKER_LOG" 2>/dev/null; then
      ok "worker registered (${i}s)"
      return 0
    fi
    if grep -q "worker_id mismatch" "$WORKER_LOG" 2>/dev/null; then
      tail -40 "$WORKER_LOG" 2>/dev/null || true
      die "worker registration failed due to TLS identity mismatch" 1
    fi
    if grep -q "credential required" "$WORKER_LOG" 2>/dev/null; then
      tail -40 "$WORKER_LOG" 2>/dev/null || true
      die "worker registration failed due to missing credential hash" 1
    fi
    if grep -q "protocol_version .* is not supported" "$WORKER_LOG" 2>/dev/null; then
      tail -40 "$WORKER_LOG" 2>/dev/null || true
      die "worker registration failed due to unsupported protocol_version" 1
    fi
    # Check worker didn't crash
    if ! kill -0 "$WPID" 2>/dev/null; then
      warn "worker process died — dumping worker log"
      tail -60 "$WORKER_LOG"
      die "worker crashed during registration" 1
    fi
    sleep 2
  done

  tail -40 "$WORKER_LOG" || true
  tail -40 "$MASTER_LOG" || true
  die "worker did not register within 60s" 1
}

# ─── Phase 5: Generate test fixtures ─────────────────────────────────────────
phase5_fixtures() {
  log "[5/8] Generating test fixtures"

  # 2-second silent MP3 voiceover
  ffmpeg -hide_banner -loglevel error -y \
    -f lavfi -i anullsrc=r=44100:cl=stereo -t 2 \
    -c:a libmp3lame "${STAGING_DIR}/voiceover.mp3" 2>/dev/null || true

  # Small teal scene image (320x240)
  ffmpeg -hide_banner -loglevel error -y \
    -f lavfi -i color=c=teal:s=320x240:d=2 -frames:v 1 \
    "${STAGING_DIR}/scene1.png" 2>/dev/null || true

  ok "fixtures: voiceover.mp3 + scene1.png"
}

# ─── Phase 6: Submit job ─────────────────────────────────────────────────────
phase6_submit() {
  log "[6/8] Submitting images.v1 job"

  # The scenes_json references the staged files via velox-asset:// OR file://
  # The master's AssetService will rewrite file:// paths on submission.
  cat > "$JOB_FILE" <<JSON
{
  "video_name": "GoldenE2E",
  "script_text": "E2E test smoke.",
  "scenes_json": "[{\"text\":\"E2E\",\"image\":\"file://${STAGING_DIR}/scene1.png\"}]",
  "voiceover_path": "${STAGING_DIR}/voiceover.mp3",
  "render_video": true,
  "save_to_db": true,
  "channel_id": "golden-e2e",
  "audio_language_for_srt": "en"
}
JSON

  SUBMIT_OUT=$(curl -sS -m 15 -X POST \
    -H "Authorization: Bearer ${ADMIN_TOKEN}" \
    -H "Content-Type: application/json" \
    --data-binary @"$JOB_FILE" \
    "http://127.0.0.1:${MASTER_PORT}/api/v1/script/generate-with-images" 2>&1) || true

  echo "$SUBMIT_OUT" | head -5
  JOB_ID=$(echo "$SUBMIT_OUT" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('job_id',''))" 2>/dev/null || true)

  if [[ -z "$JOB_ID" ]]; then
    warn "submit response: $SUBMIT_OUT"
    die "job submission failed — could not extract job_id" 1
  fi

  local now_utc
  now_utc="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  sqlite3 "${DATA_DIR}/velox.db" <<SQL
INSERT INTO job_delivery_plans (
  job_id, destination_id, enabled, priority, metadata_json, created_at, updated_at
)
SELECT
  '${JOB_ID}',
  destination_id,
  1,
  0,
  '{}',
  '${now_utc}',
  '${now_utc}'
FROM delivery_destinations
WHERE enabled = 1
ON CONFLICT(job_id, destination_id) DO UPDATE SET
  enabled = excluded.enabled,
  updated_at = excluded.updated_at;
SQL

  log "job_id=${JOB_ID}"
  ok "job submitted (PENDING)"
}

# ─── Phase 7: Poll lifecycle → SUCCEEDED ─────────────────────────────────────
phase7_poll() {
  log "[7/8] Polling job lifecycle → SUCCEEDED"
  local db="${DATA_DIR}/velox.db"
  local job_id="${1}"
  local max_polls=40  # 40 × 10s = ~7 minutes timeout
  local poll_interval=10

  for i in $(seq 1 "$max_polls"); do
    local status
    status=$(sqlite3 "$db" "SELECT status FROM jobs WHERE job_id='${job_id}';" 2>/dev/null || true)

    case "$status" in
      SUCCEEDED)
        ok "job SUCCEEDED after ~$(( i * poll_interval ))s"
        return 0
        ;;
      FAILED|TIMEOUT|REJECTED|CANCELLED)
        warn "job terminal with status=${status}"
        sqlite3 "$db" "SELECT job_id, status, lease_id, updated_at FROM jobs WHERE job_id='${job_id}';" 2>/dev/null || true
        die "job reached terminal status ${status} (expected SUCCEEDED)" 2
        ;;
      ""|PENDING|RUNNING|LEASED|RENDER_FINISHED|FINALIZING)
        # In progress — keep polling
        if (( i % 5 == 0 )); then
          log "  poll[${i}/${max_polls}] status=${status}  (elapsed=$(( i * poll_interval ))s)"
        fi
        ;;
      *)
        warn "unknown status: ${status}"
        ;;
    esac
    sleep "$poll_interval"
  done

  die "job did not reach SUCCEEDED within $(( max_polls * poll_interval ))s" 126
}

# ─── Phase 8: Assert final video artifact in VELOX_STORAGE_DIR ───────────────
phase8_verify_storage() {
  log "[8/8] Verifying final video artifact in storage"

  local video_count
  video_count=$(find "${STORAGE_DIR}" \( -name '*.mp4' -o -name '*.f4v' \) 2>/dev/null | wc -l)

  if [[ "$video_count" -eq 0 ]]; then
    warn "no final video artifact (.mp4/.f4v) found in ${STORAGE_DIR}"
    find "${STORAGE_DIR}" -type f 2>/dev/null | head -20 || true
    die "final video artifact not produced (expected ≥1 .mp4 or .f4v in storage)" 2
  fi

  local video_size
  video_size=$(find "${STORAGE_DIR}" \( -name '*.mp4' -o -name '*.f4v' \) -exec stat -c%s {} + 2>/dev/null | paste -sd+ | bc || echo 0)

  ok "${video_count} final video artifact(s) found (total ${video_size} bytes)"

  # Final database sanity dump
  sqlite3 "${DATA_DIR}/velox.db" \
    "SELECT job_id, status, video_name, job_run_id, updated_at FROM jobs ORDER BY updated_at DESC LIMIT 5;" \
    2>/dev/null || true
}

# ─── Main ─────────────────────────────────────────────────────────────────────
main() {
  cat <<BANNER

╔══════════════════════════════════════════════════════════════╗
║   Velox Golden E2E Test                                     ║
║   Recipe:   docs/worker-reliability-fixes.md (19-Jun-2026)  ║
║   TMPDIR:   ${TMPDIR}          ║
║   Version:  ${VERSION:-$(tr -d '[:space:]' < "${REPO_ROOT}/VERSION.txt" 2>/dev/null || echo "unknown")}                ║
╚══════════════════════════════════════════════════════════════╝

BANNER

  phase0_deps
  phase1_build
  phase2_certs
  phase3_master
  phase4_worker
  phase5_fixtures
  phase6_submit
  phase7_poll "${JOB_ID}"
  phase8_verify_storage

  echo
  ok "╔══════════════════════════════════════════════════════════════╗"
  ok "║ GOLDEN E2E TEST PASSED                                      ║"
  ok "║  SUCCEEDED + final video artifact verified                  ║"
  ok "╚══════════════════════════════════════════════════════════════╝"
}

main "$@"
