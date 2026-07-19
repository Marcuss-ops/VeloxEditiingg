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
#   0   Pipeline OK — lifecycle, artifact identity, media contract passed
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
SOCIAL_API_PORT="${SOCIAL_API_PORT:-$((MASTER_PORT + 2))}"
readonly ADMIN_TOKEN="e2e-test-admin-token"
readonly WORKER_ID="e2e-worker-1"
readonly WORKER_NAME="e2e-worker"
readonly WORKER_SECRET="golden-e2e-worker-secret"
BUNDLE_HASH=""
GOLDEN_PROFILE="${GOLDEN_PROFILE:-small}"
VIDEO_WIDTH=320
VIDEO_HEIGHT=240
VIDEO_FPS=30
VIDEO_DURATION=2
AUDIO_SAMPLE_RATE=48000
AUDIO_CHANNELS=2
SCENE_COUNT=1
declare -a SCENE_COLORS=(teal)

# Binaries
readonly MASTER_BIN="${TMPDIR}/bin/velox-server"
readonly WORKER_BIN="${TMPDIR}/bin/velox-worker-agent"
readonly ENGINE_BIN="${TMPDIR}/bin/velox_video_engine"

# Paths
readonly MASTER_LOG="${LOGDIR}/master.log"
readonly WORKER_LOG="${LOGDIR}/worker.log"
readonly MASTER_PIDFILE="${TMPDIR}/master.pid"
readonly WORKER_PIDFILE="${TMPDIR}/worker.pid"
readonly SOCIAL_PIDFILE="${TMPDIR}/social-stub.pid"
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
    [[ -f "$SOCIAL_PIDFILE" ]] && kill "$(cat "$SOCIAL_PIDFILE")" 2>/dev/null || true
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

  case "${GOLDEN_PROFILE}" in
    small)
      # The renderer's canonical canvas is 1920x1080 even for the short
      # smoke fixture; keep the small profile small in time and scene count.
      VIDEO_WIDTH=1920
      VIDEO_HEIGHT=1080
      VIDEO_DURATION=2
      SCENE_COUNT=1
      SCENE_COLORS=(teal)
      ;;
    production-shaped)
      VIDEO_WIDTH=1920
      VIDEO_HEIGHT=1080
      VIDEO_DURATION=30
      SCENE_COUNT=5
      SCENE_COLORS=(red green blue yellow magenta)
      ;;
    *)
      die "unsupported GOLDEN_PROFILE=${GOLDEN_PROFILE} (use small or production-shaped)" 3
      ;;
  esac
  log "profile: ${GOLDEN_PROFILE} ${VIDEO_WIDTH}x${VIDEO_HEIGHT}@${VIDEO_FPS} duration=${VIDEO_DURATION}s scenes=${SCENE_COUNT}"

  mkdir -p "$LOGDIR" "$CERTS_DIR" "$DATA_DIR" "$STAGING_DIR" "$STORAGE_DIR" "$(dirname "$MASTER_BIN")" "${TMPDIR}/tests/fixtures" "$WORKER_CACHE_DIR" "$WORKER_BLOB_DIR" "${TMPDIR}/state"
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

  setsid nohup python3 "${REPO_ROOT}/scripts/ci/golden-e2e-social-stub.py" "${SOCIAL_API_PORT}" \
    </dev/null >"${LOGDIR}/social-stub.log" 2>&1 &
  echo "$!" > "${SOCIAL_PIDFILE}"
  for i in $(seq 1 10); do
    if (echo >/dev/tcp/127.0.0.1/"${SOCIAL_API_PORT}") 2>/dev/null; then
      ok "local Social API stub ready (${i}s)"
      break
    fi
    sleep 1
  done

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
SOCIAL_API_URL=http://127.0.0.1:${SOCIAL_API_PORT}
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

  # The render worker downloads file-backed assets over the authenticated
  # worker-assets HTTP endpoint.  gRPC mTLS authenticates the stream, but the
  # HTTP asset bridge also requires a short-lived command token.  Obtain the
  # token through the real registration endpoint and inject it only into this
  # isolated test worker.
  local register_out worker_token
  register_out="$(curl -fsS -m 15 -X POST \
    -H "Content-Type: application/json" \
    --data "{\"worker_id\":\"${WORKER_ID}\",\"worker_name\":\"${WORKER_NAME}\",\"version\":\"${VERSION}\",\"bundle_hash\":\"${BUNDLE_HASH}\",\"protocol_version\":\"v3\"}" \
    "http://127.0.0.1:${MASTER_PORT}/api/v1/workers/register")" \
    || die "worker HTTP registration failed: ${register_out:-no response}" 1
  worker_token="$(printf '%s' "${register_out}" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("session_id", ""))')"
  [[ -n "${worker_token}" ]] || die "worker registration returned no session token: ${register_out}" 1

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
    VELOX_STATE_DIR="${TMPDIR}/state" \
    VELOX_WORKER_TOKEN="${worker_token}" \
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

  # The worker always muxes the input into AAC.  Use a non-silent stereo
  # source so the final media contract can assert a real audio stream.
  ffmpeg -hide_banner -loglevel error -y \
    -f lavfi -i "sine=frequency=440:sample_rate=${AUDIO_SAMPLE_RATE}" \
    -f lavfi -i "sine=frequency=660:sample_rate=${AUDIO_SAMPLE_RATE}" \
    -filter_complex "[0:a][1:a]amerge=inputs=2[a]" \
    -map "[a]" -ac "${AUDIO_CHANNELS}" -ar "${AUDIO_SAMPLE_RATE}" \
    -t "${VIDEO_DURATION}" -c:a pcm_s16le "${STAGING_DIR}/voiceover.wav"

  local i color
  for ((i = 0; i < SCENE_COUNT; i++)); do
    color="${SCENE_COLORS[$i]}"
    ffmpeg -hide_banner -loglevel error -y \
      -f lavfi -i "color=c=${color}:s=${VIDEO_WIDTH}x${VIDEO_HEIGHT}:d=1" \
      -frames:v 1 "${STAGING_DIR}/scene$((i + 1)).png"
  done

  ok "fixtures: voiceover.wav (${AUDIO_SAMPLE_RATE}Hz/${AUDIO_CHANNELS}ch) + ${SCENE_COUNT} scene images"
}

# ─── Phase 6: Submit job ─────────────────────────────────────────────────────
phase6_submit() {
  log "[6/8] Submitting images.v1 job"

  local scenes_json='['
  local i
  for ((i = 0; i < SCENE_COUNT; i++)); do
    (( i > 0 )) && scenes_json+=','
    scenes_json+="{\"text\":\"Scene $((i + 1))\",\"image\":\"file://${STAGING_DIR}/scene$((i + 1)).png\"}"
  done
  scenes_json+=']'
  local scenes_json_json
  scenes_json_json="$(printf '%s' "$scenes_json" | python3 -c 'import json,sys; print(json.dumps(sys.stdin.read()))')"
  local destination_id
  sqlite3 "${DATA_DIR}/velox.db" <<'SQL'
INSERT OR IGNORE INTO delivery_destinations (
  destination_id, provider, external_destination_id, name, enabled, configuration_json, created_at, updated_at
) VALUES (
  'golden-e2e-destination', 'social_gateway', 'golden-e2e-external-destination', 'Golden E2E destination', 1, '{}',
  STRFTIME('%Y-%m-%dT%H:%M:%fZ','now'), STRFTIME('%Y-%m-%dT%H:%M:%fZ','now')
);
SQL
  destination_id="$(sqlite3 -noheader -batch "${DATA_DIR}/velox.db" \
    "SELECT destination_id FROM delivery_destinations WHERE enabled=1 ORDER BY destination_id LIMIT 1;" | tr -d '\r')"
  [[ -n "${destination_id}" ]] || die "no enabled delivery destination available for Golden E2E" 1

  # The scenes_json references the staged files via velox-asset:// OR file://
  # The master's AssetService will rewrite file:// paths on submission.
  cat > "$JOB_FILE" <<JSON
{
  "video_name": "GoldenE2E",
  "script_text": "Golden E2E ${GOLDEN_PROFILE} scene contract.",
  "scenes_json": ${scenes_json_json},
  "voiceover_path": "${STAGING_DIR}/voiceover.wav",
  "render_video": true,
  "save_to_db": true,
  "channel_id": "golden-e2e",
  "audio_language_for_srt": "en",
  "delivery_plan": [{"destination_id":"${destination_id}","retry_budget":1,"priority":0}]
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

# ─── Phase 8: Assert lifecycle, artifact identity and media contract ─────────
sqlite_scalar() {
  sqlite3 -noheader -batch "${DATA_DIR}/velox.db" "$1" | tr -d '\r'
}

assert_count() {
  local label="$1" expected="$2" actual="$3"
  [[ "${actual}" == "${expected}" ]] || die "${label}: got ${actual}, want ${expected}" 2
  ok "${label}=${actual}"
}

resolve_artifact_path() {
  local local_path="$1" storage_key="$2"
  if [[ -n "${local_path}" && -f "${local_path}" ]]; then
    printf '%s\n' "${local_path}"
  elif [[ -n "${storage_key}" && "${storage_key}" = /* && -f "${storage_key}" ]]; then
    printf '%s\n' "${storage_key}"
  elif [[ -n "${storage_key}" && -f "${STORAGE_DIR}/${storage_key}" ]]; then
    printf '%s\n' "${STORAGE_DIR}/${storage_key}"
  else
    return 1
  fi
}

verify_scene_contract() {
  [[ "${GOLDEN_PROFILE}" == "production-shaped" ]] || return 0
  local i timestamp
  local -a rgb=("255 0 0" "0 128 0" "0 0 255" "255 255 0" "255 0 255")
  for ((i = 0; i < SCENE_COUNT; i++)); do
    timestamp=$((i * VIDEO_DURATION / SCENE_COUNT + 1))
    local frame_file="${TMPDIR}/scene-frame-${i}.raw"
    ffmpeg -hide_banner -loglevel error -ss "${timestamp}" -i "${VIDEO_PATH}" \
      -frames:v 1 -vf scale=1:1 -f rawvideo -pix_fmt rgb24 "${frame_file}"
    python3 - "${frame_file}" "${rgb[$i]}" "${timestamp}" <<'PY'
import sys
with open(sys.argv[1], "rb") as handle:
    pixel = handle.read(3)
want = tuple(map(int, sys.argv[2].split()))
if len(pixel) != 3:
    raise SystemExit("could not sample video frame")
got = tuple(pixel)
if sum(abs(a - b) for a, b in zip(got, want)) > 180:
    raise SystemExit(f"scene at t={sys.argv[3]}s: RGB={got}, want near {want}")
PY
    ok "scene $((i + 1)) at ${timestamp}s matches ${SCENE_COLORS[$i]}"
  done
}

phase8_verify_storage() {
  # Keep the canonical assertions in one verifier shared with the two-host
  # driver.  The legacy inline implementation below is retained temporarily
  # for reviewability while callers migrate; this return makes the shared
  # verifier authoritative.
  bash "${REPO_ROOT}/scripts/e2e/verify-golden-job.sh" \
    --db "${DATA_DIR}/velox.db" \
    --job-id "${JOB_ID}" \
    --storage-dir "${STORAGE_DIR}" \
    --tmpdir "${TMPDIR}/verify" \
    --profile "${GOLDEN_PROFILE}" \
    --width "${VIDEO_WIDTH}" --height "${VIDEO_HEIGHT}" --fps "${VIDEO_FPS}" \
    --duration "${VIDEO_DURATION}" --sample-rate "${AUDIO_SAMPLE_RATE}" \
    --channels "${AUDIO_CHANNELS}" --scenes "${SCENE_COUNT}"
  return 0

  log "[8/8] Verifying exact Job artifact, lifecycle invariants and media"
  local task_count task_id winner_id artifact_count upload_count
  local open_uploads expected_deliveries delivery_count bad_deliveries
  local artifact_id local_path storage_key artifact_sha artifact_attempt decl_attempt upload_id upload_attempt ta_id upload_worker upload_lease ta_worker ta_lease

  assert_count "Job SUCCEEDED" 1 "$(sqlite_scalar "SELECT COUNT(*) FROM jobs WHERE job_id='${JOB_ID}' AND status='SUCCEEDED';")"
  task_count="$(sqlite_scalar "SELECT COUNT(*) FROM tasks WHERE job_id='${JOB_ID}';")"
  assert_count "tasks for Job" 1 "${task_count}"
  task_id="$(sqlite_scalar "SELECT task_id FROM tasks WHERE job_id='${JOB_ID}';")"
  winner_id="$(sqlite_scalar "SELECT COALESCE(winning_attempt_id,'') FROM tasks WHERE task_id='${task_id}';")"
  [[ -n "${winner_id}" ]] || die "Task has no winning_attempt_id" 2
  assert_count "Task SUCCEEDED" 1 "$(sqlite_scalar "SELECT COUNT(*) FROM tasks WHERE task_id='${task_id}' AND status='SUCCEEDED';")"
  assert_count "winning TaskAttempt" 1 "$(sqlite_scalar "SELECT COUNT(*) FROM task_attempts WHERE task_id='${task_id}' AND id='${winner_id}' AND status='SUCCEEDED';")"
  assert_count "non-terminal Tasks" 0 "$(sqlite_scalar "SELECT COUNT(*) FROM tasks WHERE job_id='${JOB_ID}' AND status NOT IN ('SUCCEEDED','FAILED','CANCELLED','TIMED_OUT');")"

  artifact_count="$(sqlite_scalar "SELECT COUNT(*) FROM artifacts WHERE job_id='${JOB_ID}' AND status='READY';")"
  assert_count "Artifact READY" 1 "${artifact_count}"
  upload_count="$(sqlite_scalar "SELECT COUNT(*) FROM artifact_uploads WHERE job_id='${JOB_ID}' AND status='COMPLETED';")"
  assert_count "ArtifactUpload COMPLETED" 1 "${upload_count}"
  assert_count "Artifact STAGING/VERIFYING" 0 "$(sqlite_scalar "SELECT COUNT(*) FROM artifacts WHERE job_id='${JOB_ID}' AND status IN ('STAGING','VERIFYING');")"
  open_uploads="$(sqlite_scalar "SELECT COUNT(*) FROM artifact_uploads WHERE job_id='${JOB_ID}' AND status IN ('CREATED','UPLOADING','RECEIVED','FINALIZING');")"
  assert_count "open ArtifactUploads" 0 "${open_uploads}"

  IFS='|' read -r artifact_id local_path storage_key artifact_sha artifact_attempt decl_attempt upload_id upload_attempt ta_id upload_worker upload_lease ta_worker ta_lease < <(
    sqlite3 -noheader -separator '|' "${DATA_DIR}/velox.db" "
      SELECT a.id, COALESCE(a.local_path,''), COALESCE(a.storage_key,''),
             COALESCE(a.sha256,''), COALESCE(CAST(a.attempt_id AS TEXT),''),
             COALESCE(d.attempt_id,''), au.upload_id, CAST(au.attempt_number AS TEXT),
             COALESCE(ta.id,''), au.worker_id, au.lease_id,
             COALESCE(ta.worker_id,''), COALESCE(ta.lease_id,'')
        FROM artifacts a
        LEFT JOIN task_output_declarations d ON d.artifact_id=a.id
        JOIN artifact_uploads au ON au.artifact_id=a.id AND au.status='COMPLETED'
        LEFT JOIN task_attempts ta ON ta.task_id='${task_id}' AND ta.attempt_number=au.attempt_number
       WHERE a.job_id='${JOB_ID}' AND a.status='READY';"
  )
  [[ -n "${artifact_id}" && -n "${upload_id}" ]] || die "READY artifact identity row missing" 2
  if [[ -n "${decl_attempt}" ]]; then
    [[ "${decl_attempt}" == "${upload_attempt}" ]] || die "declaration attempt=${decl_attempt}, upload attempt=${upload_attempt}" 2
  fi
  [[ "${ta_id}" == "${winner_id}" ]] || die "upload attempt resolves to ${ta_id}, winner=${winner_id}" 2
  [[ "${artifact_attempt}" == "${upload_attempt}" ]] || die "artifact.attempt_id=${artifact_attempt}, upload.attempt_number=${upload_attempt}" 2
  [[ "${upload_worker}" == "${ta_worker}" && "${upload_lease}" == "${ta_lease}" ]] || die "upload fence ${upload_worker}/${upload_lease} differs from attempt ${ta_worker}/${ta_lease}" 2
  [[ "${artifact_sha}" =~ ^[0-9a-fA-F]{64}$ ]] || die "artifact ${artifact_id} has invalid sha256=${artifact_sha}" 2
  VIDEO_PATH="$(resolve_artifact_path "${local_path}" "${storage_key}")" || die "artifact ${artifact_id} path not found (local_path=${local_path} storage_key=${storage_key})" 2
  [[ -s "${VIDEO_PATH}" ]] || die "artifact ${artifact_id} is empty: ${VIDEO_PATH}" 2
  [[ "$(sha256sum "${VIDEO_PATH}" | awk '{print $1}')" == "${artifact_sha}" ]] || die "artifact SHA mismatch for ${artifact_id}" 2
  ok "artifact identity: ${artifact_id} upload=${upload_id} attempt=${winner_id} path=${VIDEO_PATH}"

  expected_deliveries="$(sqlite_scalar "SELECT COUNT(*) FROM job_delivery_plans WHERE job_id='${JOB_ID}' AND enabled=1;")"
  delivery_count="$(sqlite_scalar "SELECT COUNT(*) FROM job_deliveries WHERE artifact_id='${artifact_id}';")"
  assert_count "JobDelivery rows" "${expected_deliveries}" "${delivery_count}"
  bad_deliveries="$(sqlite_scalar "SELECT COUNT(*) FROM job_deliveries WHERE artifact_id='${artifact_id}' AND status NOT IN ('PENDING','SUCCEEDED');")"
  assert_count "invalid JobDelivery states" 0 "${bad_deliveries}"

  python3 "${REPO_ROOT}/scripts/ci/golden-e2e-verify-media.py" "${VIDEO_PATH}" \
    "${VIDEO_WIDTH}" "${VIDEO_HEIGHT}" "${VIDEO_FPS}" "${VIDEO_DURATION}" \
    "${AUDIO_SAMPLE_RATE}" "${AUDIO_CHANNELS}"
  verify_scene_contract
  sqlite3 "${DATA_DIR}/velox.db" "SELECT job_id, status, video_name, updated_at FROM jobs WHERE job_id='${JOB_ID}';"
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
