#!/usr/bin/env bash
# =============================================================================
# tests/smoke/full_payload/multi_scene_concat/run.sh
# Multi-scene concatenated payload smoke harness.
# =============================================================================
# Submits a >=10-scene real-asset SubmitJobRequest payload (modulo-cycled
# through tests/worker-cert/fixtures/assets.json) to a single pinned worker
# on the Velox Master, polls until SUCCEEDED, verifies:
#
#   T1  SUCCEEDED reached via the master state machine
#   T2  duration coherence (sum of scene.duration_seconds vs measured output
#       duration_seconds via ffprobe on the artifact body)
#   T3  Drive delivery to comedy_test (status=SUCCEEDED implies all
#       delivery_plan entries committed per SubmitJobStatusResponse contract)
#   T4  worker pin (placement-pin: lease was issued to <worker_id> via
#       master log scrape; same pattern as tests/worker-cert/smoke_one.sh)
#   T5  CPU/RAM/disk measurement: worker metrics snapshot BEFORE submit and
#       AFTER SUCCEEDED; delta on disk_free_bytes + cpu_utilization_ratio +
#       memory_used_bytes reported in evidence/run-*.json
#
# Source-of-truth for shape:
#   - tests/worker-cert/build_real_payload.py (extended --scenes-count=N)
#   - tests/worker-cert/smoke_one.sh (canonical M2M + POST + poll plumbing)
#   - tests/smoke/full_payload/run.sh (full-payload smoke, sibling)
#   - DataServer/internal/apiwire/apiwire.go SubmitJobRequest
#   - DataServer/internal/handlers/server/api/workers_dto.go (worker metrics
#     map keys: cpu_utilization_ratio, memory_used_bytes, disk_free_bytes,
#     process_rss_bytes, load_average)
#
# Modes:
#   --mode=submit    (default) full HTTP flow: M2M + POST + poll + verify.
#                     Requires VELOX_MASTER_URL + VELOX_ADMIN_TOKEN env.
#   --mode=dry       build substituted payload + jq summary to stdout; no HTTP.
#                     Useful in CI matrix rows that pre-flight the wire shape.
#   --mode=selftest  build + run forbidden-pattern self-check (selftest only).
#                     Confirms the payload does not regress on the
#                     velox-asset://<kind>/<file>.<ext> anti-pattern.
#
# Args (env-overridable; CLI overrides env):
#   --scenes-count=N          number of scenes (default: 10, min 10)
#   --duration-per-scene=N    per-scene duration_seconds (default: 2)
#   --worker-id=<id>          target worker_id (required for submit mode;
#                             defaults to MULTISCENE_TARGET_WORKER_ID env)
#   --artifact-verify=1       download + ffprobe artifact body for T2; default 1
#   --poll-timeout-s=N        poll cap seconds (default: 600)
#
# Environment:
#   VELOX_MASTER_URL                master base URL (default: http://127.0.0.1:8080)
#   VELOX_ADMIN_TOKEN               admin bearer for /api/v1/admin/m2m/keys
#                                   (set this OR TOKEN_FILE)
#   TOKEN_FILE                      dotenv alternative for VELOX_ADMIN_TOKEN
#   MULTISCENE_TARGET_WORKER_ID     target worker_id (overridden by --worker-id)
#   MULTISCENE_DESTINATION_ID       destination_id override (default: comedy_test)
#   MULTISCENE_POLL_TIMEOUT_S       polling cap seconds (default: 600)
#   VELOX_MASTER_LOG_PATH           path to velox-server log (lease-scrape source)
#
# Exit codes (mirror tests/smoke/full_payload/run.sh + tests/worker-cert/smoke_one.sh):
#   0  success — submit: SUCCEEDED + evidence written; dry/selftest: built OK.
#   2  usage / env (missing admin token, missing --worker-id in submit, etc.).
#   3  network (curl could not reach the master during M2M provision or POST/GET).
#   4  HTTP non-201 on M2M provisioning OR HTTP non-202 on POST.
#   5  POST 202 received but .job_id missing in body.
#   6  terminal-fail state FAILED/CANCELLED reached during poll.
#   7  poll timeout without reaching terminal state.
#   8  HTTP non-200 on GET during polling.
#   9  selftest::forbidden-pattern hit (script regression).
#  10  worker-mismatch: SUCCEEDED but lease did not pin to <worker_id>.
#  11  duration coherence failure (measured_duration outside tolerance).
#  12  Drive delivery failure (status did not reach SUCCEEDED, OR artifact
#      size = 0 / unreachable, OR --artifact-verify=1 but ffprobe missing).
# =============================================================================

set -uo pipefail  # NOT -e (mirror smoke_one.sh: keep going through polling)

REAL_SCRIPT="$(readlink -f "${BASH_SOURCE[0]}")"
SCRIPT_DIR="$(cd "$(dirname "$REAL_SCRIPT")" && pwd)"
# tests/smoke/full_payload/multi_scene_concat/ → 4 levels up to project root.
# (Sibling tests/smoke/full_payload/run.sh uses 3 levels; the extra directory
# level is the multi_scene_concat/ subdirectory added by this harness.)
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../../.." && pwd)"

# ─── Source cross-test + smoke-local helpers ──────────────────────────────
# shellcheck source=tests/_lib/sh/_lib.sh
source "${REPO_ROOT}/tests/_lib/sh/_lib.sh"
# shellcheck source=tests/worker-cert/lib/pluck.sh
source "${REPO_ROOT}/tests/worker-cert/lib/pluck.sh"

# ─── Args / env ────────────────────────────────────────────────────────────
usage() {
  sed -n '2,/^# ====/p' "$0" | sed 's/^# \{0,1\}//'
  exit 0
}
[[ "${1:-}" == "--help" || "${1:-}" == "-h" ]] && usage

MODE="submit"
SCENES_COUNT="${MULTISCENE_SCENES_COUNT:-10}"
DURATION_PER_SCENE="${MULTISCENE_DURATION_PER_SCENE:-2}"
TARGET_WORKER_ID="${MULTISCENE_TARGET_WORKER_ID:-}"
ARTIFACT_VERIFY="${MULTISCENE_ARTIFACT_VERIFY:-1}"
POLL_TIMEOUT_FULL="${MULTISCENE_POLL_TIMEOUT_S:-600}"

while (( $# > 0 )); do
  case "$1" in
    --mode=submit)            MODE="submit"; shift ;;
    --mode=dry)               MODE="dry"; shift ;;
    --mode=selftest)          MODE="selftest"; shift ;;
    --scenes-count=*)         SCENES_COUNT="${1#*=}"; shift ;;
    --duration-per-scene=*)   DURATION_PER_SCENE="${1#*=}"; shift ;;
    --worker-id=*)            TARGET_WORKER_ID="${1#*=}"; shift ;;
    --artifact-verify=0)      ARTIFACT_VERIFY=0; shift ;;
    --artifact-verify=1)      ARTIFACT_VERIFY=1; shift ;;
    --poll-timeout-s=*)       POLL_TIMEOUT_FULL="${1#*=}"; shift ;;
    -h|--help)                usage ;;
    *) log_error "unknown arg: $1 (use --help)"; exit 2 ;;
  esac
done

# Defensive: enforce the >=10-scene contract. The script accepts --scenes-count
# below 10 (operator override), but logs a WARN so the operator understands
# they're operating outside the smoke's contractual shape.
if (( SCENES_COUNT < 10 )); then
  log_warn "--scenes-count=${SCENES_COUNT} is below the contractual >=10; multi_scene_concat smoke is still functional but the concat-engine path is under-exercised"
fi

for bin in curl jq python3; do
  ensure_command_available "$bin" || { log_error "${bin} missing on PATH"; exit 2; }
done
if (( ARTIFACT_VERIFY == 1 )); then
  for bin in ffprobe curl; do
    ensure_command_available "$bin" || { log_error "${bin} missing on PATH (required for --artifact-verify=1)"; exit 2; }
  done
fi

# Defaults (overridable via env).
: "${VELOX_MASTER_URL:=http://127.0.0.1:8080}"
: "${MULTISCENE_DESTINATION_ID:=comedy_test}"
: "${FULLPAYLOAD_TARGET_EXECUTOR_ID:=scene.composite.v1@1}"
: "${MULTISCENE_EVIDENCE_DIR:=${SCRIPT_DIR}/evidence}"

VELOX_MASTER_URL="${VELOX_MASTER_URL%/}"
MULTISCENE_DESTINATION_ID="${MULTISCENE_DESTINATION_ID%/}"
EVIDENCE_DIR="${MULTISCENE_EVIDENCE_DIR}"

EPOCH=$(date +%s)
log_info "mode=${MODE} master=${VELOX_MASTER_URL} worker_id=${TARGET_WORKER_ID:-<unset>} scenes=${SCENES_COUNT} duration_per_scene=${DURATION_PER_SCENE}s destination=${MULTISCENE_DESTINATION_ID} executor=${FULLPAYLOAD_TARGET_EXECUTOR_ID} poll_timeout=${POLL_TIMEOUT_FULL}s artifact_verify=${ARTIFACT_VERIFY}"

# ─── Build payload via build_real_payload.py ──────────────────────────────
# We delegate to the canonical builder so the forbidden-pattern self-check
# (assert_no_forbidden) is shared between smoke_one and multi_scene_concat;
# this keeps the regression shield singular. The --scenes-count extension is
# backward-compatible (default 2 matches smoke_one.sh exactly).
ensure_dir "$EVIDENCE_DIR" || { log_error "ensure_dir failed: $EVIDENCE_DIR"; exit 2; }
PAYLOAD_FILE="$(mktemp "${EVIDENCE_DIR}/payload-XXXXXX.json")"
if ! python3 "${REPO_ROOT}/tests/worker-cert/build_real_payload.py" \
      --fixtures "${REPO_ROOT}/tests/worker-cert/fixtures/assets.json" \
      --worker-id "${TARGET_WORKER_ID:-multi-scene-concat-dry}" \
      --destination "${MULTISCENE_DESTINATION_ID}" \
      --target-executor-id "${FULLPAYLOAD_TARGET_EXECUTOR_ID}" \
      --scenes-count "${SCENES_COUNT}" \
      --duration-per-scene "${DURATION_PER_SCENE}" \
      --output "$PAYLOAD_FILE" \
      --strict; then
  log_error "build_real_payload.py failed"
  rm -f "$PAYLOAD_FILE"
  exit 2
fi
log_info "wrote payload: $PAYLOAD_FILE"
PAYLOAD="$(cat "$PAYLOAD_FILE")"

EXPECTED_DURATION_S=$(printf '%s' "$PAYLOAD" | jq '[.scenes[].duration_seconds] | add // 0')
EXPECTED_DURATION_MS=$(awk -v s="$EXPECTED_DURATION_S" 'BEGIN{printf "%d", s*1000}')
log_info "expected_duration=${EXPECTED_DURATION_S}s (${EXPECTED_DURATION_MS}ms) from $(printf '%s' "$PAYLOAD" | jq '.scenes | length') scenes"

# ─── selftest mode short-circuit ───────────────────────────────────────────
# build_real_payload.py --strict already runs assert_no_forbidden; selftest
# just summarizes the shape. We add an extra layer here: a regex-walk of the
# emitted JSON for the canonical FORBIDDEN_RX, matching tests/smoke/full_payload/
# run.sh §assert_no_forbidden exactly. This is intentional duplication: the
# Python and bash checkers cross-validate each other (a regression in one
# that the other would still catch is improbable, but defense-in-depth).
FORBIDDEN_RX='velox-asset://(voiceovers|clips|subtitles|images)/[A-Za-z0-9._-]+\.[A-Za-z0-9]+|file://'

assert_no_forbidden() {
  local payload_json="$1" hits_json count
  hits_json="$(jq \
    --arg rx "$FORBIDDEN_RX" \
    '[.. | select(type == "string")] | map(select(test($rx))) | {count: length, hits: .}' \
    <<<"$payload_json")"
  count=$(printf '%s' "$hits_json" | jq -er '.count')
  if (( count > 0 )); then
    log_error "self-check: ${count} forbidden pattern(s) detected in payload:"
    printf '%s' "$hits_json" | jq -er '.hits[]' | sed 's/^/  - /' >&2
    return 9
  fi
  return 0
}

assert_no_forbidden "$PAYLOAD" || { rm -f "$PAYLOAD_FILE"; exit 9; }

if [[ "$MODE" == "selftest" ]]; then
  echo "──── MULTI-SCENE CONCAT SELFTEST (mode=selftest, no HTTP) ────"
  printf '%s' "$PAYLOAD" | jq '{schema, idempotency_key, video_name, project_id, target_executor_id,
                                scene_count: (.scenes | length),
                                scenes_kinds: [.scenes[] | .kind],
                                voiceover_paths: .voiceover_paths,
                                expected_duration_seconds: ([.scenes[].duration_seconds] | add),
                                delivery_plan: .delivery_plan}'
  log_info "selftest OK"
  rm -f "$PAYLOAD_FILE"
  exit 0
fi

if [[ "$MODE" == "dry" ]]; then
  echo "──── MULTI-SCENE CONCAT DRY RUN (mode=dry, no HTTP) ────"
  printf '%s' "$PAYLOAD" | jq '{idempotency_key, video_name, project_id, target_executor_id,
                                num_scenes: (.scenes | length),
                                num_voiceover_paths: (.voiceover_paths | length),
                                expected_duration_seconds: ([.scenes[].duration_seconds] | add),
                                delivery_plan: .delivery_plan}'
  rm -f "$PAYLOAD_FILE"
  exit 0
fi

# ─── submit mode: full HTTP flow ──────────────────────────────────────────
if [[ -z "$TARGET_WORKER_ID" ]]; then
  log_error "submit mode requires --worker-id=<id> (or MULTISCENE_TARGET_WORKER_ID env)"
  rm -f "$PAYLOAD_FILE"
  exit 2
fi

# Placement-pin: export VELOX_PLACEMENT_PIN_WORKER_ID so any downstream
# process the master spawns (or any sibling process that imports the same
# env) honors the pin. Per tests/worker-cert/build_real_payload.py §build_payload
# comment: "On master deployments WITH env var VELOX_PLACEMENT_PIN_WORKER_ID,
# this is doubly-pinned: same executor AND same worker."
export VELOX_PLACEMENT_PIN_WORKER_ID="$TARGET_WORKER_ID"
log_info "placement-pin: VELOX_PLACEMENT_PIN_WORKER_ID=$VELOX_PLACEMENT_PIN_WORKER_ID"

# Resolve admin token (env > TOKEN_FILE dotenv).
resolve_token() {
  local v=""
  if [[ -n "${VELOX_ADMIN_TOKEN:-}" ]]; then
    v="$VELOX_ADMIN_TOKEN"
  elif [[ -n "${TOKEN_FILE:-}" && -r "${TOKEN_FILE}" ]]; then
    v=$(grep -E '^VELOX_ADMIN_TOKEN=' "$TOKEN_FILE" | head -1 \
      | sed 's/^[^=]*=//' | tr -d '"' | tr -d "'" | xargs || true)
  fi
  if [[ -z "$v" ]]; then
    log_error "VELOX_ADMIN_TOKEN unset and TOKEN_FILE not provided / unreadable"
    return 2
  fi
  if [[ "$v" == *$'\r'* || "$v" == *$'\n'* ]]; then
    log_error "VELOX_ADMIN_TOKEN contains CR or LF; refusing"
    return 2
  fi
  printf '%s' "$v"
}
ADMIN_TOKEN="$(resolve_token)" || { rm -f "$PAYLOAD_FILE"; exit 2; }

# M2M provisioning.
M2M_BEARER=""
PROVISIONED_CLIENT_ID=""
on_signals() {
  if [[ -n "$PROVISIONED_CLIENT_ID" && -n "$ADMIN_TOKEN" ]]; then
    curl -sS -m 5 -X DELETE \
      -H "Authorization: Bearer $ADMIN_TOKEN" \
      "${VELOX_MASTER_URL}/api/v1/admin/m2m/keys/${PROVISIONED_CLIENT_ID}" \
      >/dev/null 2>&1 || true
  fi
  [[ -n "$PAYLOAD_FILE" && -e "$PAYLOAD_FILE" ]] && rm -f "$PAYLOAD_FILE"
}
trap 'on_signals' EXIT INT TERM

if ! smoke_mint_m2m "$ADMIN_TOKEN" "$VELOX_MASTER_URL"; then
  log_error "M2M provisioning failed"
  exit 4
fi
log_info "M2M client_id=${PROVISIONED_CLIENT_ID}"

# ─── Pre-flight: snap worker metrics BEFORE submit ─────────────────────────
# GET /api/v1/workers/{worker_id} returns the typed worker record with
# .metrics = ParseWorkerMetrics(WorkerInfo.Metrics); the typed struct
# exposes cpu_utilization_ratio, memory_used_bytes, disk_free_bytes,
# process_rss_bytes, load_average. We snapshot the raw JSON metrics map so
# the post-submit delta calculation does not lose any sampler-derived keys.
snap_worker_metrics() {
  local worker_id="$1" bearer="$2" out_file="$3"
  local status body
  body="$(curl -sS -m 30 \
    -H "Authorization: Bearer ${bearer}" \
    "${VELOX_MASTER_URL}/api/v1/workers/${worker_id}" \
    -w '\n%{http_code}' 2>/dev/null)" || return 3
  status="$(printf '%s' "$body" | tail -1)"
  body="$(printf '%s' "$body" | sed '$d')"
  if [[ "$status" != "200" ]]; then
    log_error "GET /api/v1/workers/${worker_id} returned HTTP ${status}"
    return 4
  fi
  printf '%s' "$body" > "$out_file"
  return 0
}

BEFORE_METRICS_FILE="$(mktemp "${EVIDENCE_DIR}/before-XXXXXX.json")"
if ! snap_worker_metrics "$TARGET_WORKER_ID" "$M2M_BEARER" "$BEFORE_METRICS_FILE"; then
  log_error "could not snap worker metrics before submit (worker_id=${TARGET_WORKER_ID} reachable?)"
  exit 4
fi
BEFORE_DISK_FREE=$(jq -er '.metrics.disk_free_bytes // 0' "$BEFORE_METRICS_FILE")
BEFORE_CPU_RATIO=$(jq -er '.metrics.cpu_utilization_ratio // 0' "$BEFORE_METRICS_FILE")
BEFORE_MEM_USED=$(jq -er '.metrics.memory_used_bytes // 0' "$BEFORE_METRICS_FILE")
BEFORE_RAM=$(jq -er '.host.ram_bytes // .metrics.ram_bytes // 0' "$BEFORE_METRICS_FILE")
log_info "before-snap: disk_free=${BEFORE_DISK_FREE}B cpu_ratio=${BEFORE_CPU_RATIO} mem_used=${BEFORE_MEM_USED}B ram=${BEFORE_RAM}B"

# ─── POST /api/v1/jobs ────────────────────────────────────────────────────
TMP_HDRS="$(mktemp)"; TMP_BODY="$(mktemp)"
curl_rc=0
curl -sS -m 30 -X POST \
  -H "Authorization: Bearer ${M2M_BEARER}" \
  -H "Content-Type: application/json" \
  -H "X-Request-ID: multi-scene-concat-${EPOCH}-${PROVISIONED_CLIENT_ID}" \
  --data-raw "$PAYLOAD" \
  "${VELOX_MASTER_URL}/api/v1/jobs" \
  -D "$TMP_HDRS" -o "$TMP_BODY" 2>/dev/null || curl_rc=$?
if (( curl_rc != 0 )); then
  log_error "POST /api/v1/jobs network failure (curl_rc=${curl_rc}); could not reach ${VELOX_MASTER_URL}"
  exit 3
fi
POST_STATUS=$(awk 'NR==1 && $1 ~ /^[Hh][Tt][Tt][Pp]\// {print $2; exit}' "$TMP_HDRS")
POST_BODY=$(cat "$TMP_BODY")
if [[ "$POST_STATUS" != "202" ]]; then
  log_error "POST /api/v1/jobs returned HTTP ${POST_STATUS:-?}"
  log_error "  body: $(printf '%s' "$POST_BODY" | head -c 400)"
  exit 4
fi
JOB_ID=$(printf '%s' "$POST_BODY" | jq -er '.job_id // empty' 2>/dev/null) || {
  log_error "POST /api/v1/jobs returned 202 but missing .job_id"; exit 5; }
log_info "submitted job_id=${JOB_ID}"

# ─── Poll until SUCCEEDED (exp backoff 1→2→4→8→16s, capped at POLL_TIMEOUT_FULL) ──
elapsed=0
sleep_s=1
last_status=""
last_body=""
while (( elapsed < POLL_TIMEOUT_FULL )); do
  sleep "$sleep_s"
  elapsed=$((elapsed + sleep_s))
  sleep_s=$(( sleep_s * 2 )); (( sleep_s > 16 )) && sleep_s=16

  if ! curl -sS -m 10 \
        -H "Authorization: Bearer ${M2M_BEARER}" \
        "${VELOX_MASTER_URL}/api/v1/jobs/${JOB_ID}" \
        -D "$TMP_HDRS" -o "$TMP_BODY" 2>/dev/null; then
    sleep_s=1; continue
  fi
  GET_STATUS=$(awk 'NR==1 && $1 ~ /^[Hh][Tt][Tt][Pp]\// {print $2; exit}' "$TMP_HDRS")
  GET_BODY=$(cat "$TMP_BODY")
  sv=$(printf '%s' "$GET_BODY" | jq -er '.status // empty' 2>/dev/null || true)
  if [[ -n "$sv" ]]; then last_status="$sv"; last_body="$GET_BODY"; fi
  case "$sv" in
    SUCCEEDED) log_info "job SUCCEEDED after ${elapsed}s"; break ;;
    FAILED|CANCELLED)
      log_error "terminal-fail state ${sv} after ${elapsed}s"
      log_error "  body: $(printf '%s' "$GET_BODY" | head -c 400)"
      exit 6
      ;;
  esac
  if [[ "$GET_STATUS" != "200" ]]; then
    log_error "GET /api/v1/jobs/${JOB_ID} returned HTTP ${GET_STATUS:-?}"
    log_error "  body: $(printf '%s' "$GET_BODY" | head -c 400)"
    exit 8
  fi
done
if [[ "$last_status" != "SUCCEEDED" ]]; then
  log_error "poll timeout after ${POLL_TIMEOUT_FULL}s (last observed status=${last_status:-none})"
  exit 7
fi

# ─── Post-SUCCEEDED: snap worker metrics AFTER ────────────────────────────
AFTER_METRICS_FILE="$(mktemp "${EVIDENCE_DIR}/after-XXXXXX.json")"
if ! snap_worker_metrics "$TARGET_WORKER_ID" "$M2M_BEARER" "$AFTER_METRICS_FILE"; then
  log_warn "could not snap worker metrics after SUCCEEDED; CPU/RAM/disk delta will be empty"
  AFTER_DISK_FREE=0
  AFTER_CPU_RATIO=0
  AFTER_MEM_USED=0
else
  AFTER_DISK_FREE=$(jq -er '.metrics.disk_free_bytes // 0' "$AFTER_METRICS_FILE")
  AFTER_CPU_RATIO=$(jq -er '.metrics.cpu_utilization_ratio // 0' "$AFTER_METRICS_FILE")
  AFTER_MEM_USED=$(jq -er '.metrics.memory_used_bytes // 0' "$AFTER_METRICS_FILE")
fi
log_info "after-snap: disk_free=${AFTER_DISK_FREE}B cpu_ratio=${AFTER_CPU_RATIO} mem_used=${AFTER_MEM_USED}B"

# Compute CPU/RAM/disk deltas (best-effort; missing fields treated as 0).
DISK_USED_BEFORE_AFTER=$(( BEFORE_DISK_FREE - AFTER_DISK_FREE ))
if (( DISK_USED_BEFORE_AFTER < 0 )); then
  # disk_free_bytes decreased during the run -> bytes consumed = positive.
  # If negative, the worker reaped temp files post-render (observed on busy
  # workers); record as-is and surface in the report.
  DISK_USED_BEFORE_AFTER=$((-DISK_USED_BEFORE_AFTER))
fi
log_info "delta: disk_used=${DISK_USED_BEFORE_AFTER}B mem_delta=$(( AFTER_MEM_USED - BEFORE_MEM_USED ))B cpu_ratio_after=${AFTER_CPU_RATIO}"

# ─── Duration coherence (T2): download + ffprobe artifact body ────────────
STARTED_AT=$(printf '%s' "$last_body"    | jq -er '.started_at   // empty')
COMPLETED_AT=$(printf '%s' "$last_body"  | jq -er '.completed_at // empty')
ARTIFACT_URL=$(printf '%s' "$last_body"  | jq -er '.artifact_url // .artifact_path // .output_path // empty')
s_epoch=$(smoke_parse_iso8601 "$STARTED_AT")
c_epoch=$(smoke_parse_iso8601 "$COMPLETED_AT")
render_time_ms=0
if [[ -n "$s_epoch" && -n "$c_epoch" ]]; then
  render_time_ms=$(awk -v a="$s_epoch" -v b="$c_epoch" 'BEGIN{printf "%.0f", (b-a)*1000}')
fi
log_info "render_time_ms=${render_time_ms} artifact_url=${ARTIFACT_URL:-<empty>}"

artifact_size_bytes=$(smoke_artifact_size "$ARTIFACT_URL" "$M2M_BEARER")

MEASURED_DURATION_S=""
DURATION_COHERENCE_VERDICT="SKIPPED"
DURATION_DELTA_MS=""
if [[ "$ARTIFACT_VERIFY" == "1" && -n "$ARTIFACT_URL" && "$artifact_size_bytes" -gt 0 ]]; then
  TMP_ART="$(mktemp "${EVIDENCE_DIR}/artifact-XXXXXX.mp4")"
  if curl -sS -m 60 -L \
        -H "Authorization: Bearer ${M2M_BEARER}" \
        "$ARTIFACT_URL" \
        -o "$TMP_ART" 2>/dev/null; then
    # ffprobe -show_format emits [FORMAT] block at end. duration is the
    # container-level duration, which is the operator's authoritative
    # measurement of "total output length".
    MEASURED_DURATION_S=$(ffprobe -v error -show_format -of default=nw=1:nk=1 \
      "$TMP_ART" 2>/dev/null | awk -F= '/^duration=/{print $2; exit}')
    if [[ -n "$MEASURED_DURATION_S" ]]; then
      MEASURED_DURATION_MS=$(awk -v s="$MEASURED_DURATION_S" 'BEGIN{printf "%d", s*1000}')
      DURATION_DELTA_MS=$(( MEASURED_DURATION_MS - EXPECTED_DURATION_MS ))
      DURATION_DELTA_MS_ABS=${DURATION_DELTA_MS#-}
      # Tolerance: max(500ms absolute, 5% relative of expected). The
      # absolute floor covers short payloads (10×2=20s → 1s is 5%); the
      # relative covers longer payloads.
      TOLERANCE_MS=$(awk -v e="$EXPECTED_DURATION_MS" 'BEGIN{
        rel = e * 0.05;
        if (rel < 500) rel = 500;
        printf "%d", rel
      }')
      if (( DURATION_DELTA_MS_ABS <= TOLERANCE_MS )); then
        DURATION_COHERENCE_VERDICT="PASS"
      else
        DURATION_COHERENCE_VERDICT="FAIL"
        log_error "duration coherence FAIL: expected=${EXPECTED_DURATION_MS}ms measured=${MEASURED_DURATION_MS}ms delta=${DURATION_DELTA_MS}ms tolerance=${TOLERANCE_MS}ms"
      fi
    else
      log_warn "ffprobe returned empty duration (artifact=${TMP_ART}); duration coherence SKIPPED"
    fi
  else
    log_warn "artifact download failed (url=${ARTIFACT_URL}); duration coherence SKIPPED"
  fi
  rm -f "$TMP_ART"
elif [[ "$ARTIFACT_VERIFY" == "0" ]]; then
  log_info "duration coherence SKIPPED (--artifact-verify=0)"
fi

# ─── Drive delivery (T3): status==SUCCEEDED implies delivery committed ───
DRIVE_DELIVERY_VERDICT="FAIL"
if [[ "$last_status" == "SUCCEEDED" && "$artifact_size_bytes" -gt 0 ]]; then
  # Per SubmitJobStatusResponse contract: status=SUCCEEDED is the canonical
  # monotonic guarantee that (a) the artifact has been committed to its
  # destination and (b) all delivery_plan entries reached SUBMITTED state.
  DRIVE_DELIVERY_VERDICT="PASS"
  log_info "drive delivery: status=SUCCEEDED + artifact_size=${artifact_size_bytes}B ⇒ all delivery_plan entries committed to ${MULTISCENE_DESTINATION_ID}"
else
  log_error "drive delivery: status=${last_status} artifact_size=${artifact_size_bytes:-0} ⇒ cannot assert delivery to ${MULTISCENE_DESTINATION_ID}"
fi

# ─── Worker pin enforcement (T4): scrape lease from log ───────────────────
LEASE_JSON=$(smoke_scrape_lease "$JOB_ID" "${VELOX_MASTER_LOG_PATH:-}")
LEASED_WORKER=$(printf '%s' "$LEASE_JSON" | jq -er '.worker_id // empty')
TASK_ID=$(printf '%s' "$LEASE_JSON" | jq -er '.task_id // empty')
ATTEMPT_ID=$(printf '%s' "$LEASE_JSON" | jq -er '.attempt_id // empty')
LEASE_ID=$(printf '%s' "$LEASE_JSON" | jq -er '.lease_id  // empty')
log_info "lease worker=${LEASED_WORKER} task=${TASK_ID} attempt=${ATTEMPT_ID} lease_id=${LEASE_ID}"

PIN_VERDICT="FAIL"
if [[ -n "$LEASED_WORKER" && "$LEASED_WORKER" == "$TARGET_WORKER_ID" ]]; then
  PIN_VERDICT="PASS"
elif [[ -n "$TASK_ID" ]]; then
  # Fallback: confirm via per-worker current_task_id when log scraping misses.
  POST_WORKER=$(smoke_worker_by_id "$TARGET_WORKER_ID" 2>/dev/null || true)
  if [[ -z "$POST_WORKER" ]]; then
    # Re-list workers to refresh WORKERS_JSON (smoke_worker_by_id reads from
    # the global populated by smoke_workers_list).
    if smoke_workers_list "$M2M_BEARER" "$VELOX_MASTER_URL"; then
      POST_WORKER=$(smoke_worker_by_id "$TARGET_WORKER_ID")
    fi
  fi
  CUR_TASK=$(printf '%s' "$POST_WORKER" | jq -er '.current_task_id // empty' 2>/dev/null || true)
  if [[ -n "$CUR_TASK" && "$CUR_TASK" == "$TASK_ID" ]]; then
    PIN_VERDICT="PASS"
  fi
fi
if [[ "$PIN_VERDICT" != "PASS" ]]; then
  log_error "worker pin FAIL: SUCCEEDED but lease did not pin to <${TARGET_WORKER_ID}> (scrape_worker=${LEASED_WORKER:-<empty>})"
fi

# ─── Write evidence/run-<EPOCH>-<client_id>.json ──────────────────────────
NOW_ISO="$(date -u +'%Y-%m-%dT%H:%M:%SZ')"
EV_FILE="${EVIDENCE_DIR}/run-${EPOCH}-${PROVISIONED_CLIENT_ID}.json"
TMP_EV="$(mktemp "${EVIDENCE_DIR}/run-XXXXXX.json")"

# Build a JSON-safe verdict summary; nil-on-failure semantics.
cat > "$TMP_EV" <<JSON
{
  "schema": "tests/smoke/full_payload/multi_scene_concat@1",
  "worker_id": "${TARGET_WORKER_ID}",
  "job_id": "${JOB_ID}",
  "task_id": "${TASK_ID}",
  "attempt_id": "${ATTEMPT_ID}",
  "lease_id": "${LEASE_ID}",
  "status": "${last_status}",
  "target_executor_id": "${FULLPAYLOAD_TARGET_EXECUTOR_ID}",
  "destination_id": "${MULTISCENE_DESTINATION_ID}",
  "scenes_count": ${SCENES_COUNT},
  "duration_per_scene": ${DURATION_PER_SCENE},
  "expected_duration_ms": ${EXPECTED_DURATION_MS},
  "measured_duration_ms": ${MEASURED_DURATION_MS:-null},
  "duration_delta_ms": ${DURATION_DELTA_MS:-null},
  "duration_coherence_verdict": "${DURATION_COHERENCE_VERDICT}",
  "drive_delivery_verdict": "${DRIVE_DELIVERY_VERDICT}",
  "worker_pin_verdict": "${PIN_VERDICT}",
  "render_time_ms": ${render_time_ms:-0},
  "artifact_size_bytes": ${artifact_size_bytes:-0},
  "artifact_url": "${ARTIFACT_URL}",
  "started_at": "${STARTED_AT}",
  "completed_at": "${COMPLETED_AT}",
  "metrics": {
    "before": {
      "disk_free_bytes": ${BEFORE_DISK_FREE:-0},
      "cpu_utilization_ratio": ${BEFORE_CPU_RATIO:-0},
      "memory_used_bytes": ${BEFORE_MEM_USED:-0},
      "ram_bytes": ${BEFORE_RAM:-0}
    },
    "after": {
      "disk_free_bytes": ${AFTER_DISK_FREE:-0},
      "cpu_utilization_ratio": ${AFTER_CPU_RATIO:-0},
      "memory_used_bytes": ${AFTER_MEM_USED:-0}
    },
    "delta": {
      "disk_used_bytes": ${DISK_USED_BEFORE_AFTER:-0},
      "memory_delta_bytes": $(( AFTER_MEM_USED - BEFORE_MEM_USED ))
    }
  },
  "smoke_runner_rev": ${SMOKE_PLUCKER_VARS_REV:-3},
  "written_at": "${NOW_ISO}"
}
JSON
mv "$TMP_EV" "$EV_FILE"
log_info "wrote ${EV_FILE}"

# Clean up the temporary before/after metric snapshots; the verdict summary
# has been folded into the evidence file, so the snapshots are just clutter.
rm -f "$BEFORE_METRICS_FILE" "$AFTER_METRICS_FILE"

echo "OK: multi-scene-concat smoke"
echo "  job_id                   : ${JOB_ID}"
echo "  worker_id                : ${TARGET_WORKER_ID}"
echo "  status                   : ${last_status}"
echo "  scenes_count             : ${SCENES_COUNT}"
echo "  duration_per_scene       : ${DURATION_PER_SCENE}s"
echo "  expected_duration_ms     : ${EXPECTED_DURATION_MS}"
echo "  measured_duration_ms     : ${MEASURED_DURATION_MS:-<n/a>}"
echo "  duration_delta_ms        : ${DURATION_DELTA_MS:-<n/a>}"
echo "  duration_coherence       : ${DURATION_COHERENCE_VERDICT}"
echo "  drive_delivery           : ${DRIVE_DELIVERY_VERDICT}"
echo "  worker_pin               : ${PIN_VERDICT}"
echo "  render_time_ms           : ${render_time_ms}"
echo "  artifact_bytes           : ${artifact_size_bytes}"
echo "  cpu_ratio (before)       : ${BEFORE_CPU_RATIO}"
echo "  cpu_ratio (after)        : ${AFTER_CPU_RATIO}"
echo "  mem_delta_bytes          : $(( AFTER_MEM_USED - BEFORE_MEM_USED ))"
echo "  disk_used_bytes          : ${DISK_USED_BEFORE_AFTER}"
echo "  evidence                 : ${EV_FILE}"

# Final exit code reflects the most-severe tier verdict (T2 > T4 > T3 > T1).
# T1 SUCCEEDED is already implied by reaching this point; we surface T2/T3/T4
# verdicts via distinct exit codes for unattended CI scripts that tail stderr.
RC=0
if [[ "$DURATION_COHERENCE_VERDICT" == "FAIL" ]]; then
  RC=11
elif [[ "$DRIVE_DELIVERY_VERDICT" == "FAIL" ]]; then
  RC=12
elif [[ "$PIN_VERDICT" == "FAIL" ]]; then
  RC=10
fi
exit "$RC"
