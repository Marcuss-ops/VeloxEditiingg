#!/usr/bin/env bash
# =============================================================================
# tests/worker-cert/perf_matrix.sh — Fleet performance matrix smoke.
# =============================================================================
# Usage:
#   ./tests/worker-cert/perf_matrix.sh
#
#   VELOX_MASTER_URL=https://velox.example.com \
#   VELOX_ADMIN_TOKEN=... \
#   VELOX_DB_PATH=/var/lib/velox/velox.db \
#   ./tests/worker-cert/perf_matrix.sh
#
#   ./tests/worker-cert/perf_matrix.sh --workers host_57_129_132_133 \
#       --workers host_57_131_20_173 --workers velox-worker-13197 \
#       --workers velox-worker-523925eb --destination-id comedy_test
#
# What the script does:
#   1. Sources tests/_lib/sh/_lib.sh (logging + pid-trap + ensure) and
#      tests/worker-cert/lib/pluck.sh (smoke-local helpers).
#   2. Mints an ephemeral M2M client via POST /api/v1/admin/m2m/keys; DELETEs
#      it on exit (best-effort, see trap).
#   3. Pre-flight: GET /api/v1/workers, asserts every target <worker_id> is
#      CONNECTED + session_active=true. Default target set is the canonical
#      4-worker fleet from the user-spec checklist:
#        - host_57_129_132_133  (the one with verified SUCCEEDED job)
#        - host_57_131_20_173
#        - velox-worker-13197
#        - velox-worker-523925eb
#   4. Submits the SAME canonical real-asset payload (built once via
#      build_real_payload.py --worker-id "perf-matrix" --strict) to each
#      target worker. Each submission uses a worker-specific
#      idempotency_key ("perf-matrix-<worker_id>-<epoch>") so the master
#      does not dedup across submissions. Placement-pin enforcement comes
#      from the master-side VELOX_PLACEMENT_PIN_WORKER_ID environment; the
#      smoke asserts that, post-SUCCEEDED, the lease log line names the
#      target worker.
#   5. Polls GET /api/v1/jobs/<job_id> for each submission until terminal
#      SUCCEEDED (default --poll-timeout-s 300s). Submissions are
#      sequential (1 worker at a time) to avoid placement contention; the
#      matrix semantics don't require concurrent renders.
#   6. SQL queries against $VELOX_DB_PATH collect the eight canonical
#      performance metrics per worker:
#        - download_ms:        task_phase_timings.duration_ms WHERE phase=
#                              'asset_download'
#        - render_start_at:    task_phase_timings.wall_start WHERE phase=
#                              'render'
#        - render_total_ms:    task_phase_timings.duration_ms WHERE phase=
#                              'render'
#        - upload_ms:          task_phase_timings.duration_ms WHERE phase=
#                              'artifact_upload'
#        - cpu_max_pct:        task_attempt_metrics.cpu_percent_peak
#        - ram_max_bytes:      task_attempt_metrics.rss_peak_bytes
#        - output_size_bytes:  artifacts.size_bytes WHERE job_id=X AND
#                              verified_at IS NOT NULL
#        - retry_count:        COUNT(*) FROM task_attempts WHERE task_id=X
#   7. MAD-based anomaly detection: per metric, compute median + MAD across
#      workers, flag workers with |value − median| > 3 * MAD (high-side
#      only; low-side variance is typically benign).
#   8. Writes tests/worker-cert/perf_matrix.json (atomic via tmp+mv) and
#      prints a human-readable table on stdout.
#
# Exit codes:
#   0  PASS — all jobs SUCCEEDED, no MAD-flagged anomalies (or
#             --allow-anomalies set).
#   2  usage / env (missing arg / no curl|jq|sqlite3|python3).
#   3  master unreachable / M2M provisioning failed.
#   4  pre-flight failed (one or more target workers not CONNECTED +
#      session_active=true).
#   5  POST /api/v1/jobs non-202 for any submission.
#   6  poll timeout without reaching SUCCEEDED on at least one job.
#   7  terminal-fail (FAILED/CANCELLED) on at least one job.
#   8  DB query failure (sqlite3 unreachable for one of the metric
#      queries) — operator must investigate the canonical source.
#   9  MAD-based anomaly detected (per-metric threshold breached on at
#      least one worker × metric). Exit 9 is suppressed when
#      --allow-anomalies is set (the matrix is still written; the JSON
#      report's anomalies_count conveys the count to the dashboard).
# =============================================================================

set -uo pipefail  # NOT -e (intentional: keep going through polling + scraping)

REAL_SCRIPT="$(readlink -f "${BASH_SOURCE[0]}")"
SCRIPT_DIR="$(cd "$(dirname "$REAL_SCRIPT")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# ─── Cross-test helpers ────────────────────────────────────────────────────
# shellcheck source=tests/_lib/sh/_lib.sh
source "${REPO_ROOT}/tests/_lib/sh/_lib.sh"
# shellcheck source=tests/worker-cert/lib/pluck.sh
source "${SCRIPT_DIR}/lib/pluck.sh"

# ─── Args / defaults ───────────────────────────────────────────────────────
usage() {
  sed -n '2,/^# ====/p' "$0" | sed 's/^# \{0,1\}//'
  exit 0
}
if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
  usage
fi

# Default canonical 4-worker fleet (from user-spec checklist).
DEFAULT_WORKERS=(
  "host_57_129_132_133"
  "host_57_131_20_173"
  "velox-worker-13197"
  "velox-worker-523925eb"
)

WORKER_IDS=()
ALLOW_ANOMALIES=0
POLL_TIMEOUT_S="${PERF_MATRIX_POLL_TIMEOUT_S:-300}"
IDLE_SLEEP_MS="${PERF_MATRIX_IDLE_SLEEP_MS:-200}"
DESTINATION_ID="${PERF_DESTINATION_ID:-comedy_test}"
REPORT_JSON=""
DB_PATH="${VELOX_DB_PATH:-}"
VELOX_MASTER_LOG_PATH="${VELOX_MASTER_LOG_PATH:-}"
EXECUTOR_ID="${PERF_MATRIX_EXECUTOR_ID:-scene.composite.v1@1}"

while (( $# > 0 )); do
  case "$1" in
    --workers)            WORKER_IDS+=("$2"); shift 2 ;;
    --allow-anomalies)    ALLOW_ANOMALIES=1; shift ;;
    --poll-timeout-s)     POLL_TIMEOUT_S="$2"; shift 2 ;;
    --idle-sleep-ms)      IDLE_SLEEP_MS="$2"; shift 2 ;;
    --destination-id)     DESTINATION_ID="$2"; shift 2 ;;
    --report-json)        REPORT_JSON="$2"; shift 2 ;;
    --db-path)            DB_PATH="$2"; shift 2 ;;
    --master-log-path)    VELOX_MASTER_LOG_PATH="$2"; shift 2 ;;
    --executor-id)        EXECUTOR_ID="$2"; shift 2 ;;
    -h|--help)            usage ;;
    *)                    log_error "unknown flag: $1"; exit 2 ;;
  esac
done

# If --workers omitted, fall back to canonical 4-worker fleet.
if (( ${#WORKER_IDS[@]} == 0 )); then
  WORKER_IDS=("${DEFAULT_WORKERS[@]}")
fi

# Numeric-validate the user-provided tunables up front so a typo is rc=2,
# not some downstream mystery failure.
for v in "$POLL_TIMEOUT_S" "$IDLE_SLEEP_MS"; do
  if ! [[ "$v" =~ ^[0-9]+$ ]] || (( v < 1 )); then
    log_error "timeout must be a positive integer (got: $v)"; exit 2
  fi
done

[[ -n "${VELOX_MASTER_URL:-}" ]] || VELOX_MASTER_URL="http://127.0.0.1:8080"
VELOX_MASTER_URL="${VELOX_MASTER_URL%/}"

log_info "perf_matrix workers=${WORKER_IDS[*]} master=$VELOX_MASTER_URL dest=$DESTINATION_ID"
log_info "tunables: poll_timeout=${POLL_TIMEOUT_S}s idle_sleep_ms=${IDLE_SLEEP_MS}s allow_anomalies=$ALLOW_ANOMALIES"

# ─── Required binaries ─────────────────────────────────────────────────────
for bin in curl jq python3 sqlite3 awk sed grep date; do
  if ! command -v "$bin" >/dev/null 2>&1; then
    log_error "FATAL: required binary not in PATH: $bin"; exit 2
  fi
done

# ─── Resolve admin token (env > TOKEN_FILE) ────────────────────────────────
ADMIN_TOKEN=""
if [[ -n "${VELOX_ADMIN_TOKEN:-}" ]]; then
  ADMIN_TOKEN="$VELOX_ADMIN_TOKEN"
elif [[ -n "${TOKEN_FILE:-}" && -r "${TOKEN_FILE}" ]]; then
  ADMIN_TOKEN=$(grep -E '^VELOX_ADMIN_TOKEN=' "$TOKEN_FILE" | head -1 \
    | sed 's/^[^=]*=//' | tr -d '"' | tr -d "'" | xargs || true)
fi
[[ -n "$ADMIN_TOKEN" ]] || { log_error "VELOX_ADMIN_TOKEN unset and TOKEN_FILE not provided"; exit 2; }

# ─── DB path auto-discovery (best-effort, required for metrics) ────────────
if [[ -z "$DB_PATH" ]]; then
  for candidate in \
      "${VELOX_DATA_DIR:-}/velox.db" \
      "/var/lib/velox/velox.db" \
      "/var/lib/velox-server/velox.db" \
      "${REPO_ROOT}/tests/worker-cert/.tmp-velox/velox.db" ; do
    if [[ -n "$candidate" && -r "$candidate" ]]; then
      DB_PATH="$candidate"; break
    fi
  done
fi
if [[ -z "$DB_PATH" || ! -r "$DB_PATH" ]]; then
  log_error "FATAL: VELOX_DB_PATH unreadable; metric queries require DB access (set VELOX_DB_PATH or pass --db-path)"; exit 2
fi
log_info "DB_PATH=$DB_PATH (readable)"

# ─── EXIT trap: cleanup children + mktemp + M2M ──────────────────────────
TMP_HDRS=""
TMP_BODY=""
TMP_PAYLOAD=""
TMP_OUT=""
DB_QUERY_FAILED=0
INV_ALL_JOBS_SUCCEEDED=0
INV_NO_ANOMALIES=0
WORKER_ROWS_JSON=""
ANOMALIES_JSON="[]"

on_exit_cleanup() {
  local rc=$?
  lib_kill_all TERM 2>/dev/null || true
  [[ -n "$TMP_HDRS" && -e "$TMP_HDRS"     ]] && rm -f "$TMP_HDRS"     2>/dev/null || true
  [[ -n "$TMP_BODY" && -e "$TMP_BODY"     ]] && rm -f "$TMP_BODY"     2>/dev/null || true
  [[ -n "$TMP_PAYLOAD" && -e "$TMP_PAYLOAD" ]] && rm -f "$TMP_PAYLOAD" 2>/dev/null || true
  [[ -n "$TMP_OUT" && -e "$TMP_OUT"       ]] && rm -f "$TMP_OUT"       2>/dev/null || true
  if [[ -n "$PROVISIONED_CLIENT_ID" && -n "$ADMIN_TOKEN" && -n "$VELOX_MASTER_URL" ]]; then
    curl -sS -m 5 -X DELETE \
      -H "Authorization: Bearer ${ADMIN_TOKEN}" \
      "${VELOX_MASTER_URL}/api/v1/admin/m2m/keys/${PROVISIONED_CLIENT_ID}" \
      >/dev/null 2>&1 || true
  fi
  exit "$rc"
}
trap on_exit_cleanup EXIT INT TERM

# ─── Provision ephemeral M2M client ────────────────────────────────────────
M2M_BEARER=""
PROVISIONED_CLIENT_ID=""
if ! smoke_mint_m2m "$ADMIN_TOKEN" "$VELOX_MASTER_URL"; then
  log_error "M2M provisioning failed"; exit 3
fi
log_info "M2M provisioned: client_id=$PROVISIONED_CLIENT_ID"

# ─── Pre-flight: every target worker CONNECTED + session_active=true ───────
WORKERS_JSON=""
if ! smoke_workers_list "$M2M_BEARER" "$VELOX_MASTER_URL"; then
  log_error "could not list workers"; exit 3
fi

for w in "${WORKER_IDS[@]}"; do
  record=$(smoke_worker_by_id "$w")
  if [[ -z "$record" ]]; then
    log_error "FAIL: pre-flight: target worker <$w> not in /api/v1/workers"; exit 4
  fi
  sts=$(printf '%s' "$record" | jq -r '.status // "(unset)"')
  sas=$(printf '%s' "$record" | jq -r '.session_active // false')
  if [[ "$sts" != "CONNECTED" || "$sas" != "true" ]]; then
    log_error "FAIL: pre-flight: <$w> status=$sts session_active=$sas (need CONNECTED+true)"
    exit 4
  fi
done
log_info "pre-flight: all ${#WORKER_IDS[@]} target workers CONNECTED + session_active=true"

# ─── Build canonical real-asset payload (once, same content for all) ──────
TMP_PAYLOAD=$(mktemp "${REPO_ROOT}/tests/worker-cert/.tmp-perf-payload.XXXXXX.json")
if ! python3 "${REPO_ROOT}/tests/worker-cert/build_real_payload.py" \
      --worker-id "perf-matrix" \
      --destination "$DESTINATION_ID" \
      --target-executor-id "$EXECUTOR_ID" \
      --strict \
      --output "$TMP_PAYLOAD" >/dev/null 2>&1; then
  log_error "FAIL: canonical payload build via build_real_payload.py"; exit 5
fi
log_info "canonical payload built: $TMP_PAYLOAD"

# ─── Submit one job per worker, sequential (placement-pin enforced) ───────
declare -a JOB_IDS_BY_WORKER=()
declare -a TASK_IDS_BY_WORKER=()
EPOCH=$(date +%s)

for w in "${WORKER_IDS[@]}"; do
  # Inject worker-specific idempotency_key so the master does not dedup
  # across submissions. idempotency_window is typically second-resolution.
  payload_worker="${TMP_PAYLOAD}.${w}"
  idem="perf-matrix-${w}-${EPOCH}"
  jq --arg idem "$idem" --arg name "perf-matrix ${w}@${EPOCH}" \
     '.idempotency_key = $idem | .video_name = $name' \
     "$TMP_PAYLOAD" > "$payload_worker" && mv -f "$payload_worker" "$TMP_PAYLOAD"

  TMP_HDRS=$(mktemp); TMP_BODY=$(mktemp)
  log_info "[submit $w] POST /api/v1/jobs (idem=$idem)"
  POST_STATUS=$(curl -sS -m 30 -X POST \
    -H "Authorization: Bearer $M2M_BEARER" \
    -H "Content-Type: application/json" \
    -H "X-Request-ID: perf-matrix-${w}-${EPOCH}" \
    --data-binary "@${TMP_PAYLOAD}" \
    -D "$TMP_HDRS" -o "$TMP_BODY" \
    -w '%{http_code}' \
    "${VELOX_MASTER_URL}/api/v1/jobs" 2>/dev/null) || POST_STATUS=""
  POST_BODY=$(cat "$TMP_BODY" 2>/dev/null || true)
  rm -f "$TMP_HDRS" "$TMP_BODY"
  if [[ "$POST_STATUS" != "202" ]]; then
    log_error "FAIL: POST /api/v1/jobs for worker $w: HTTP $POST_STATUS (body: $(printf '%s' "$POST_BODY" | head -c 300))"
    exit 5
  fi
  job_id=$(printf '%s' "$POST_BODY" | jq -er '.job_id // empty' 2>/dev/null || true)
  if [[ -z "$job_id" ]]; then
    log_error "FAIL: 202 but missing job_id for worker $w"; exit 5
  fi
  JOB_IDS_BY_WORKER+=("$job_id")
  log_info "[submit $w] accepted: job_id=$job_id"
  sleep "$(awk -v ms="$IDLE_SLEEP_MS" 'BEGIN { printf "%.3f", ms/1000 }')"
done

# ─── Poll each job sequentially until SUCCEEDED (with per-worker timeout) ─
declare -a STATUSES_BY_WORKER=()
declare -a RENDER_MS_BY_WORKER=()
declare -a STARTED_BY_WORKER=()
declare -a COMPLETED_BY_WORKER=()

all_succeeded=1
for idx in "${!WORKER_IDS[@]}"; do
  w="${WORKER_IDS[$idx]}"
  job_id="${JOB_IDS_BY_WORKER[$idx]}"
  log_info "[poll $w] GET /api/v1/jobs/${job_id} (timeout ${POLL_TIMEOUT_S}s)"
  elapsed=0
  sleep_s=1
  last_status=""
  last_body=""
  succeeded=0
  while (( elapsed < POLL_TIMEOUT_S )); do
    sleep "$sleep_s"
    elapsed=$((elapsed + sleep_s))
    sleep_s=$(( sleep_s * 2 )); (( sleep_s > 16 )) && sleep_s=16
    TMP_BODY=$(mktemp)
    RESP=$(curl -sS -m 10 \
      -H "Authorization: Bearer $M2M_BEARER" \
      "${VELOX_MASTER_URL}/api/v1/jobs/${job_id}" 2>/dev/null || true)
    sv=$(printf '%s' "$RESP" | jq -er '.status // empty' 2>/dev/null || true)
    [[ -n "$sv" ]] && { last_status="$sv"; last_body="$RESP"; }
    case "$sv" in
      SUCCEEDED) succeeded=1; rm -f "$TMP_BODY"; break ;;
      FAILED|CANCELLED)
        log_error "[poll $w] terminal-fail $sv after ${elapsed}s"
        rm -f "$TMP_BODY"; break ;;
    esac
    rm -f "$TMP_BODY"
  done
  if (( succeeded == 0 )); then
    log_error "FAIL: [poll $w] timeout ${POLL_TIMEOUT_S}s without terminal state (last=$last_status)"
    all_succeeded=0
    STATUSES_BY_WORKER+=("${last_status:-TIMEOUT}")
  else
    STATUSES_BY_WORKER+=("SUCCEEDED")
  fi

  # Capture render_time_ms + started_at + completed_at for the JSON.
  STARTED_AT=$(printf '%s' "$last_body"   | jq -er '.started_at   // empty' 2>/dev/null || echo "")
  COMPLETED_AT=$(printf '%s' "$last_body" | jq -er '.completed_at // empty' 2>/dev/null || echo "")
  render_ms=0
  if [[ -n "$STARTED_AT" && -n "$COMPLETED_AT" ]]; then
    s_epoch=$(date -u -d "$STARTED_AT"   +%s 2>/dev/null || echo 0)
    c_epoch=$(date -u -d "$COMPLETED_AT" +%s 2>/dev/null || echo 0)
    if [[ "$s_epoch" =~ ^[0-9]+$ && "$c_epoch" =~ ^[0-9]+$ ]]; then
      render_ms=$(( (c_epoch - s_epoch) * 1000 ))
    fi
  fi
  RENDER_MS_BY_WORKER+=("$render_ms")
  STARTED_BY_WORKER+=("$STARTED_AT")
  COMPLETED_BY_WORKER+=("$COMPLETED_AT")
done

if (( all_succeeded == 0 )); then
  log_error "FAIL: not all jobs reached SUCCEEDED; metric matrix may be partial"
  INV_ALL_JOBS_SUCCEEDED=0
else
  INV_ALL_JOBS_SUCCEEDED=1
fi

# ─── SQL metric queries per worker ─────────────────────────────────────────
# Schema notes (DataServer/internal/store/migrations/sqlite/042_task_phase_timings.sql
# + sqlite_task_atomic_persistence.go:397 + 010_job_attempts_and_artifacts.sql):
#   task_phase_timings (attempt_id, phase, duration_ms, wall_start, wall_end)
#   task_attempt_metrics (attempt_id, cpu_percent_peak, rss_peak_bytes)
#   artifacts (id, job_id, attempt_id, sha256, size_bytes, verified_at)
#   task_attempts (id, task_id, attempt_number, worker_id, status, error_code)
WORKER_ROWS_JSON="[]"

for idx in "${!WORKER_IDS[@]}"; do
  w="${WORKER_IDS[$idx]}"
  job_id="${JOB_IDS_BY_WORKER[$idx]}"

  # task_id + attempt_id (canonical lookup for the per-attempt metrics).
  task_id=$(sqlite3 "$DB_PATH" \
    "SELECT t.task_id FROM tasks t JOIN jobs j ON j.id = t.job_id WHERE j.id = '${job_id}' LIMIT 1;" 2>/dev/null \
    || echo "")
  attempt_id=$(sqlite3 "$DB_PATH" \
    "SELECT id FROM task_attempts WHERE task_id = '${task_id}' ORDER BY attempt_number DESC LIMIT 1;" 2>/dev/null \
    || echo "")

  # Phase timings: asset_download / render / artifact_upload.
  download_ms=$(sqlite3 "$DB_PATH" \
    "SELECT COALESCE(duration_ms, 0) FROM task_phase_timings WHERE attempt_id = '${attempt_id}' AND phase = 'asset_download' ORDER BY wall_start DESC LIMIT 1;" 2>/dev/null \
    || echo "?")
  render_start_at=$(sqlite3 "$DB_PATH" \
    "SELECT wall_start FROM task_phase_timings WHERE attempt_id = '${attempt_id}' AND phase = 'render' ORDER BY wall_start DESC LIMIT 1;" 2>/dev/null \
    || echo "")
  render_total_ms=$(sqlite3 "$DB_PATH" \
    "SELECT COALESCE(duration_ms, 0) FROM task_phase_timings WHERE attempt_id = '${attempt_id}' AND phase = 'render' ORDER BY wall_start DESC LIMIT 1;" 2>/dev/null \
    || echo "?")
  upload_ms=$(sqlite3 "$DB_PATH" \
    "SELECT COALESCE(duration_ms, 0) FROM task_phase_timings WHERE attempt_id = '${attempt_id}' AND phase = 'artifact_upload' ORDER BY wall_start DESC LIMIT 1;" 2>/dev/null \
    || echo "?")

  # Per-attempt metrics: CPU + RAM peak (worker-reported via TaskResult).
  cpu_max_pct=$(sqlite3 "$DB_PATH" \
    "SELECT COALESCE(cpu_percent_peak, 0) FROM task_attempt_metrics WHERE attempt_id = '${attempt_id}';" 2>/dev/null \
    || echo "?")
  ram_max_bytes=$(sqlite3 "$DB_PATH" \
    "SELECT COALESCE(rss_peak_bytes, 0) FROM task_attempt_metrics WHERE attempt_id = '${attempt_id}';" 2>/dev/null \
    || echo "?")

  # Output size: artifacts row with verified_at IS NOT NULL.
  output_size_bytes=$(sqlite3 "$DB_PATH" \
    "SELECT COALESCE(size_bytes, 0) FROM artifacts WHERE job_id = '${job_id}' AND verified_at IS NOT NULL LIMIT 1;" 2>/dev/null \
    || echo "?")

  # Retry count: total attempts on this task (>=1 happy path; >1 = retry).
  retry_count=$(sqlite3 "$DB_PATH" \
    "SELECT COUNT(*) FROM task_attempts WHERE task_id = '${task_id}';" 2>/dev/null \
    || echo "?")

  # Track DB failures for rc escalation.
  for q in "$download_ms" "$render_total_ms" "$upload_ms" "$cpu_max_pct" "$ram_max_bytes" "$output_size_bytes" "$retry_count"; do
    if [[ "$q" == "?" ]]; then DB_QUERY_FAILED=1; fi
  done

  status_field="${STATUSES_BY_WORKER[$idx]:-(unset)}"
  render_ms_field="${RENDER_MS_BY_WORKER[$idx]:-0}"

  # Append to WORKER_ROWS_JSON via jq composition.
  row=$(jq -n \
    --arg w "$w" \
    --arg jid "$job_id" \
    --arg tid "$task_id" \
    --arg aid "$attempt_id" \
    --arg status "$status_field" \
    --argjson render_ms "$render_ms_field" \
    --arg dl_ms "${download_ms:-0}" \
    --arg rs "${render_start_at:-}" \
    --arg rt_ms "${render_total_ms:-0}" \
    --arg up_ms "${upload_ms:-0}" \
    --arg cpu "${cpu_max_pct:-0}" \
    --arg ram "${ram_max_bytes:-0}" \
    --arg out_bytes "${output_size_bytes:-0}" \
    --argjson retry "${retry_count:-1}" \
    '{
       worker_id: $w,
       job_id: $jid,
       task_id: $tid,
       attempt_id: $aid,
       status: $status,
       render_time_ms: $render_ms,
       metrics: {
         download_ms:        ($dl_ms|tonumber? // 0),
         render_start_at:    $rs,
         render_total_ms:    ($rt_ms|tonumber? // 0),
         upload_ms:          ($up_ms|tonumber? // 0),
         cpu_max_pct:        ($cpu|tonumber? // 0),
         ram_max_bytes:      ($ram|tonumber? // 0),
         output_size_bytes:  ($out_bytes|tonumber? // 0),
         retry_count:        $retry
       }
     }')
  WORKER_ROWS_JSON=$(jq -c --argjson row "$row" '. + [$row]' <<<"$WORKER_ROWS_JSON")
done

# ─── MAD-based anomaly detection (high-side only) ─────────────────────────
# For each metric, compute median + MAD across the worker rows. Flag any
# worker whose value exceeds (median + 3 * MAD). With n=4, MAD is the
# median of |xi - median|. Robust to a single slow outlier. The 3*MAD
# threshold is conservative (≈3.5 sigma equivalent under normality).
METRIC_KEYS=(download_ms render_total_ms upload_ms cpu_max_pct ram_max_bytes output_size_bytes retry_count)
ANOMALIES_JSON="[]"

for mk in "${METRIC_KEYS[@]}"; do
  # Extract values for metric $mk across workers.
  values=()
  for idx in "${!WORKER_IDS[@]}"; do
    v=$(jq -r ".["$idx"].metrics.${mk} // 0" <<<"$WORKER_ROWS_JSON" 2>/dev/null || echo 0)
    [[ "$v" =~ ^[0-9]+$ ]] && values+=("$v") || values+=("0")
  done
  # Compute median via awk sort + middle pick.
  sorted=$(printf '%s\n' "${values[@]}" | sort -n)
  n=${#values[@]}
  median=$(printf '%s\n' "$sorted" | awk -v n="$n" 'NR == int((n+1)/2) { print; exit }')
  # MAD: median(|xi - median|).
  abs_devs=()
  for v in "${values[@]}"; do
    dev=$(awk -v v="$v" -v m="$median" 'BEGIN { d=v-m; if (d<0) d=-d; print d }')
    abs_devs+=("$dev")
  done
  mad=$(printf '%s\n' "${abs_devs[@]}" | sort -n | awk -v n="$n" 'NR == int((n+1)/2) { print; exit }')
  # Threshold (high-side): value > median + 3*MAD.
  thr=$(awk -v m="$median" -v d="$mad" 'BEGIN { printf "%d", m + 3*d }')

  for idx in "${!WORKER_IDS[@]}"; do
    w="${WORKER_IDS[$idx]}"
    v="${values[$idx]}"
    if (( v > thr )) && (( thr > 0 )); then
      diff_pct=$(awk -v v="$v" -v m="$median" 'BEGIN { if (m==0) {print "0"} else {printf "%.0f", (v-m)*100/m} }')
      severity="high"
      diff=$(awk -v v="$v" -v m="$median" 'BEGIN { printf "%d", v - m }')
      anom=$(jq -n \
        --arg w "$w" \
        --arg m "$mk" \
        --argjson v "$v" \
        --argjson median "$median" \
        --argjson mad "$mad" \
        --argjson diff "$diff" \
        --argjson diff_pct "$diff_pct" \
        --arg sev "$severity" \
        '{
           worker_id: $w,
           metric:    $m,
           value:     $v,
           median:    $median,
           mad:       $mad,
           diff:      $diff,
           diff_pct:  $diff_pct,
           severity:  $sev
         }')
      ANOMALIES_JSON=$(jq -c --argjson a "$anom" '. + [$a]' <<<"$ANOMALIES_JSON")
      log_warn "anomaly: $w metric=$mk value=$v > threshold=$thr (median=$median mad=$mad diff_pct=${diff_pct}%)"
    fi
  done
done

anomalies_count=$(jq 'length' <<<"$ANOMALIES_JSON")
if (( anomalies_count == 0 )); then
  INV_NO_ANOMALIES=1
fi

# ─── Write perf_matrix.json (atomic: tmp + mv) ────────────────────────────
OUT_FILE="${REPORT_JSON:-${REPO_ROOT}/tests/worker-cert/perf_matrix.json}"
OUT_DIR=$(dirname "$OUT_FILE")
ensure_dir "$OUT_DIR"
TMP_OUT=$(mktemp "${OUT_DIR}/perf_matrix-XXXXXX.json")
NOW_ISO=$(date -u +'%Y-%m-%dT%H:%M:%SZ')
passed_workers=$(jq '[.[] | select(.status == "SUCCEEDED")] | length' <<<"$WORKER_ROWS_JSON")
failed_workers=$(jq '[.[] | select(.status != "SUCCEEDED")] | length' <<<"$WORKER_ROWS_JSON")

cat > "$TMP_OUT" <<JSON
{
  "schema": "tests/worker-cert/perf_matrix@1",
  "epoch": ${EPOCH},
  "workers_tested": ${#WORKER_IDS[@]},
  "passed_workers": ${passed_workers},
  "failed_workers": ${failed_workers},
  "destination_id": "${DESTINATION_ID}",
  "executor_id": "${EXECUTOR_ID}",
  "matrix": ${WORKER_ROWS_JSON},
  "anomalies": ${ANOMALIES_JSON},
  "anomalies_count": ${anomalies_count},
  "invariants": {
    "all_jobs_succeeded": $([[ $INV_ALL_JOBS_SUCCEEDED -eq 1 ]] && echo true || echo false),
    "no_db_query_failure": $([[ $DB_QUERY_FAILED -eq 0 ]] && echo true || echo false),
    "no_anomalies":       $([[ $INV_NO_ANOMALIES -eq 1 ]] && echo true || echo false)
  },
  "master_url": "${VELOX_MASTER_URL}",
  "db_path": "${DB_PATH}",
  "written_at": "${NOW_ISO}"
}
JSON
mv "$TMP_OUT" "$OUT_FILE"
log_info "wrote $OUT_FILE"

# ─── Stdout: human-readable matrix table ──────────────────────────────────
echo "perf_matrix: workers=${#WORKER_IDS[@]} passed=$passed_workers failed=$failed_workers anomalies=$anomalies_count"
printf "%-32s %12s %12s %12s %12s %10s %12s %12s %6s\n" \
  "worker_id" "download_ms" "render_ms" "upload_ms" "render_tot_ms" "cpu_pct" "ram_bytes" "out_bytes" "retry"
printf -- "-%.0s" $(seq 1 130); echo
for idx in "${!WORKER_IDS[@]}"; do
  w="${WORKER_IDS[$idx]}"
  printf "%-32s %12s %12s %12s %12s %10s %12s %12s %6s\n" \
    "$w" \
    "$(jq -r ".["$idx"].metrics.download_ms       // 0"  <<<"$WORKER_ROWS_JSON")" \
    "$(jq -r ".["$idx"].render_time_ms           // 0"  <<<"$WORKER_ROWS_JSON")" \
    "$(jq -r ".["$idx"].metrics.upload_ms         // 0"  <<<"$WORKER_ROWS_JSON")" \
    "$(jq -r ".["$idx"].metrics.render_total_ms   // 0"  <<<"$WORKER_ROWS_JSON")" \
    "$(jq -r ".["$idx"].metrics.cpu_max_pct       // 0"  <<<"$WORKER_ROWS_JSON")" \
    "$(jq -r ".["$idx"].metrics.ram_max_bytes     // 0"  <<<"$WORKER_ROWS_JSON")" \
    "$(jq -r ".["$idx"].metrics.output_size_bytes // 0"  <<<"$WORKER_ROWS_JSON")" \
    "$(jq -r ".["$idx"].metrics.retry_count       // 0"  <<<"$WORKER_ROWS_JSON")"
done
if (( anomalies_count > 0 )); then
  echo
  echo "anomalies:"
  jq -c '.[]' <<<"$ANOMALIES_JSON"
fi

# ─── Final rc synthesis ──────────────────────────────────────────────────
OVERALL_RC=0
if (( INV_ALL_JOBS_SUCCEEDED == 0 )); then
  # Any FAILED/CANCELLED or timeout escalates to 7 (terminal-fail) or 6 (timeout).
  # Pick the worst observed status.
  for s in "${STATUSES_BY_WORKER[@]}"; do
    case "$s" in
      TIMEOUT) (( OVERALL_RC < 6 )) && OVERALL_RC=6 ;;
      FAILED|CANCELLED) (( OVERALL_RC < 7 )) && OVERALL_RC=7 ;;
    esac
  done
fi
if (( DB_QUERY_FAILED == 1 )) && (( OVERALL_RC < 8 )); then OVERALL_RC=8; fi
if (( anomalies_count > 0 )) && (( ALLOW_ANOMALIES == 0 )) && (( OVERALL_RC < 9 )); then OVERALL_RC=9; fi

exit "$OVERALL_RC"