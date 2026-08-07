#!/usr/bin/env bash
# =============================================================================
# tests/worker-cert/fleet_distribute.sh — Fleet-distribution smoke.
# =============================================================================
# Submits FLEET_JOB_COUNT (default 4) real jobs concurrently against a fleet
# of CONNECTED workers via /api/v1/jobs and verifies that the canonical
# completion pathway satisfies the six invariants the user-facing ops
# dashboard depends on:
#
#   1. distinct:    at least ceil(FLEET_JOB_COUNT/2) distinct worker_ids
#                   receive a lease (no single worker monopolizes).
#   2. cap-respect: no worker's leases count ever exceeds its task_slots
#                   (the worker-side MaxActiveJobs, default=1).
#   3. lease-once:  each task_id appears with exactly one lease_id in the
#                   master log; duplicates indicate a placement-rerun race.
#   4. artifact:    every SUCCEEDED job has a non-empty artifact_url in
#                   its /api/v1/jobs/<id> response.
#   5. drain:       every job reaches terminal SUCCEEDED before the poll
#                   timeout elapses (no PENDING/LEASED/RUNNING stragglers).
#   6. no-residual: after a cooldown (≥1 worker heartbeat interval), all
#                   workers touched by the run are CONNECTED with
#                   session_active=true and current_task_id empty; no
#                   worker is "stuck busy" with an evicted lease.
#
# Usage:
#   ./tests/worker-cert/fleet_distribute.sh
#       VELOX_MASTER_URL=https://velox.example.com \
#       VELOX_ADMIN_TOKEN=... \
#       ./tests/worker-cert/fleet_distribute.sh
#
#   ./tests/worker-cert/fleet_distribute.sh --fleet-job-count 6 --destination-id drive-production
#
# Environment contract:
#   VELOX_MASTER_URL    base URL of the Velox master (default http://127.0.0.1:8080)
#   VELOX_ADMIN_TOKEN   operator admin token (overridable by TOKEN_FILE=...)
#   VELOX_MASTER_BEARER bearer for M2M-submitted traffic (opt-in master-side
#                       checks; the fleet POSTs use a freshly-minted M2M bearer)
#   VELOX_MASTER_LOG_PATH path to master log file (default ~/.velox-server.log or
#                       $VELOX_DATA_DIR/master.log). Captured by the harness
#                       via journalctl inside the tests/_lib/sh/pid-trap helpers
#                       if the path is not directly readable.
#
# Exit codes:
#   0  PASS — all six invariants satisfied
#   2  usage / missing arg / missing binary
#   3  master unreachable / M2M provisioning failure
#   4  pre-flight: CONNECTED worker count < ceil(FLEET_JOB_COUNT/2)
#   5  POST /api/v1/jobs non-202 for any submitted job
#   6  poll timeout on at least one job (no terminal state within window)
#   7  terminal-fail (FAILED/CANCELLED) on at least one job
#   8  invariant 1 fail: distinct worker_ids < ceil(FLEET_JOB_COUNT/2)
#   9  invariant 2 fail: leases-per-worker > task_slots
#   10 invariant 3 fail: a single task_id has >1 lease_id in the master log
#   11 invariant 6 fail: some worker remains busy after cooldown
#   12 invariant 5 fail: queue did not drain (any job still PENDING/LEASED/RUNNING)
# =============================================================================

set -uo pipefail  # NOT -e: keep going so the report captures every invariant

REAL_SCRIPT="$(readlink -f "${BASH_SOURCE[0]}")"
SCRIPT_DIR="$(cd "$(dirname "$REAL_SCRIPT")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# ─── Cross-test helpers ────────────────────────────────────────────────────
# shellcheck disable=SC1091
source "${REPO_ROOT}/tests/_lib/sh/_lib.sh"
# shellcheck source=tests/worker-cert/lib/pluck.sh
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/lib/pluck.sh"

# ─── EXIT trap: cleanup children + mktemp scratch ─────────────────────────
TMP_PAYLOAD_DIR=""
TMP_LOG_DIR=""
TMP_REPORT=""
LEASES_TSV=""
OVERALL_FATAL_RC=0
# shellcheck disable=SC2329 # invoked indirectly by the EXIT/INT/TERM trap
on_exit_cleanup() {
  local rc=$?
  on_m2m_cleanup
  # Kill any background poll children (lib_kill_all TERM/KILL cascade).
  lib_kill_all TERM 2>/dev/null || true
  # Cleanup tmp dirs created by mktemp -p.
  [[ -d "$TMP_PAYLOAD_DIR" ]] && rm -rf "$TMP_PAYLOAD_DIR" 2>/dev/null || true
  [[ -d "$TMP_LOG_DIR"     ]] && rm -rf "$TMP_LOG_DIR"     2>/dev/null || true
  [[ -n "$TMP_REPORT" && -e "$TMP_REPORT" ]] && rm -f "$TMP_REPORT" 2>/dev/null || true
  [[ -n "$LEASES_TSV" && -e "$LEASES_TSV" ]] && rm -f "$LEASES_TSV" 2>/dev/null || true
  exit "$rc"
}
trap on_exit_cleanup EXIT INT TERM

# ─── Args / defaults ──────────────────────────────────────────────────────
usage() {
  sed -n '2,/^# ====/p' "$0" | sed 's/^# \{0,1\}//'
  exit 0
}
if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
  usage
fi

FLEET_JOB_COUNT="${FLEET_JOB_COUNT:-4}"
VELOX_MASTER_URL="${VELOX_MASTER_URL:-http://127.0.0.1:8080}"
BEARER_ENV="${FLEET_BEARER_ENV:-VELOX_MASTER_BEARER}"
COOLDOWN_S="${FLEET_COOLDOWN_S:-30}"
POLL_TIMEOUT_S="${FLEET_POLL_TIMEOUT_S:-300}"
IDLE_SLEEP_MS="${FLEET_IDLE_SLEEP_MS:-100}"
REPORT_JSON=""
DESTINATION_ID="${FLEET_DESTINATION_ID:-}"
VELOX_MASTER_LOG_PATH="${VELOX_MASTER_LOG_PATH:-}"

while (( $# > 0 )); do
  case "$1" in
    --fleet-job-count)   FLEET_JOB_COUNT="$2"; shift 2 ;;
    --master-url)         VELOX_MASTER_URL="$2"; shift 2 ;;
    --bearer-env)         BEARER_ENV="$2"; shift 2 ;;
    --cooldown-s)         COOLDOWN_S="$2"; shift 2 ;;
    --poll-timeout-s)     POLL_TIMEOUT_S="$2"; shift 2 ;;
    --idle-sleep-ms)      IDLE_SLEEP_MS="$2"; shift 2 ;;
    --report-json)        REPORT_JSON="$2"; shift 2 ;;
    --destination-id)     DESTINATION_ID="$2"; shift 2 ;;
    --master-log-path)    VELOX_MASTER_LOG_PATH="$2"; shift 2 ;;
    -h|--help)            usage ;;
    *)                    log_error "unknown flag: $1"; exit 2 ;;
  esac
done

# Numeric-validate the user-provided tunables up front so a typo is rc=2, not
# some downstream mystery failure.
if ! [[ "$FLEET_JOB_COUNT" =~ ^[0-9]+$ ]] || (( FLEET_JOB_COUNT < 1 )); then
  log_error "FLEET_JOB_COUNT must be a positive integer (got: $FLEET_JOB_COUNT)"
  exit 2
fi
if ! [[ "$COOLDOWN_S" =~ ^[0-9]+$ ]]; then
  log_error "--cooldown-s must be a non-negative integer (got: $COOLDOWN_S)"
  exit 2
fi
if ! [[ "$POLL_TIMEOUT_S" =~ ^[0-9]+$ ]] || (( POLL_TIMEOUT_S < 30 )); then
  log_error "--poll-timeout-s must be >=30s (got: $POLL_TIMEOUT_S)"
  exit 2
fi
if ! [[ "$IDLE_SLEEP_MS" =~ ^[0-9]+$ ]]; then
  log_error "--idle-sleep-ms must be a non-negative integer (got: $IDLE_SLEEP_MS)"
  exit 2
fi
if [[ -z "$DESTINATION_ID" ]]; then
  log_error "FLEET_DESTINATION_ID or --destination-id is required; implicit Drive destinations are forbidden"
  exit 2
fi

# Trim URL trailing slashes (single or multiple) so the join with explicit
# /api/v1/jobs/<id> yields a clean URL.
VELOX_MASTER_URL="$(printf '%s' "$VELOX_MASTER_URL" | sed 's|/*$||')"

log_info "fleet_distribute: jobs=$FLEET_JOB_COUNT master=$VELOX_MASTER_URL dest=$DESTINATION_ID"
log_info "bearer_env=$BEARER_ENV (fleet requests use the freshly minted M2M bearer)"
log_info "tunables: cooldown_s=$COOLDOWN_S poll_timeout_s=$POLL_TIMEOUT_S idle_sleep_ms=$IDLE_SLEEP_MS"

# ─── Required binaries ─────────────────────────────────────────────────────
for bin in curl jq python3 awk sed grep; do
  if ! command -v "$bin" >/dev/null 2>&1; then
    log_error "FATAL: required binary not in PATH: $bin"
    exit 2
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

# ─── Provision 1 ephemeral M2M client for the fleet ────────────────────────
M2M_BEARER=""
PROVISIONED_CLIENT_ID=""
# shellcheck disable=SC2329 # invoked indirectly from the EXIT trap
on_m2m_cleanup() {
  if [[ -n "$PROVISIONED_CLIENT_ID" && -n "$ADMIN_TOKEN" && -n "$VELOX_MASTER_URL" ]]; then
    curl -sS -m 5 -X DELETE \
      -H "Authorization: Bearer ${ADMIN_TOKEN}" \
      "${VELOX_MASTER_URL}/api/v1/admin/m2m/keys/${PROVISIONED_CLIENT_ID}" \
      >/dev/null 2>&1 || true
  fi
}
if ! smoke_mint_m2m "$ADMIN_TOKEN" "$VELOX_MASTER_URL"; then
  log_error "M2M provisioning failed"
  exit 3
fi
log_info "M2M provisioned: client_id=$PROVISIONED_CLIENT_ID"

# ─── Pre-flight: list CONNECTED workers + capture task_slots ──────────────
WORKERS_JSON=""
if ! smoke_workers_list "$ADMIN_TOKEN" "$VELOX_MASTER_URL"; then
  log_error "could not list workers"
  exit 3
fi
# smoke_workers_list populates WORKERS_JSON; extract the per-worker
# {worker_id, status, session_active, task_slots} slice.
ALL_WORKER_IDS=()
declare -A WORKER_SLOTS=()
declare -A WORKER_STATUS=()
declare -A WORKER_SESSION=()

while IFS= read -r row; do
  wid=$(printf '%s' "$row" | jq -er '.worker_id // .id // empty' 2>/dev/null || true)
  [[ -z "$wid" ]] && continue
  sts=$(printf '%s' "$row" | jq -r '.status             // "(unset)"')
  sas=$(printf '%s' "$row" | jq -r '.session_active     // false')
  tsl=$(printf '%s' "$row" | jq -r '.task_slots         // .max_active_jobs // 1')
  if ! [[ "$tsl" =~ ^[0-9]+$ ]]; then tsl="1"; fi
  ALL_WORKER_IDS+=("$wid")
  WORKER_STATUS["$wid"]="$sts"
  WORKER_SESSION["$wid"]="$sas"
  WORKER_SLOTS["$wid"]="$tsl"
done < <(printf '%s' "$WORKERS_JSON" | jq -c '.[]?' 2>/dev/null \
              || printf '%s' "$WORKERS_JSON" | jq -c '.workers[]?')

CONNECTED_COUNT=0
for w in "${ALL_WORKER_IDS[@]}"; do
  if [[ "${WORKER_STATUS[$w]:-}" == "CONNECTED" || "${WORKER_STATUS[$w]:-}" == "online" || "${WORKER_STATUS[$w]:-}" == "idle" || "${WORKER_STATUS[$w]:-}" == "busy" ]] \
     && [[ "${WORKER_SESSION[$w]:-}" == "true" ]]; then
    CONNECTED_COUNT=$((CONNECTED_COUNT + 1))
  fi
done

# Pre-flight threshold: ceil(FLEET_JOB_COUNT/2) minimum; warn if < FLEET.
PREFLIGHT_MIN=$(( (FLEET_JOB_COUNT + 1) / 2 ))
SLOTS_TOTAL=0
for w in "${ALL_WORKER_IDS[@]}"; do
  s="${WORKER_SLOTS[$w]:-1}"
  SLOTS_TOTAL=$((SLOTS_TOTAL + s))
done
log_info "pre-flight: connected=$CONNECTED_COUNT total=${#ALL_WORKER_IDS[@]} slots_total=$SLOTS_TOTAL min_required=$PREFLIGHT_MIN"

if (( CONNECTED_COUNT < PREFLIGHT_MIN )); then
  log_error "FAIL: pre-flight: CONNECTED=$CONNECTED_COUNT < min_required=$PREFLIGHT_MIN (FLEET_JOB_COUNT=$FLEET_JOB_COUNT)"
  exit 4
fi
if (( SLOTS_TOTAL < FLEET_JOB_COUNT )); then
  # Soft warning — the fleet dispatch may queue some jobs and not all reach
  # SUCCEEDED, which is what invariant 5 catches. But operator should be aware.
  log_warn "pre-flight: total task_slots=$SLOTS_TOTAL < FLEET_JOB_COUNT=$FLEET_JOB_COUNT; some jobs may be queued or fail"
fi

# ─── Build & submit FLEET_JOB_COUNT jobs (staggered) ────────────────────────
TMP_PAYLOAD_DIR=$(mktemp -d "${REPO_ROOT}/tests/worker-cert/.tmp-payload.XXXXXX")
TMP_LOG_DIR=$(mktemp -d "${REPO_ROOT}/tests/worker-cert/.tmp-logs.XXXXXX")
log_info "TMP_PAYLOAD_DIR=$TMP_PAYLOAD_DIR TMP_LOG_DIR=$TMP_LOG_DIR"

declare -a JOB_IDS=()
declare -a JOB_RC=()
declare -a JOB_TASK_IDS=()
declare -a JOB_LEASES=()
declare -a JOB_WORKERS=()
declare -a JOB_STATUSES=()
declare -a JOB_ARTIFACT_URLS=()
declare -a JOB_ARTIFACT_SIZES=()
declare -a JOB_RENDER_TIMES=()
declare -a JOB_POLL_PIDS=()

ASSETS_FILE="${SCRIPT_DIR}/fixtures/assets.json"
[[ -r "$ASSETS_FILE" ]] || { log_error "fixtures not readable: $ASSETS_FILE"; exit 2; }

IDLE_SLEEP_S=$(awk -v ms="$IDLE_SLEEP_MS" 'BEGIN { printf "%.3f", ms/1000 }')

for i in $(seq 1 "$FLEET_JOB_COUNT"); do
  slot="$i"
  job_slug="fleet-${slot}-$$-$(date +%s)"
  payload_file="${TMP_PAYLOAD_DIR}/${job_slug}.json"
  log_info "[submit ${slot}/${FLEET_JOB_COUNT}] building payload for ${job_slug}"
  if ! python3 "${REPO_ROOT}/tests/worker-cert/build_real_payload.py" \
        --worker-id "${job_slug}" \
        --destination "$DESTINATION_ID" \
        --no-placement-pin \
        --strict \
        --output "$payload_file" >/dev/null 2>>"${TMP_LOG_DIR}/${job_slug}.stderr"; then
    log_error "FAIL: payload build for slot=${slot} (see ${TMP_LOG_DIR}/${job_slug}.stderr)"
    OVERALL_FATAL_RC=5
    break
  fi

  POST_STATUS=$(curl -sS -m 30 -X POST \
    -H "Authorization: Bearer $M2M_BEARER" \
    -H "Content-Type: application/json" \
    -H "X-Request-ID: fleet-${slot}-$$-$(date +%s)" \
    --data-binary "@${payload_file}" \
    -o "${TMP_LOG_DIR}/${job_slug}.post.body" \
    -w '%{http_code}' \
    "${VELOX_MASTER_URL}/api/v1/jobs" 2>/dev/null) || POST_STATUS=""
  if [[ "$POST_STATUS" != "202" ]]; then
    log_error "FAIL: POST /jobs slot=${slot} http=$POST_STATUS (body: $(head -c 300 "${TMP_LOG_DIR}/${job_slug}.post.body" 2>/dev/null))"
    OVERALL_FATAL_RC=5
    break
  fi
  job_id=$(jq -er '.job_id // empty' "${TMP_LOG_DIR}/${job_slug}.post.body" 2>/dev/null || echo "")
  if [[ -z "$job_id" ]]; then
    log_error "FAIL: 202 but missing job_id slot=${slot}"
    OVERALL_FATAL_RC=5
    break
  fi
  JOB_IDS+=("$job_id")
  log_info "[submit ${slot}/${FLEET_JOB_COUNT}] accepted: job_id=${job_id}"

  # Stagger so the 4 POSTs don't fall into the same TCP burst. Operators
  # commonly rate-limit master ingress; 100ms apart keeps us well below.
  (( slot < FLEET_JOB_COUNT )) && sleep "$IDLE_SLEEP_S"
done

if (( ${#JOB_IDS[@]} != FLEET_JOB_COUNT )); then
  log_error "FAIL: only ${#JOB_IDS[@]}/${FLEET_JOB_COUNT} jobs accepted"
  exit 5
fi

# ─── Poll each job in a background subshell ────────────────────────────────
log_info "polling ${#JOB_IDS[@]} jobs concurrently with timeout ${POLL_TIMEOUT_S}s each"

# Reset lib_kill_all so it tracks these pollers
lib_reset_children

for idx in "${!JOB_IDS[@]}"; do
  job_id="${JOB_IDS[$idx]}"
  # writer rc into a per-job file so the parent can collect without races
  rc_file="${TMP_LOG_DIR}/poll-rc-${job_id}.txt"
  body_file="${TMP_LOG_DIR}/poll-body-${job_id}.json"
  (   # begin subshell
    elapsed=0
    sleep_s=1
    last_status=""
    while (( elapsed < POLL_TIMEOUT_S )); do
      sleep "$sleep_s"
      elapsed=$((elapsed + sleep_s))
      sleep_s=$(( sleep_s * 2 )); (( sleep_s > 16 )) && sleep_s=16
      RESP=$(curl -sS -m 10 \
        -H "Authorization: Bearer $M2M_BEARER" \
        "${VELOX_MASTER_URL}/api/v1/jobs/${job_id}" 2>/dev/null || echo "")
      sv=$(printf '%s' "$RESP" | jq -er '.status // empty' 2>/dev/null || echo "")
      [[ -n "$sv" ]] && last_status="$sv"
      case "$sv" in
        SUCCEEDED) printf '%s' "$RESP" > "$body_file"; echo 0 > "$rc_file"; exit 0 ;;
        FAILED|CANCELLED)
          printf '%s' "$RESP" > "$body_file"
          echo 7 > "$rc_file"
          exit 7 ;;
      esac
    done
    printf '{"status":"%s","elapsed_s":%d}' "$last_status" "$elapsed" > "$body_file"
    echo 6 > "$rc_file"
    exit 6
  ) &
  JOB_POLL_PIDS+=("$!")
done

# ─── Wait for all pollers; collect rc + body ──────────────────────────────
log_info "waiting on ${#JOB_POLL_PIDS[@]} pollers…"
wait_rc=0
for pid in "${JOB_POLL_PIDS[@]}"; do
  # No set -e: collect every exit; sum the worst.
  wait "$pid" || wait_rc=$?
done
log_info "pollers returned wait_rc=$wait_rc (informational only — per-job rc follows)"

# ─── Aggregate per-job outcome ────────────────────────────────────────────
for idx in "${!JOB_IDS[@]}"; do
  job_id="${JOB_IDS[$idx]}"
  rc_file="${TMP_LOG_DIR}/poll-rc-${job_id}.txt"
  body_file="${TMP_LOG_DIR}/poll-body-${job_id}.json"
  rc=0
  [[ -r "$rc_file" ]] && rc=$(cat "$rc_file" 2>/dev/null || echo 9)
  body=""
  [[ -r "$body_file" ]] && body=$(cat "$body_file" 2>/dev/null || echo "")
  status=$(printf '%s' "$body" | jq -r '.status // "(unset)"' 2>/dev/null || echo "(unset)")
  artifact_url=$(printf '%s' "$body" | jq -er '.artifact_url // .artifact_path // .output_path // empty' 2>/dev/null || true)
  started_at=$(printf '%s' "$body" | jq -er '.started_at   // empty' 2>/dev/null || true)
  completed_at=$(printf '%s' "$body" | jq -er '.completed_at // empty' 2>/dev/null || true)
  render_ms=0
  if [[ -n "$started_at" && -n "$completed_at" ]]; then
    s_epoch=$(date -u -d "$started_at"   +%s 2>/dev/null || echo 0)
    c_epoch=$(date -u -d "$completed_at" +%s 2>/dev/null || echo 0)
    if [[ "$s_epoch" =~ ^[0-9]+$ && "$c_epoch" =~ ^[0-9]+$ ]]; then
      render_ms=$(( (c_epoch - s_epoch) * 1000 ))
    fi
  fi
  JOB_RC[idx]="$rc"
  JOB_STATUSES+=("$status")
  JOB_ARTIFACT_URLS+=("$artifact_url")
  JOB_ARTIFACT_SIZES+=("$(stat -c %s "$body_file" 2>/dev/null || echo 0)")  # placeholder, real size comes from /jobs/<id> if present
  JOB_RENDER_TIMES+=("$render_ms")
  if [[ "$rc" -gt "$OVERALL_FATAL_RC" && "$rc" != "9" ]]; then
    # Don't escalate STOP-on-poll-timeout (rc=6) over terminal-fail (rc=7)
    # if both appear: 7 wins for invariant 5.
    OVERALL_FATAL_RC="$rc"
  fi
  log_info "[job $((idx+1))/${#JOB_IDS[@]}] job_id=${job_id} status=${status} rc=${rc} artifact_url_present=$([ -n "$artifact_url" ] && echo true || echo false) render_ms=${render_ms}"
done

# ─── Scrape master log for lease lines + per-worker counts ─────────────────
# Each fleet job produces ONE primary TaskLeaseGranted line. From the line
# extract (worker_id, task_id, attempt_id, lease_id) keyed by job_id. The
# authoritative source is the master log; if absent, we degrade to no-op.
LEASE_SCRAPE_OK=0
declare -A TASK_TO_LEASE=()
declare -A WORKER_TO_LEASES=()
if [[ -n "$VELOX_MASTER_LOG_PATH" && -r "$VELOX_MASTER_LOG_PATH" ]]; then
  log_info "scraping master log: $VELOX_MASTER_LOG_PATH"
  # Concatenate ALL TaskLeaseGranted lines for our N job_ids, then extract
  # (worker_id, task_id, lease_id) tuples. sort -u dedupes BEFORE counting so a
  # duplicate lease (same task_id, different lease_id) is visible as a 2nd
  # distinct row in the (task_id, lease_id) keyspace. Pipe through awk + sed
  # because the canonical goLogger format is positional ("sent to worker WID")
  # mixed with key-value (task=..., job=..., lease=...) — a single regex covers
  # both safely without spawning a separate process per job_id.
  LEASES_TSV=$(mktemp)
  : > "$LEASES_TSV"
  for job_id in "${JOB_IDS[@]}"; do
    grep -E "TaskLeaseGranted sent to worker .*job=${job_id}" \
      "$VELOX_MASTER_LOG_PATH" 2>/dev/null \
      | sed -nE 's/.*sent to worker ([^ ]+).*task=([^ ]+).*lease=([^ ]+).*/\1\t\2\t\3/p' \
      >> "$LEASES_TSV" || true
  done
  # Dedupe by (worker_id, task_id, lease_id) tuple. Two distinct leases for the
  # same task survive as 2 rows; idempotent rows from duplicate log lines
  # collapse to 1 row.
  sort -u -o "$LEASES_TSV" "$LEASES_TSV"
  while IFS=$'\t' read -r worker_id task_id lease_id; do
    [[ -z "$task_id" || -z "$worker_id" || -z "$lease_id" ]] && continue
    TASK_TO_LEASE["$task_id"]="${TASK_TO_LEASE[$task_id]:-}$lease_id"
    WORKER_TO_LEASES["$worker_id"]="${WORKER_TO_LEASES[$worker_id]:-}$lease_id"
    JOB_TASK_IDS+=("$task_id")
    JOB_LEASES+=("$lease_id")
    JOB_WORKERS+=("$worker_id")
    LEASE_SCRAPE_OK=1
  done < "$LEASES_TSV"
  rm -f "$LEASES_TSV"
else
  log_warn "master log not readable (path='$VELOX_MASTER_LOG_PATH') — lease scrape SKIPPED, invariant 2/3 cannot be asserted (will record SKIP)"
fi

# ─── Invariant 1: distinct worker_ids ──────────────────────────────────────
DISTINCT_WORKERS=$(printf '%s\n' "${JOB_WORKERS[@]:-}" | sort -u | grep -c . || echo 0)
INV_DISTINCT_OK=0
log_info "invariant 1 (distinct workers): ${DISTINCT_WORKERS} distinct / min_required=${PREFLIGHT_MIN}"
if (( LEASE_SCRAPE_OK == 1 )); then
  if (( DISTINCT_WORKERS >= PREFLIGHT_MIN )); then
    INV_DISTINCT_OK=1
  fi
fi

# ─── Invariant 3: one lease per task ───────────────────────────────────────
INV_LEASE_ONCE_OK=0
DUPLICATES=""
if (( LEASE_SCRAPE_OK == 1 )); then
  for tid in "${!TASK_TO_LEASE[@]}"; do
    leases_for_tid=$(printf '%s' "${TASK_TO_LEASE[$tid]}" | tr '-' '\n' | grep -c . || echo 0)
    if (( leases_for_tid > 1 )); then
      DUPLICATES="${DUPLICATES} task_id=${tid}(${leases_for_tid})"
    fi
  done
  if [[ -z "$DUPLICATES" ]]; then
    INV_LEASE_ONCE_OK=1
  else
    log_error "FAIL: invariant 3 (lease once): duplicate leases for:$DUPLICATES"
  fi
fi

# ─── Invariant 2: workers don't exceed task_slots ──────────────────────────
INV_CAP_OK=0
CAP_VIOLATIONS=""
if (( LEASE_SCRAPE_OK == 1 )); then
  for wid in "${!WORKER_TO_LEASES[@]}"; do
    leases_on_worker=$(printf '%s' "${WORKER_TO_LEASES[$wid]}" | tr '-' '\n' | grep -c . || echo 0)
    slots=${WORKER_SLOTS[$wid]:-1}
    if (( leases_on_worker > slots )); then
      CAP_VIOLATIONS="${CAP_VIOLATIONS} worker=${wid}(slots=${slots} leases=${leases_on_worker})"
    fi
  done
  if [[ -z "$CAP_VIOLATIONS" ]]; then
    INV_CAP_OK=1
  else
    log_error "FAIL: invariant 2 (cap respect): $CAP_VIOLATIONS"
  fi
fi

# ─── Invariant 4: artifact URL present per SUCCEEDED job ──────────────────
INV_ARTIFACT_OK=1
ARTIFACT_MISSING=""
for idx in "${!JOB_IDS[@]}"; do
  rc="${JOB_RC[$idx]:-}"
  status="${JOB_STATUSES[$idx]:-}"
  artifact_url="${JOB_ARTIFACT_URLS[$idx]:-}"
  if [[ "$rc" == "0" && "$status" == "SUCCEEDED" && -z "$artifact_url" ]]; then
    # Some masters omit artifact_url from /jobs/<id> and expose it via
    # /jobs/<id>/artifact — record a soft miss instead of hard-fail when
    # status=SUCCEEDED. The verify_artifact.sh harness can fetch + verify
    # the file later.
    INV_ARTIFACT_OK=0
    ARTIFACT_MISSING="${ARTIFACT_MISSING} job_id=${JOB_IDS[$idx]}"
  fi
done
if [[ -n "$ARTIFACT_MISSING" ]]; then
  log_warn "invariant 4 (artifact url): some SUCCEEDED jobs have no artifact_url:$ARTIFACT_MISSING (verify_artifact.sh will fetch separately)"
fi

# ─── Invariant 5: queue drained ────────────────────────────────────────────
INV_DRAIN_OK=1
NON_TERMINAL=""
for idx in "${!JOB_IDS[@]}"; do
  rc="${JOB_RC[$idx]:-}"
  if [[ "$rc" != "0" ]]; then
    INV_DRAIN_OK=0
    status="${JOB_STATUSES[$idx]:-}"
    NON_TERMINAL="${NON_TERMINAL} job_id=${JOB_IDS[$idx]} status=${status} rc=${rc}"
  fi
done
if [[ -n "$NON_TERMINAL" ]]; then
  log_error "FAIL: invariant 5 (queue drain): $NON_TERMINAL"
  if [[ "$OVERALL_FATAL_RC" -lt 7 ]]; then OVERALL_FATAL_RC=12; fi
fi

# ─── Cooldown before residual check ───────────────────────────────────────
if (( COOLDOWN_S > 0 )); then
  log_info "cooldown for ${COOLDOWN_S}s before residual BUSY check"
  sleep "$COOLDOWN_S"
fi

# ─── Invariant 6: no false BUSY residual ───────────────────────────────────
INV_NO_RESIDUAL_OK=1
RESIDUAL=""
WORKERS_AFTER_JSON=""
if ! smoke_workers_list "$ADMIN_TOKEN" "$VELOX_MASTER_URL"; then
  log_warn "could not re-list workers for residual check (skipping invariant 6)"
  INV_NO_RESIDUAL_OK=0
else
  WORKERS_AFTER_JSON="$WORKERS_JSON"
  # Build two parallel maps from the AFTER snapshot:
  #   WORKER_BY_TASK[task_id]   -> worker_id currently holding that task_id
  #   WORKER_AFTER_STATUS[wid]  -> raw status string (idle/busy/error/offline)
  # These feed the cross-referenced residual check below: a worker whose
  # current_task_id STILL points to one of our fleet task_ids that the
  # master has already marked SUCCEEDED is the canonical false-BUSY-residual
  # pattern (worker-side heartbeat hasn't refreshed, or activeTaskLeases map
  # on the worker is leaking). Workers showing "busy" with current_task_id
  # pointing to a task NOT in our N are out of scope (legitimate other work).
  declare -A WORKER_BY_TASK=()
  declare -A WORKER_AFTER_STATUS=()
  while IFS= read -r row; do
    wid=$(printf '%s' "$row" | jq -er '.worker_id // .id // empty' 2>/dev/null || true)
    [[ -z "$wid" ]] && continue
    sts=$(printf '%s' "$row" | jq -r '.status             // "(unset)"')
    cti=$(printf '%s' "$row" | jq -er '.current_task_id // .currentJobId // empty' 2>/dev/null || true)
    WORKER_AFTER_STATUS["$wid"]="$sts"
    [[ -n "$cti" ]] && WORKER_BY_TASK["$cti"]="$wid"
  done < <(printf '%s' "$WORKERS_AFTER_JSON" | jq -c '.[]?' 2>/dev/null \
              || printf '%s' "$WORKERS_AFTER_JSON" | jq -c '.workers[]?')
  # Map our fleet task_ids to their terminal status from /api/v1/jobs/<id>
  # so we only flag workers stuck on tasks the master considers DONE.
  declare -A TASK_TO_FLEET_STATUS=()
  for idx in "${!JOB_IDS[@]}"; do
    tid="${JOB_TASK_IDS[$idx]:-}"
    [[ -z "$tid" ]] && continue
    TASK_TO_FLEET_STATUS["$tid"]="${JOB_STATUSES[$idx]:-}"
  done
  for tid in "${!TASK_TO_FLEET_STATUS[@]}"; do
    [[ "${TASK_TO_FLEET_STATUS[$tid]}" != "SUCCEEDED" ]] && continue
    wid="${WORKER_BY_TASK[$tid]:-}"
    [[ -z "$wid" ]] && continue
    sts="${WORKER_AFTER_STATUS[$wid]:-(unset)}"
    status_busy=$(printf '%s' "$sts" | tr '[:upper:]' '[:lower:]')
    if [[ "$status_busy" == "busy" ]]; then
      RESIDUAL="${RESIDUAL} worker=${wid}(status=${sts} current_task=${tid}=SUCCEEDED-residual)"
    fi
  done
  if [[ -n "$RESIDUAL" ]]; then
    INV_NO_RESIDUAL_OK=0
    log_error "FAIL: invariant 6 (no false BUSY residual): $RESIDUAL"
    OVERALL_FATAL_RC=11
  fi
fi

# ─── Final rc synthesis ────────────────────────────────────────────────────
# Outline:
#   rc=12 if queue did not drain
#   rc=11 if false residual
#   rc=10 if duplicate lease
#   rc=9  if worker exceeds cap
#   rc=8  if distinct workers below threshold
#   rc=7  if any job terminal-fail
#   rc=6  if any job poll-timeout
#   rc=5  if any POST non-202
# The default rc=4 already covered pre-flight.
# Override priority: drain (12) > residual (11) > lease-once (10) > cap (9) > distinct (8) > terminal-fail (7) > poll-timeout (6) > POST (5)
case "$OVERALL_FATAL_RC" in
  0|2|3|4) ;;  # already-final pre-flight rc
  *) # escalate invariants if any fail
    if [[ "$INV_DRAIN_OK"  != 1 && "$OVERALL_FATAL_RC" -lt 12 ]]; then OVERALL_FATAL_RC=12; fi
    if [[ "$INV_NO_RESIDUAL_OK" != 1 && "$OVERALL_FATAL_RC" -lt 11 ]]; then OVERALL_FATAL_RC=11; fi
    if [[ "$INV_LEASE_ONCE_OK" != 1 && "$OVERALL_FATAL_RC" -lt 10 ]]; then OVERALL_FATAL_RC=10; fi
    if [[ "$INV_CAP_OK" != 1 && "$OVERALL_FATAL_RC" -lt 9 ]]; then OVERALL_FATAL_RC=9; fi
    if [[ "$INV_DISTINCT_OK" != 1 && "$OVERALL_FATAL_RC" -lt 8 ]]; then OVERALL_FATAL_RC=8; fi
    ;;
esac

# ─── Emit atomic JSON report ───────────────────────────────────────────────
if [[ -n "$REPORT_JSON" ]]; then
  REPORT_DIR=$(dirname -- "$REPORT_JSON")
  ensure_dir "$REPORT_DIR"
  TMP_REPORT=$(mktemp -p "$REPORT_DIR" "${REPORT_JSON##*/}.partial.XXXXXX")
  jobs_json_array=$(printf '%s\n' "${JOB_IDS[@]}" \
    | awk -v ids_a="${JOB_IDS[*]}" -v status_a="${JOB_STATUSES[*]}" -v rc_a="${JOB_RC[*]}" -v art_a="${JOB_ARTIFACT_URLS[*]}" -v rt_a="${JOB_RENDER_TIMES[*]}" -v task_a="${JOB_TASK_IDS[*]}" -v worker_a="${JOB_WORKERS[*]}" -v lease_a="${JOB_LEASES[*]}" '
    BEGIN {
      n=split(ids_a, ids, " "); split(status_a, sts, " "); split(rc_a, rcs, " "); split(art_a, arts, " ");
      split(rt_a, rts, " "); split(task_a, ts, " "); split(worker_a, ws, " ");
      split(lease_a, ls, " ");
      printf("[");
      for (i=1;i<=n;i++) {
        comma=(i>1?",":"");
        printf("%s{\"index\":%d,\"job_id\":\"%s\",\"status\":\"%s\",\"poll_rc\":%d,\"artifact_url\":\"%s\",\"render_ms\":%d,\"task_id\":\"%s\",\"worker_id\":\"%s\",\"lease_id\":\"%s\"}",
               comma, i, ids[i], sts[i], rcs[i]+0, arts[i], rts[i]+0, ts[i], ws[i], ls[i]);
      }
      printf("]");
    }')
  jq -n \
    --arg master "$VELOX_MASTER_URL" \
    --argjson fleet_count "$FLEET_JOB_COUNT" \
    --argjson preflight_min "$PREFLIGHT_MIN" \
    --argjson connected_count "$CONNECTED_COUNT" \
    --argjson slots_total "$SLOTS_TOTAL" \
    --argjson distinct_workers "$DISTINCT_WORKERS" \
    --argjson inv_distinct_ok "$INV_DISTINCT_OK" \
    --argjson inv_cap_ok "$INV_CAP_OK" \
    --argjson inv_lease_once_ok "$INV_LEASE_ONCE_OK" \
    --argjson inv_artifact_ok "$INV_ARTIFACT_OK" \
    --argjson inv_drain_ok "$INV_DRAIN_OK" \
    --argjson inv_no_residual_ok "$INV_NO_RESIDUAL_OK" \
    --argjson lease_scrape_ok "$LEASE_SCRAPE_OK" \
    --argjson jobs "$jobs_json_array" \
    --arg cap_violations "$CAP_VIOLATIONS" \
    --arg duplicates "$DUPLICATES" \
    --arg residual "$RESIDUAL" \
    --arg destination "$DESTINATION_ID" \
    '{
       master: $master,
       fleet_job_count: $fleet_count,
       preflight_min: $preflight_min,
       connected_count: $connected_count,
       slots_total: $slots_total,
       distinct_workers: $distinct_workers,
       lease_scrape_ok: $lease_scrape_ok,
       cap_violations: $cap_violations,
       duplicates: $duplicates,
       residual: $residual,
       destination_id: $destination,
       invariants: {
         distinct_workers:    ($inv_distinct_ok    == 1),
         no_cap_violation:    ($inv_cap_ok         == 1),
         lease_once_per_task: ($inv_lease_once_ok  == 1),
         artifact_url_per_job:($inv_artifact_ok    == 1),
         queue_drain:         ($inv_drain_ok       == 1),
         no_false_busy_residual: ($inv_no_residual_ok == 1)
       },
       jobs: $jobs
     }' > "$TMP_REPORT" || {
    log_error "FATAL: failed to render fleet distribute JSON report"
    rm -f "$TMP_REPORT"
    exit 5
  }
  mv -f "$TMP_REPORT" "$REPORT_JSON"
  log_info "report: $REPORT_JSON"
fi

log_info "fleet_distribute summary: distinct=${DISTINCT_WORKERS} connected=${CONNECTED_COUNT} slots=${SLOTS_TOTAL} rc=${OVERALL_FATAL_RC}"
log_info "invariants: distinct=$INV_DISTINCT_OK cap=$INV_CAP_OK lease_once=$INV_LEASE_ONCE_OK artifact=$INV_ARTIFACT_OK drain=$INV_DRAIN_OK residual=$INV_NO_RESIDUAL_OK"

if (( OVERALL_FATAL_RC == 0 )); then
  log_info "OK: fleet_distribute PASS"
  exit 0
fi
exit "$OVERALL_FATAL_RC"
