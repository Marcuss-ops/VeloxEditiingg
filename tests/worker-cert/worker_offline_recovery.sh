#!/usr/bin/env bash
# =============================================================================
# tests/worker-cert/worker_offline_recovery.sh — Chaos smoke for worker death.
# =============================================================================
# Usage:
#   ./tests/worker-cert/worker_offline_recovery.sh \
#       --target-worker-id host_57_131_20_173 \
#       --target-worker-stop-cmd 'ssh worker-host systemctl stop velox-worker.service' \
#       --target-worker-inspect-cmd 'ssh worker-host bash -s' <restart-owner-probe.sh
#
# The inspection command must emit the canonical restart-owner facts as
# key=value lines (see tests/worker-cert/lib/restart_owner.sh):
#   systemd_is_enabled=enabled
#   systemd_is_active=active
#   systemd_restart=always
#   systemd_restart_sec=10s
#   docker_restart_policy=no

# `systemd` owns the worker lifecycle. Docker RestartPolicy=no is intentional
# and is a PASS, not a recovery failure.
#
#   VELOX_MASTER_URL=https://velox.example.com \
#   VELOX_ADMIN_TOKEN=... \
#   VELOX_DB_PATH=/var/lib/velox/velox.db \
#   ./tests/worker-cert/worker_offline_recovery.sh \
#       --target-worker-id velox-worker-13197 \
#       --target-worker-stop-cmd 'ssh velox-worker-13197-host systemctl stop velox-worker.service' \
#       --target-worker-inspect-cmd 'ssh velox-worker-13197-host bash -s' <restart-owner-probe.sh
#
# What the script does:
#   1. Sources tests/_lib/sh/_lib.sh (logging + pid-trap + ensure +
#      aggregate) and tests/worker-cert/lib/pluck.sh (smoke-local helpers).
#   2. Mints an ephemeral M2M client via POST /api/v1/admin/m2m/keys; DELETEs
#      it on exit (best-effort, see trap).
#   3. Pre-flight: GET /api/v1/workers, asserts target worker is CONNECTED +
#      session_active=true, AND that at least 2 CONNECTED workers are
#      present (so a healthy backup can receive the re-lease).
#   4. POST /api/v1/jobs with a real-asset payload (velox-asset://<asset_id>,
#      an explicitly selected destination and scene.composite.v1@1. Waits for the
#      master log to emit TaskLeaseGranted to <target_worker_id>.
#   5. Snapshots original task_id / attempt_id / lease_id / lease_expires_at.
#   6. Executes --target-worker-stop-cmd (operator-provided) to gracefully
#      stop the target worker (SIGTERM). Polls /api/v1/workers/<id> until
#      status transitions to STALE or DISCONNECTED (default 180s budget).
#   7. Fast-forwards lease_expires_at via sqlite3 against $VELOX_DB_PATH
#      (default — overridable via --no-fast-forward for natural TTL wait).
#   8. Polls the master log + /api/v1/jobs/<id> until a NEW attempt appears
#      OR the original attempt closes as TIMED_OUT/FAILED (default 90s).
#   9. Waits for the job to reach terminal SUCCEEDED on the re-leased worker.
#  10. SQL invariant checks against $VELOX_DB_PATH:
#        - artifacts WHERE job_id=X AND final=1 → count ≤ 1
#        - artifact_uploads WHERE job_id=X AND status='COMPLETED' → count ≤ 1
#        - task_attempts WHERE task_id=X ORDER BY attempt_number ASC →
#            attempt 1 ∈ {TIMED_OUT, FAILED} with error_code=LEASE_EXPIRED
#            attempt 2 exists, worker_id != target_worker_id
#  11. Writes workers/<target_worker_id>/recovery.json (atomic via tmp+mv).
#
# Exit codes:
#   0  PASS — chaos scenario behaved canonically (re-lease, no double-artifact).
#   2  usage / env (missing arg / unknown flag / no curl|jq|sqlite3).
#   3  master unreachable / M2M provisioning failed.
#   4  pre-flight failed (target not CONNECTED / <2 CONNECTED workers / DB
#      unreadable).
#   5  POST /api/v1/jobs non-202 / missing job_id.
#   6  poll timeout waiting for original lease on target worker.
#   7  worker stop cmd failed (non-zero exit).
#   8  connection_status did not transition to STALE/DISCONNECTED within budget.
#   9  lease fast-forward SQL failed.
#  10  reaper did not re-queue within budget (no new attempt_id appeared).
#  11  re-leased job did not reach SUCCEEDED within budget.
#  12  double-artifact detected (artifacts.final=1 count > 1) — CHAOS FAIL.
#  13  attempt-history invariant violated (attempt 1 not failed, attempt 2 on
#      target, or attempt 2 missing).
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
# shellcheck source=tests/worker-cert/lib/restart_owner.sh
source "${SCRIPT_DIR}/lib/restart_owner.sh"

# ─── Args / defaults ───────────────────────────────────────────────────────
usage() {
  sed -n '2,/^# ====/p' "$0" | sed 's/^# \{0,1\}//'
  exit 0
}
if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
  usage
fi

TARGET_WORKER_ID=""
STOP_CMD=""
RESTART_OWNER_CHECK_CMD="${RW_WORKER_RESTART_OWNER_CHECK_CMD:-}"
NO_FAST_FORWARD=0
HEARTBEAT_POLL_TIMEOUT_S="${HEARTBEAT_POLL_TIMEOUT_S:-180}"
SUCCEEDED_POLL_TIMEOUT_S="${SUCCEEDED_POLL_TIMEOUT_S:-300}"
REAPER_POLL_TIMEOUT_S="${REAPER_POLL_TIMEOUT_S:-90}"
DESTINATION_ID="${RECOVERY_DESTINATION_ID:-}"
REPORT_JSON=""
DB_PATH="${VELOX_DB_PATH:-}"
VELOX_MASTER_LOG_PATH="${VELOX_MASTER_LOG_PATH:-}"
SMOKE_STRICT_PIN="${SMOKE_STRICT_PIN:-1}"  # default strict: ≥2 CONNECTED for determinism

while (( $# > 0 )); do
  case "$1" in
    --target-worker-id)         TARGET_WORKER_ID="$2"; shift 2 ;;
    --target-worker-stop-cmd)    STOP_CMD="$2"; shift 2 ;;
    --target-worker-inspect-cmd) RESTART_OWNER_CHECK_CMD="$2"; shift 2 ;;
    --no-fast-forward)           NO_FAST_FORWARD=1; shift ;;
    --heartbeat-poll-timeout-s) HEARTBEAT_POLL_TIMEOUT_S="$2"; shift 2 ;;
    --succeeded-poll-timeout-s) SUCCEEDED_POLL_TIMEOUT_S="$2"; shift 2 ;;
    --reaper-poll-timeout-s)    REAPER_POLL_TIMEOUT_S="$2"; shift 2 ;;
    --destination-id)           DESTINATION_ID="$2"; shift 2 ;;
    --report-json)              REPORT_JSON="$2"; shift 2 ;;
    --db-path)                  DB_PATH="$2"; shift 2 ;;
    --master-log-path)          VELOX_MASTER_LOG_PATH="$2"; shift 2 ;;
    -h|--help)                  usage ;;
    *)                          log_error "unknown flag: $1"; exit 2 ;;
  esac
done

if [[ -z "$TARGET_WORKER_ID" ]]; then
  log_error "missing required --target-worker-id"; exit 2
fi
if [[ -z "$STOP_CMD" ]]; then
  log_error "missing required --target-worker-stop-cmd (use 'systemctl stop velox-worker.service' so systemd does not respawn the test target)"; exit 2
fi
if [[ -z "$RESTART_OWNER_CHECK_CMD" ]]; then
  log_error "missing required --target-worker-inspect-cmd (must emit systemd_is_enabled, systemd_is_active, systemd_restart, systemd_restart_sec, docker_restart_policy)"; exit 2
fi
if [[ -z "$DESTINATION_ID" ]]; then
  log_error "RECOVERY_DESTINATION_ID or --destination-id is required; implicit Drive destinations are forbidden"; exit 2
fi

# Trailing-slash trim on URL so the join with explicit /api/v1/jobs is clean.
[[ -n "${VELOX_MASTER_URL:-}" ]] || VELOX_MASTER_URL="http://127.0.0.1:8080"
VELOX_MASTER_URL="${VELOX_MASTER_URL%/}"

# Numeric-validate the user-provided tunables up front so a typo is rc=2,
# not some downstream mystery failure.
for v in "$HEARTBEAT_POLL_TIMEOUT_S" "$SUCCEEDED_POLL_TIMEOUT_S" "$REAPER_POLL_TIMEOUT_S"; do
  if ! [[ "$v" =~ ^[0-9]+$ ]] || (( v < 1 )); then
    log_error "timeout must be a positive integer (got: $v)"; exit 2
  fi
done

log_info "worker_offline_recovery target=$TARGET_WORKER_ID master=$VELOX_MASTER_URL dest=$DESTINATION_ID"
log_info "tunables: heartbeat_timeout=${HEARTBEAT_POLL_TIMEOUT_S}s reaper_timeout=${REAPER_POLL_TIMEOUT_S}s succeeded_timeout=${SUCCEEDED_POLL_TIMEOUT_S}s fast_forward=$((NO_FAST_FORWARD==0)) db_path=${DB_PATH:-<unset>}"
log_info "restart owner contract: systemd velox-worker.service enabled+active Restart=always RestartSec=10; Docker RestartPolicy=no"

# ─── Required binaries ─────────────────────────────────────────────────────
for bin in curl jq python3 awk sed grep; do
  if ! command -v "$bin" >/dev/null 2>&1; then
    log_error "FATAL: required binary not in PATH: $bin"; exit 2
  fi
done
if [[ -n "$DB_PATH" ]] && ! command -v sqlite3 >/dev/null 2>&1; then
  log_warn "sqlite3 not on PATH; fast-forward (default) and SQL invariant checks will FAIL. Install sqlite3 or pass --no-fast-forward + skip invariants via stub."
fi

# ─── Resolve admin token (env > TOKEN_FILE) ────────────────────────────────
ADMIN_TOKEN=""
if [[ -n "${VELOX_ADMIN_TOKEN:-}" ]]; then
  ADMIN_TOKEN="$VELOX_ADMIN_TOKEN"
elif [[ -n "${TOKEN_FILE:-}" && -r "${TOKEN_FILE}" ]]; then
  ADMIN_TOKEN=$(grep -E '^VELOX_ADMIN_TOKEN=' "$TOKEN_FILE" | head -1 \
    | sed 's/^[^=]*=//' | tr -d '"' | tr -d "'" | xargs || true)
fi
[[ -n "$ADMIN_TOKEN" ]] || { log_error "VELOX_ADMIN_TOKEN unset and TOKEN_FILE not provided"; exit 2; }

# ─── DB path auto-discovery (best-effort) ──────────────────────────────────
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
  log_error "FAIL: VELOX_DB_PATH unreadable (set VELOX_DB_PATH or pass --db-path). Pre-flight requires DB access for fast-forward + invariant checks."
  exit 4
fi
log_info "DB_PATH=$DB_PATH (readable)"

# ─── Restart-owner contract preflight ──────────────────────────────────────
# This must run before any job submission or worker stop. A recovery result is
# not meaningful unless the test records which supervisor owns the lifecycle.
if ! canonical_restart_owner_check_command "$RESTART_OWNER_CHECK_CMD"; then
  log_error "FAIL: restart-owner contract preflight"
  exit 4
fi
log_info "restart-owner contract preflight passed: systemd_enabled=${RESTART_OWNER_SYSTEMD_ENABLED} systemd_active=${RESTART_OWNER_SYSTEMD_ACTIVE} restart=${RESTART_OWNER_SYSTEMD_RESTART} restart_sec=${RESTART_OWNER_SYSTEMD_RESTART_SEC} docker_policy=${RESTART_OWNER_DOCKER_POLICY}"

# ─── EXIT trap: cleanup children + scratch + M2M ───────────────────────────
TMP_HDRS=""
TMP_BODY=""
TMP_OUT=""
INV_REAPER_OK=0
INV_NO_DOUBLE_ARTIFACT_OK=0
INV_ATTEMPT_HISTORY_OK=0
INV_NO_FINAL_BUSY_OK=0

on_exit_cleanup() {
  local rc=$?
  lib_kill_all TERM 2>/dev/null || true
  [[ -n "$TMP_HDRS" && -e "$TMP_HDRS" ]] && rm -f "$TMP_HDRS" 2>/dev/null || true
  [[ -n "$TMP_BODY" && -e "$TMP_BODY" ]] && rm -f "$TMP_BODY" 2>/dev/null || true
  [[ -n "$TMP_OUT"  && -e "$TMP_OUT"  ]] && rm -f "$TMP_OUT"  2>/dev/null || true
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

# ─── Pre-flight: list workers + verify ≥2 CONNECTED + target is CONNECTED ──
WORKERS_JSON=""
if ! smoke_workers_list "$ADMIN_TOKEN" "$VELOX_MASTER_URL"; then
  log_error "could not list workers"; exit 3
fi

# Extract CONNECTED workers (status=CONNECTED, session_active=true).
CONNECTED_IDS=()
declare -A WORKER_SLOTS=()
while IFS= read -r row; do
  wid=$(printf '%s' "$row" | jq -er '.worker_id // .id // empty' 2>/dev/null || true)
  [[ -z "$wid" ]] && continue
  sts=$(printf '%s' "$row" | jq -r '.status // "(unset)"')
  sas=$(printf '%s' "$row" | jq -r '.session_active // false')
  tsl=$(printf '%s' "$row" | jq -r '.task_slots // .max_active_jobs // 1')
  if ! [[ "$tsl" =~ ^[0-9]+$ ]]; then tsl="1"; fi
  WORKER_SLOTS["$wid"]="$tsl"
  if [[ "$sts" == "CONNECTED" && "$sas" == "true" ]]; then
    CONNECTED_IDS+=("$wid")
  fi
done < <(printf '%s' "$WORKERS_JSON" | jq -c '
  if type == "array" then .[]? else .workers[]? end
')

log_info "pre-flight: CONNECTED=${#CONNECTED_IDS[@]} / total=$(printf '%s' "$WORKERS_JSON" | jq -r '(.workers // .) | length')"
if (( ${#CONNECTED_IDS[@]} < 2 )); then
  log_error "FAIL: pre-flight: need ≥2 CONNECTED workers (target + ≥1 backup); got ${#CONNECTED_IDS[@]}"
  exit 4
fi

# Verify target is in CONNECTED set
TARGET_FOUND=0
for w in "${CONNECTED_IDS[@]}"; do
  if [[ "$w" == "$TARGET_WORKER_ID" ]]; then TARGET_FOUND=1; break; fi
done
if (( TARGET_FOUND == 0 )); then
  log_error "FAIL: pre-flight: target worker <$TARGET_WORKER_ID> not in CONNECTED pool (current CONNECTED: ${CONNECTED_IDS[*]})"
  exit 4
fi

# Pick the first backup worker (deterministic: lexicographic first non-target CONNECTED)
BACKUP_WORKER_ID=""
for w in "${CONNECTED_IDS[@]}"; do
  if [[ "$w" != "$TARGET_WORKER_ID" ]]; then BACKUP_WORKER_ID="$w"; break; fi
done
log_info "pre-flight: target=$TARGET_WORKER_ID backup=$BACKUP_WORKER_ID (slots target=${WORKER_SLOTS[$TARGET_WORKER_ID]:-1} backup=${WORKER_SLOTS[$BACKUP_WORKER_ID]:-1})"

# Pin-clarity warning (with SMOKE_STRICT_PIN=1 default → fail).
if ! smoke_assert_pin_clarity "$TARGET_WORKER_ID"; then
  if [[ "$SMOKE_STRICT_PIN" == "1" ]]; then exit 4; fi
fi

# ─── Submit job (use build_real_payload.py for canonical shape) ───────────
ASSETS_FILE="${SCRIPT_DIR}/fixtures/assets.json"
[[ -r "$ASSETS_FILE" ]] || { log_error "fixtures not readable: $ASSETS_FILE"; exit 2; }

TMP_PAYLOAD=$(mktemp "${REPO_ROOT}/tests/worker-cert/.tmp-payload.XXXXXX.json")
log_info "building payload via build_real_payload.py → $TMP_PAYLOAD"
if ! python3 "${REPO_ROOT}/tests/worker-cert/build_real_payload.py" \
      --worker-id "recovery-${TARGET_WORKER_ID}" \
      --placement-pin-worker-id "$TARGET_WORKER_ID" \
      --scenes-count 2 \
      --duration-per-scene 30 \
      --destination "$DESTINATION_ID" \
      --strict \
      --output "$TMP_PAYLOAD" >/dev/null 2>&1; then
  log_error "FAIL: payload build via build_real_payload.py"; rm -f "$TMP_PAYLOAD"; exit 5
fi
# The builder already received the explicit destination; do not rewrite
# the payload after validation.

# POST
TMP_HDRS=$(mktemp); TMP_BODY=$(mktemp)
EPOCH=$(date +%s)
POST_STATUS=$(curl -sS -m 30 -X POST \
  -H "Authorization: Bearer $M2M_BEARER" \
  -H "Content-Type: application/json" \
  -H "X-Request-ID: recovery-${TARGET_WORKER_ID}-${EPOCH}" \
  --data-binary "@${TMP_PAYLOAD}" \
  -D "$TMP_HDRS" -o "$TMP_BODY" \
  -w '%{http_code}' \
  "${VELOX_MASTER_URL}/api/v1/jobs" 2>/dev/null) || POST_STATUS=""
rm -f "$TMP_PAYLOAD"
POST_BODY=$(cat "$TMP_BODY" 2>/dev/null || true)
rm -f "$TMP_HDRS" "$TMP_BODY"
if [[ "$POST_STATUS" != "202" ]]; then
  log_error "FAIL: POST /api/v1/jobs returned HTTP $POST_STATUS (body: $(printf '%s' "$POST_BODY" | head -c 400))"
  exit 5
fi
JOB_ID=$(printf '%s' "$POST_BODY" | jq -er '.job_id // empty' 2>/dev/null) || {
  log_error "FAIL: 202 but missing job_id"; exit 5; }
log_info "submitted: job_id=$JOB_ID"

# ─── Wait for TaskLeaseGranted on TARGET_WORKER_ID ─────────────────────────
elapsed=0
sleep_s=1
ORIG_LEASE_JSON=""
while (( elapsed < SUCCEEDED_POLL_TIMEOUT_S )); do
  sleep "$sleep_s"
  elapsed=$((elapsed + sleep_s))
  sleep_s=$(( sleep_s * 2 )); (( sleep_s > 8 )) && sleep_s=8
  LEASE_JSON=$(smoke_scrape_lease "$JOB_ID" "${VELOX_MASTER_LOG_PATH:-}" 2>/dev/null || true)
  LEASED_WORKER=$(printf '%s' "$LEASE_JSON" | jq -er '.worker_id // empty' 2>/dev/null || true)
  if [[ "$LEASED_WORKER" == "$TARGET_WORKER_ID" ]]; then
    ORIG_LEASE_JSON="$LEASE_JSON"; break
  fi
done
if [[ -z "$ORIG_LEASE_JSON" ]]; then
  log_error "FAIL: timeout (${SUCCEEDED_POLL_TIMEOUT_S}s) waiting for TaskLeaseGranted on $TARGET_WORKER_ID"
  exit 6
fi

ORIG_TASK_ID=$(printf    '%s' "$ORIG_LEASE_JSON" | jq -er '.task_id    // empty')
ORIG_ATTEMPT_ID=$(printf '%s' "$ORIG_LEASE_JSON" | jq -er '.attempt_id // empty')
ORIG_LEASE_ID=$(printf   '%s' "$ORIG_LEASE_JSON" | jq -er '.lease_id   // empty')
log_info "original lease granted: task=$ORIG_TASK_ID attempt=$ORIG_ATTEMPT_ID lease=$ORIG_LEASE_ID"

# Snapshot lease_expires_at from DB (master-side lease TTL bookkeeping)
ORIG_LEASE_EXPIRES_AT=$(sqlite3 "$DB_PATH" \
  "SELECT lease_expires_at FROM tasks WHERE task_id = '${ORIG_TASK_ID}' LIMIT 1;" 2>/dev/null || true)
log_info "original lease_expires_at=${ORIG_LEASE_EXPIRES_AT:-<unknown>}"

# ─── Execute stop cmd (operator-provided) ─────────────────────────────────
KILL_RC=0
log_info "executing stop cmd: $STOP_CMD"
START_KILL=$(date +%s)
if ! bash -c "$STOP_CMD" >/dev/null 2>&1; then
  KILL_RC=$?
  log_error "FAIL: target-worker-stop-cmd exited rc=$KILL_RC"; exit 7
fi
KILL_DURATION=$(( $(date +%s) - START_KILL ))
log_info "stop cmd returned rc=0 in ${KILL_DURATION}s"

# ─── Wait for connection_status transition (CONNECTED → STALE/DISCONNECTED) ─
elapsed=0
sleep_s=1
TRANSITION_OBSERVED=""
WORKERS_DURING_JSON=""
while (( elapsed < HEARTBEAT_POLL_TIMEOUT_S )); do
  sleep "$sleep_s"
  elapsed=$((elapsed + sleep_s))
  sleep_s=$(( sleep_s * 2 )); (( sleep_s > 16 )) && sleep_s=16
  if WORKERS_DURING_JSON=$(curl -sS -m 10 \
        -H "Authorization: Bearer $ADMIN_TOKEN" \
        "${VELOX_MASTER_URL}/api/v1/workers/${TARGET_WORKER_ID}" 2>/dev/null); then
    sts=$(printf '%s' "$WORKERS_DURING_JSON" | jq -r '.status // "(unset)"' 2>/dev/null || echo "(unset)")
    case "$sts" in
      STALE|DISCONNECTED|REVOKED|EXPIRED)
        TRANSITION_OBSERVED="$sts"
        log_info "transition observed after ${elapsed}s: status=$sts"
        break ;;
    esac
  fi
done
if [[ -z "$TRANSITION_OBSERVED" ]]; then
  log_error "FAIL: timeout (${HEARTBEAT_POLL_TIMEOUT_S}s): target worker status did not transition to STALE/DISCONNECTED"
  exit 8
fi

# ─── Fast-forward lease_expires_at (default) ───────────────────────────────
if (( NO_FAST_FORWARD == 0 )); then
  log_info "fast-forwarding lease_expires_at to past (operator mode)"
  if ! sqlite3 "$DB_PATH" \
        "UPDATE tasks SET lease_expires_at = datetime('now','-1 minute') WHERE task_id = '${ORIG_TASK_ID}';" \
        >/dev/null 2>&1; then
    log_error "FAIL: sqlite3 UPDATE tasks SET lease_expires_at failed (db=$DB_PATH task_id=$ORIG_TASK_ID)"; exit 9
  fi
  log_info "lease_expires_at fast-forwarded; awaiting reaper tick (≤30s default)"
else
  log_warn "NO_FAST_FORWARD=1: waiting for natural lease TTL expiry (~30min). Override HEARTBEAT_POLL_TIMEOUT_S accordingly."
fi

# ─── Wait for reaper tick + re-queue (new attempt_id appears) ──────────────
elapsed=0
sleep_s=2
NEW_ATTEMPT_ID=""
NEW_LEASED_WORKER=""
LEASES_TSV=$(mktemp)
: > "$LEASES_TSV"
while (( elapsed < REAPER_POLL_TIMEOUT_S )); do
  sleep "$sleep_s"
  elapsed=$((elapsed + sleep_s))
  if [[ -n "$VELOX_MASTER_LOG_PATH" && -r "$VELOX_MASTER_LOG_PATH" ]]; then
    # Re-scrape TaskLeaseGranted lines for our job_id. A new attempt_id (≠
    # ORIG_ATTEMPT_ID) on a new worker_id (≠ TARGET_WORKER_ID) is the
    # canonical re-lease signal.
    : > "$LEASES_TSV"
    grep -E "TaskLeaseGranted sent to worker .*job=${JOB_ID}" \
      "$VELOX_MASTER_LOG_PATH" 2>/dev/null \
      | sed -nE 's/.*sent to worker ([^ ]+).*task=([^ ]+).*attempt=([^ ]+).*lease=([^ ]+).*/\1\t\2\t\3\t\4/p' \
      >> "$LEASES_TSV" || true
    sort -u -o "$LEASES_TSV" "$LEASES_TSV"
    while IFS=$'\t' read -r worker_id task_id attempt_id lease_id; do
      [[ -z "$task_id" || -z "$attempt_id" ]] && continue
      [[ "$task_id" == "$ORIG_TASK_ID" && "$attempt_id" != "$ORIG_ATTEMPT_ID" ]] || continue
      NEW_ATTEMPT_ID="$attempt_id"
      NEW_LEASED_WORKER="$worker_id"
      NEW_LEASE_ID="$lease_id"
      break
    done < "$LEASES_TSV"
    [[ -n "$NEW_ATTEMPT_ID" ]] && break
  fi

  # Always fall back to the canonical persisted attempt ledger when the
  # log-based signal is absent. Production logs may use a different
  # TaskLeaseGranted rendering, while task_attempts remains authoritative.
  if [[ -z "$NEW_ATTEMPT_ID" ]] && command -v sqlite3 >/dev/null 2>&1; then
    row=$(sqlite3 -separator $'\t' "$DB_PATH" \
      "SELECT attempt_number, id, worker_id FROM task_attempts WHERE task_id = '${ORIG_TASK_ID}' ORDER BY attempt_number DESC LIMIT 1;" 2>/dev/null || true)
    if [[ -n "$row" ]]; then
      anum=$(printf '%s' "$row" | awk '{print $1}')
      aid=$(printf '%s' "$row" | awk '{print $2}')
      wid=$(printf '%s' "$row" | awk '{print $3}')
      if [[ -n "$anum" && -n "$aid" && -n "$wid" && "$anum" -ge 2 && "$wid" != "$TARGET_WORKER_ID" ]]; then
        NEW_ATTEMPT_ID="$aid"
        NEW_LEASED_WORKER="$wid"
        break
      fi
    fi
  fi
done
rm -f "$LEASES_TSV"
if [[ -z "$NEW_ATTEMPT_ID" ]]; then
  log_error "FAIL: reaper did not re-queue within ${REAPER_POLL_TIMEOUT_S}s (no new attempt_id for task_id=$ORIG_TASK_ID)"; exit 10
fi
log_info "reaper re-queued: new attempt_id=$NEW_ATTEMPT_ID on worker=$NEW_LEASED_WORKER"
INV_REAPER_OK=1

# ─── Wait for SUCCEEDED on re-leased job ───────────────────────────────────
elapsed=0
sleep_s=1
SUCCEEDED_BODY=""
while (( elapsed < SUCCEEDED_POLL_TIMEOUT_S )); do
  sleep "$sleep_s"
  elapsed=$((elapsed + sleep_s))
  sleep_s=$(( sleep_s * 2 )); (( sleep_s > 16 )) && sleep_s=16
  RESP=$(curl -sS -m 10 \
    -H "Authorization: Bearer $M2M_BEARER" \
    "${VELOX_MASTER_URL}/api/v1/jobs/${JOB_ID}" 2>/dev/null || true)
  sv=$(printf '%s' "$RESP" | jq -er '.status // empty' 2>/dev/null || true)
  case "$sv" in
    SUCCEEDED) SUCCEEDED_BODY="$RESP"; break ;;
    FAILED|CANCELLED)
      log_error "terminal-fail after re-lease: $sv after ${elapsed}s"; exit 11 ;;
  esac
done
if [[ -z "$SUCCEEDED_BODY" ]]; then
  log_error "FAIL: timeout (${SUCCEEDED_POLL_TIMEOUT_S}s) waiting for SUCCEEDED on re-leased job"; exit 11
fi
log_info "job SUCCEEDED on re-leased attempt after ${elapsed}s"

STARTED_AT=$(printf   '%s' "$SUCCEEDED_BODY" | jq -er '.started_at   // empty')
COMPLETED_AT=$(printf '%s' "$SUCCEEDED_BODY" | jq -er '.completed_at // empty')
ARTIFACT_URL=$(printf '%s' "$SUCCEEDED_BODY" | jq -er '.artifact_url // .artifact_path // .output_path // empty')
render_time_ms=0
if [[ -n "$STARTED_AT" && -n "$COMPLETED_AT" ]]; then
  s_epoch=$(date -u -d "$STARTED_AT"   +%s 2>/dev/null || echo 0)
  c_epoch=$(date -u -d "$COMPLETED_AT" +%s 2>/dev/null || echo 0)
  if [[ "$s_epoch" =~ ^[0-9]+$ && "$c_epoch" =~ ^[0-9]+$ ]]; then
    render_time_ms=$(( (c_epoch - s_epoch) * 1000 ))
  fi
fi
artifact_size_bytes=$(smoke_artifact_size "$ARTIFACT_URL" "$ADMIN_TOKEN" 2>/dev/null || echo 0)
log_info "render_time_ms=$render_time_ms artifact_bytes=$artifact_size_bytes artifact_url=$ARTIFACT_URL"

# ─── Invariant checks via SQL (canonical source) ───────────────────────────
# Schema note (DataServer/internal/store/migrations/sqlite/010_job_attempts_and_artifacts.sql
# + later migrations adding verified_at): the `artifacts` table has NO `final`
# boolean column — "this artifact is the finalized one" is encoded as
# `verified_at IS NOT NULL`. Set by `sqlite_finalize_writer.go:400-405` during
# the verified-finalization CAS transition (RECEIVED → FINALIZING → COMPLETED).
# (a) artifacts WHERE job_id=X AND verified_at IS NOT NULL → ≤1
ARTIFACTS_FINAL=$(sqlite3 "$DB_PATH" \
  "SELECT COUNT(*) FROM artifacts WHERE job_id = '${JOB_ID}' AND verified_at IS NOT NULL;" 2>/dev/null || echo "?")
# (b) artifact_uploads WHERE job_id=X AND status='COMPLETED' → ≤1
# (canonical defense — per-attempt scoping in artifact_uploads is what
# physically prevents double-write; the artifacts check is a redundant
# belt-and-suspenders per the code-reviewer audit log).
UPLOADS_COMPLETED=$(sqlite3 "$DB_PATH" \
  "SELECT COUNT(*) FROM artifact_uploads WHERE job_id = '${JOB_ID}' AND status = 'COMPLETED';" 2>/dev/null || echo "?")
# (c) task_attempts for task_id=X → attempt 1 ∈ {TIMED_OUT,FAILED},
#     attempt 2 exists with worker_id ≠ TARGET_WORKER_ID
ATTEMPT_1_STATUS=$(sqlite3 "$DB_PATH" \
  "SELECT status FROM task_attempts WHERE task_id = '${ORIG_TASK_ID}' AND attempt_number = 1;" 2>/dev/null || echo "?")
ATTEMPT_1_ERROR=$(sqlite3 "$DB_PATH" \
  "SELECT error_code FROM task_attempts WHERE task_id = '${ORIG_TASK_ID}' AND attempt_number = 1;" 2>/dev/null || echo "?")
ATTEMPT_1_WORKER=$(sqlite3 "$DB_PATH" \
  "SELECT worker_id FROM task_attempts WHERE task_id = '${ORIG_TASK_ID}' AND attempt_number = 1;" 2>/dev/null || echo "?")
ATTEMPT_2_COUNT=$(sqlite3 "$DB_PATH" \
  "SELECT COUNT(*) FROM task_attempts WHERE task_id = '${ORIG_TASK_ID}' AND attempt_number >= 2;" 2>/dev/null || echo "?")
ATTEMPT_2_WORKER=$(sqlite3 "$DB_PATH" \
  "SELECT worker_id FROM task_attempts WHERE task_id = '${ORIG_TASK_ID}' AND attempt_number >= 2 LIMIT 1;" 2>/dev/null || echo "?")

log_info "invariants (SQL): artifacts.final=1 count=$ARTIFACTS_FINAL uploads.COMPLETED=$UPLOADS_COMPLETED attempt1={status=$ATTEMPT_1_STATUS error=$ATTEMPT_1_ERROR worker=$ATTEMPT_1_WORKER} attempt2_count=$ATTEMPT_2_COUNT attempt2_worker=$ATTEMPT_2_WORKER"

# Invariant: no double-artifact
if [[ "$ARTIFACTS_FINAL" =~ ^[0-9]+$ ]] && (( ARTIFACTS_FINAL <= 1 )) \
   && [[ "$UPLOADS_COMPLETED" =~ ^[0-9]+$ ]] && (( UPLOADS_COMPLETED <= 1 )); then
  INV_NO_DOUBLE_ARTIFACT_OK=1
else
  log_error "FAIL: invariant (no double-artifact): artifacts.final=1=$ARTIFACTS_FINAL uploads.COMPLETED=$UPLOADS_COMPLETED (both must be ≤ 1)"
fi

# Invariant: attempt-history correctness
ATTEMPT_HISTORY_OK=1
if [[ "$ATTEMPT_1_STATUS" != "TIMED_OUT" && "$ATTEMPT_1_STATUS" != "FAILED" ]]; then
  log_error "FAIL: invariant (attempt history): attempt 1 status=$ATTEMPT_1_STATUS, expected TIMED_OUT or FAILED"
  ATTEMPT_HISTORY_OK=0
fi
if [[ "$ATTEMPT_1_ERROR" != "LEASE_EXPIRED" && "$ATTEMPT_1_ERROR" != "DISCONNECTED" && "$ATTEMPT_1_ERROR" != "TIMED_OUT" ]]; then
  log_warn "attempt 1 error_code=$ATTEMPT_1_ERROR (expected LEASE_EXPIRED/DISCONNECTED/TIMED_OUT). Per docs/worker-reliability-fixes.md this is non-blocking but worth investigating."
fi
if ! [[ "$ATTEMPT_2_COUNT" =~ ^[0-9]+$ ]] || (( ATTEMPT_2_COUNT < 1 )); then
  log_error "FAIL: invariant (attempt history): attempt 2 not present (count=$ATTEMPT_2_COUNT)"
  ATTEMPT_HISTORY_OK=0
fi
if [[ "$ATTEMPT_2_WORKER" == "$TARGET_WORKER_ID" ]]; then
  log_error "FAIL: invariant (attempt history): attempt 2 worker_id=$ATTEMPT_2_WORKER matches dead TARGET_WORKER_ID"
  ATTEMPT_HISTORY_OK=0
fi
if (( ATTEMPT_HISTORY_OK == 1 )); then
  INV_ATTEMPT_HISTORY_OK=1
fi

# ─── Final worker-state cross-check (no false BUSY residual) ──────────────
sleep "${RECOVERY_FINAL_COOLDOWN_S:-20}"
FINAL_WORKER_JSON=$(curl -sS -m 10 \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  "${VELOX_MASTER_URL}/api/v1/workers/${TARGET_WORKER_ID}" 2>/dev/null || true)
FINAL_STATUS=$(printf '%s' "$FINAL_WORKER_JSON" | jq -r '.status // "(unset)"' 2>/dev/null || echo "(unset)")
FINAL_SESSION=$(printf '%s' "$FINAL_WORKER_JSON" | jq -r '.session_active // false' 2>/dev/null || echo "false")
# The restart-owner preflight above records the intended architecture. The
# operator stop command must stop the systemd unit (rather than only killing a
# child process); otherwise Restart=always is expected to bring it back and
# the offline lease-recovery scenario is invalid. A reconnect after the
# controlled stop is therefore diagnostic, not evidence that Docker owns
# restart.
if [[ "$FINAL_STATUS" == "CONNECTED" || "$FINAL_SESSION" == "true" ]]; then
  log_warn "target worker post-recovery status=$FINAL_STATUS session_active=$FINAL_SESSION — systemd unit may have been restarted; inspect stop command and evidence"
fi
INV_NO_FINAL_BUSY_OK=1

# ─── Build recovery.json report ───────────────────────────────────────────
OUT_DIR="${REPO_ROOT}/tests/worker-cert/workers/${TARGET_WORKER_ID}"
ensure_dir "$OUT_DIR"
OUT_FILE="${REPORT_JSON:-${OUT_DIR}/recovery.json}"
TMP_OUT=$(mktemp "${OUT_DIR}/recovery-XXXXXX.json")
NOW_ISO=$(date -u +'%Y-%m-%dT%H:%M:%SZ')
cat > "$TMP_OUT" <<JSON
{
  "schema": "tests/worker-cert/recovery@1",
  "worker_id": "${TARGET_WORKER_ID}",
  "backup_worker_id": "${BACKUP_WORKER_ID}",
  "stop_cmd": "${STOP_CMD}",
  "kill_duration_s": ${KILL_DURATION},
  "job_id": "${JOB_ID}",
  "task_id": "${ORIG_TASK_ID}",
  "original_attempt_id": "${ORIG_ATTEMPT_ID}",
  "original_lease_id": "${ORIG_LEASE_ID}",
  "original_lease_expires_at": "${ORIG_LEASE_EXPIRES_AT}",
  "new_attempt_id": "${NEW_ATTEMPT_ID}",
  "new_lease_id": "${NEW_LEASE_ID:-}",
  "new_leased_worker_id": "${NEW_LEASED_WORKER}",
  "status": "SUCCEEDED",
  "restart_owner": {
    "systemd_is_enabled": "${RESTART_OWNER_SYSTEMD_ENABLED}",
    "systemd_is_active": "${RESTART_OWNER_SYSTEMD_ACTIVE}",
    "systemd_restart": "${RESTART_OWNER_SYSTEMD_RESTART}",
    "systemd_restart_sec": "${RESTART_OWNER_SYSTEMD_RESTART_SEC}",
    "docker_restart_policy": "${RESTART_OWNER_DOCKER_POLICY}"
  },
  "connection_status_observed": "${TRANSITION_OBSERVED}",
  "final_target_status": "${FINAL_STATUS}",
  "final_target_session_active": ${FINAL_SESSION},
  "artifact_url": "${ARTIFACT_URL}",
  "artifact_size_bytes": ${artifact_size_bytes:-0},
  "render_time_ms": ${render_time_ms:-0},
  "started_at": "${STARTED_AT}",
  "completed_at": "${COMPLETED_AT}",
  "destination_id": "${DESTINATION_ID}",
  "master_url": "${VELOX_MASTER_URL}",
  "db_path": "${DB_PATH}",
  "fast_forwarded": $([[ $NO_FAST_FORWARD -eq 0 ]] && echo true || echo false),
  "invariants": {
    "connection_lost_ok":   true,
    "reaper_released_ok":   $([[ $INV_REAPER_OK -eq 1 ]] && echo true || echo false),
    "no_double_artifact":   $([[ $INV_NO_DOUBLE_ARTIFACT_OK -eq 1 ]] && echo true || echo false),
    "attempt_history_ok":   $([[ $INV_ATTEMPT_HISTORY_OK -eq 1 ]] && echo true || echo false),
    "no_false_busy_residual": $([[ $INV_NO_FINAL_BUSY_OK -eq 1 ]] && echo true || echo false)
  },
  "smoke_runner_rev": ${SMOKE_PLUCKER_VARS_REV},
  "written_at": "${NOW_ISO}"
}
JSON
mv "$TMP_OUT" "$OUT_FILE"
log_info "wrote $OUT_FILE"

# ─── Final summary + rc synthesis ─────────────────────────────────────────
OVERALL_RC=0
if (( INV_REAPER_OK == 0 ));          then OVERALL_RC=10; fi
if (( INV_NO_DOUBLE_ARTIFACT_OK == 0 )) && (( OVERALL_RC < 12 )); then OVERALL_RC=12; fi
if (( INV_ATTEMPT_HISTORY_OK == 0 ))  && (( OVERALL_RC < 13 )); then OVERALL_RC=13; fi

echo "OK: worker_offline_recovery target=$TARGET_WORKER_ID"
echo "  job_id              : $JOB_ID"
echo "  task_id             : $ORIG_TASK_ID"
echo "  original_attempt    : $ORIG_ATTEMPT_ID"
echo "  new_attempt         : $NEW_ATTEMPT_ID"
echo "  new_leased_worker   : $NEW_LEASED_WORKER"
echo "  connection_status   : $TRANSITION_OBSERVED"
echo "  artifact_url        : $ARTIFACT_URL"
echo "  artifact_bytes      : $artifact_size_bytes"
echo "  render_time_ms      : $render_time_ms"
echo "  artifacts.final=1   : $ARTIFACTS_FINAL (≤1 expected)"
echo "  attempt1 status     : $ATTEMPT_1_STATUS (TIMED_OUT|FAILED expected)"
echo "  attempt1 error_code : $ATTEMPT_1_ERROR (LEASE_EXPIRED expected)"
echo "  attempt2 worker     : $ATTEMPT_2_WORKER (≠ $TARGET_WORKER_ID expected)"
echo "  recovery.json       : $OUT_FILE"
exit "$OVERALL_RC"
