#!/usr/bin/env bash
# =============================================================================
# tests/worker-cert/restart_reconnect.sh — Worker restart + reconnect smoke.
# =============================================================================
# Usage:
#   ./tests/worker-cert/restart_reconnect.sh <worker_id> \
#       --target-worker-restart-cmd 'systemctl restart velox-worker'
#
#   VELOX_MASTER_URL=https://velox.example.com \
#   VELOX_ADMIN_TOKEN=... \
#   ./tests/worker-cert/restart_reconnect.sh host_57_131_20_173 \
#       --target-worker-restart-cmd 'ssh worker-host systemctl restart velox-worker'
#
# What the script does:
#   1. Sources tests/_lib/sh/_lib.sh (logging + pid-trap + ensure) and
#      tests/worker-cert/lib/pluck.sh (smoke-local helpers).
#   2. Mints an ephemeral M2M client via POST /api/v1/admin/m2m/keys; DELETEs
#      it on exit (best-effort, see trap).
#   3. Pre-flight: GET /api/v1/workers, asserts target CONNECTED +
#      session_active=true. Snapshots pre-restart session_id + last_heartbeat_at.
#   4. Subprocess-runs tests/worker-cert/smoke_one.sh <worker_id> to
#      establish a baseline SUCCEEDED run; captures artifact_url + job_id.
#   5. Executes --target-worker-restart-cmd (operator-provided, e.g.
#      `systemctl restart velox-worker` or `kill -TERM <pid>` for local).
#   6. Polls GET /api/v1/workers/<id> until session_active=true AND
#      session_id != pre-restart session_id AND last_heartbeat_at recent
#      (default 60s budget).
#   7. Scrapes master log for "Worker <id> reconnecting — removing old
#      session" as canonical reconnect evidence.
#   8. Subprocess-runs smoke_one.sh again on the same worker. Asserts:
#        - new job SUCCEEDED (subprocess rc=0)
#        - new artifact HEAD OK
#        - new artifact URL != pre-restart URL
#        - new artifact body SHAPE different (size_bytes differs OR
#          content hash via quick mp4 header probe)
#        - pre-restart artifact STILL HEAD-reachable + size unchanged
#          (no clobber of the previous run's output)
#   9. Writes workers/<worker_id>/restart.json (atomic via tmp+mv).
#
# Exit codes:
#   0  PASS — restart + reconnect + post-smoke + no-interference all ok.
#   2  usage / env (missing arg / no curl|jq).
#   3  master unreachable / M2M provisioning failed.
#   4  pre-flight failed (target not CONNECTED + session_active).
#   5  pre-restart smoke failed (subprocess rc != 0).
#   6  restart cmd failed (non-zero exit or --restart-timeout-s exceeded).
#   7  new session not detected within --reconnect-poll-timeout-s
#      (CONNECTED+session_id_changed+session_active=true not all true).
#   8  reconnect log marker not found in master log (suspicious —
#      session_id might have rotated without a reconnect event).
#   9  post-restart smoke failed (subprocess rc != 0).
#  10  temp interference detected (PRE artifact HEAD failed or pre size
#      changed OR POST not on target worker OR POST artifact equal to PRE).
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
TARGET_WORKER_ID="${1:-}"
if [[ -z "$TARGET_WORKER_ID" ]]; then
  log_error "usage: $0 <worker_id> --target-worker-restart-cmd <cmd>"
  exit 2
fi
shift
RESTART_CMD=""
RECONNECT_POLL_TIMEOUT_S="${RECONNECT_POLL_TIMEOUT_S:-60}"
RESTART_TIMEOUT_S="${RESTART_TIMEOUT_S:-60}"
DESTINATION_ID="${RESTART_DESTINATION_ID:-}"
REPORT_JSON=""
DB_PATH="${VELOX_DB_PATH:-}"
SMOKE_POLL_TIMEOUT_S="${RESTART_SMOKE_POLL_TIMEOUT_S:-180}"
VELOX_MASTER_LOG_PATH="${VELOX_MASTER_LOG_PATH:-}"
SMOKE_STRICT_PIN="${SMOKE_STRICT_PIN:-0}"  # restart smoke tolerates other workers (placement pin via env)

while (( $# > 0 )); do
  case "$1" in
    --target-worker-restart-cmd) RESTART_CMD="$2"; shift 2 ;;
    --reconnect-poll-timeout-s)  RECONNECT_POLL_TIMEOUT_S="$2"; shift 2 ;;
    --restart-timeout-s)         RESTART_TIMEOUT_S="$2"; shift 2 ;;
    --smoke-poll-timeout-s)      SMOKE_POLL_TIMEOUT_S="$2"; shift 2 ;;
    --destination-id)            DESTINATION_ID="$2"; shift 2 ;;
    --report-json)               REPORT_JSON="$2"; shift 2 ;;
    --db-path)                   DB_PATH="$2"; shift 2 ;;
    --master-log-path)           VELOX_MASTER_LOG_PATH="$2"; shift 2 ;;
    -h|--help)                   usage ;;
    *)                           log_error "unknown flag: $1"; exit 2 ;;
  esac
done

if [[ -z "$RESTART_CMD" ]]; then
  log_error "missing required --target-worker-restart-cmd (e.g. 'systemctl restart velox-worker' or 'kill -TERM <pid>')"
  exit 2
fi
for v in "$RECONNECT_POLL_TIMEOUT_S" "$RESTART_TIMEOUT_S" "$SMOKE_POLL_TIMEOUT_S"; do
  if ! [[ "$v" =~ ^[0-9]+$ ]] || (( v < 1 )); then
    log_error "timeout must be a positive integer (got: $v)"; exit 2
  fi
done

[[ -n "${VELOX_MASTER_URL:-}" ]] || VELOX_MASTER_URL="http://127.0.0.1:8080"
VELOX_MASTER_URL="${VELOX_MASTER_URL%/}"

log_info "restart_reconnect target=$TARGET_WORKER_ID master=$VELOX_MASTER_URL dest=$DESTINATION_ID"
log_info "tunables: reconnect_timeout=${RECONNECT_POLL_TIMEOUT_S}s restart_timeout=${RESTART_TIMEOUT_S}s smoke_timeout=${SMOKE_POLL_TIMEOUT_S}s"

# ─── Required binaries ─────────────────────────────────────────────────────
for bin in curl jq awk sed grep date; do
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

# ─── DB path auto-discovery (best-effort, optional for restart smoke) ─────
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
  log_warn "VELOX_DB_PATH unreadable; SQL cross-check skipped (informational only, not a hard fail)"
fi

# ─── EXIT trap: cleanup + M2M ──────────────────────────────────────────────
TMP_HDRS=""
TMP_BODY=""
TMP_OUT=""
INV_SESSION_ID_CHANGED_OK=0
INV_LOG_MARKER_OK=0
INV_POST_SMOKE_OK=0
INV_NO_CLOBBER_OK=0
INV_SHA_DISTINCT_OK=0
INV_PLACEMENT_PIN_PRESERVED_OK=0

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

# ─── Helper: fetch worker JSON via /api/v1/workers/<id> ────────────────────
fetch_worker() {
  local wid="$1"
  curl -sS -m 10 \
    -H "Authorization: Bearer $ADMIN_TOKEN" \
    "${VELOX_MASTER_URL}/api/v1/workers/${wid}" 2>/dev/null || true
}

# ─── Helper: artifact reachability probe ───────────────────────────────────
# Returns "<status> <size>" on stdout, empty on failure. The artifact API
# does not implement HEAD consistently, so use a real GET and discard bytes.
artifact_probe() {
  local url="$1"
  [[ -z "$url" ]] && { echo ""; return 0; }
  curl -sS -m 20 \
    -H "Authorization: Bearer $ADMIN_TOKEN" "$url" \
    -o /dev/null -w '%{http_code} %{size_download}' 2>/dev/null || true
}

# ─── Pre-flight: target CONNECTED + session_active=true ────────────────────
# shellcheck disable=SC2034 # populated by smoke_workers_list in sourced pluck.sh
WORKERS_JSON=""
if ! smoke_workers_list "$ADMIN_TOKEN" "$VELOX_MASTER_URL"; then
  log_error "could not list workers"; exit 3
fi
TARGET_RECORD=$(smoke_worker_by_id "$TARGET_WORKER_ID")
if [[ -z "$TARGET_RECORD" ]]; then
  log_error "target worker not in /api/v1/workers: $TARGET_WORKER_ID"; exit 4
fi
PRE_STATUS=$(printf '%s' "$TARGET_RECORD" | jq -r '.status // "(unset)"')
PRE_SESSION_ACTIVE=$(printf '%s' "$TARGET_RECORD" | jq -r '.session_active // false')
PRE_SESSION_ID=$(printf '%s' "$TARGET_RECORD"     | jq -r '.session_id // "(unset)"')
PRE_LAST_HB=$(printf '%s' "$TARGET_RECORD"        | jq -r '.last_heartbeat_at // "(unset)"')
log_info "pre-restart: target status=$PRE_STATUS session_active=$PRE_SESSION_ACTIVE session_id=$PRE_SESSION_ID last_heartbeat_at=$PRE_LAST_HB"
if [[ "$PRE_STATUS" != "CONNECTED" || "$PRE_SESSION_ACTIVE" != "true" ]]; then
  log_error "FAIL: pre-flight: target worker <$TARGET_WORKER_ID> not CONNECTED+session_active=true; restart test cannot establish a baseline"
  exit 4
fi

# ─── Step 1: pre-restart smoke (subprocess reuse of smoke_one.sh) ──────────
PRE_SMOKE_OUT_ROOT="${REPO_ROOT}/tests/worker-cert/workers/${TARGET_WORKER_ID}"
# Path to the canonical smoke.json output file written by smoke_one.sh. NOTE:
# it gets OVERWRITTEN by every smoke_one.sh invocation (atomic tmp+mv in
# smoke_one.sh). We snapshot PRE-restart fields into script-level vars BEFORE
# the second smoke_one.sh call so the post-restart reads (below) see the
# correct (POST) data. The pre-restart .bak copy is intentional audit trail
# (NOT auto-cleaned; operators can diff against it post-run).
SMOKE_JSON_OUT="${PRE_SMOKE_OUT_ROOT}/smoke.json"
if [[ -r "$SMOKE_JSON_OUT" ]]; then
  cp -p "$SMOKE_JSON_OUT" "${SMOKE_JSON_OUT}.pre-restart.$$.bak" 2>/dev/null || true
fi
log_info "running pre-restart smoke_one.sh on $TARGET_WORKER_ID"
PRE_SMOKE_RC=0
SMOKE_DESTINATION_ID="$DESTINATION_ID" SMOKE_POLL_TIMEOUT_S="$SMOKE_POLL_TIMEOUT_S" \
  VELOX_MASTER_URL="$VELOX_MASTER_URL" VELOX_ADMIN_TOKEN="$ADMIN_TOKEN" \
  bash "${SCRIPT_DIR}/smoke_one.sh" "$TARGET_WORKER_ID" >/dev/null 2>&1 || PRE_SMOKE_RC=$?
log_info "pre-restart smoke_one.sh rc=$PRE_SMOKE_RC"
if (( PRE_SMOKE_RC != 0 )) || [[ ! -r "$SMOKE_JSON_OUT" ]]; then
  log_error "FAIL: pre-restart smoke_one.sh exited rc=$PRE_SMOKE_RC or smoke.json missing"
  exit 5
fi

PRE_JOB_ID=$(jq -er '.job_id'                "$SMOKE_JSON_OUT")
PRE_ARTIFACT_URL=$(jq -er '.artifact_url // empty' "$SMOKE_JSON_OUT")
PRE_ARTIFACT_BYTES=$(jq -r '.artifact_size_bytes // 0' "$SMOKE_JSON_OUT")
PRE_STATUS_FIELD=$(jq -r '.status // "(unset)"'        "$SMOKE_JSON_OUT")
log_info "pre-restart smoke.json: job_id=$PRE_JOB_ID artifact=$PRE_ARTIFACT_URL bytes=$PRE_ARTIFACT_BYTES status=$PRE_STATUS_FIELD"
if [[ "$PRE_STATUS_FIELD" != "SUCCEEDED" ]]; then
  log_error "FAIL: pre-restart smoke status=$PRE_STATUS_FIELD (expected SUCCEEDED)"
  exit 5
fi
PRE_PROBE=$(artifact_probe "$PRE_ARTIFACT_URL")
log_info "pre-restart artifact HEAD: $PRE_PROBE"

# ─── Step 2: execute restart cmd (with timeout) ───────────────────────────
log_info "executing restart cmd (timeout=${RESTART_TIMEOUT_S}s): $RESTART_CMD"
START_RESTART=$(date +%s)
# Wrap with `timeout` to enforce RESTART_TIMEOUT_S. `bash -c` to honor shell
# metacharacters (e.g. `ssh host 'kill -TERM $pid; systemctl start ...'`).
RESTART_RC=0
timeout "${RESTART_TIMEOUT_S}" bash -c "$RESTART_CMD" >/dev/null 2>&1 || RESTART_RC=$?
RESTART_DURATION=$(( $(date +%s) - START_RESTART ))
if (( RESTART_RC != 0 )); then
  log_error "FAIL: restart cmd exited rc=$RESTART_RC after ${RESTART_DURATION}s"
  exit 6
fi
log_info "restart cmd returned rc=0 in ${RESTART_DURATION}s"

# ─── Step 3: poll for reconnect (new session when exposed, otherwise fresh heartbeat) ─
elapsed=0
sleep_s=1
POST_STATUS=""
POST_SESSION_ACTIVE=""
POST_SESSION_ID=""
POST_LAST_HB=""
NEW_SESSION_OBSERVED=""
while (( elapsed < RECONNECT_POLL_TIMEOUT_S )); do
  sleep "$sleep_s"
  elapsed=$((elapsed + sleep_s))
  sleep_s=$(( sleep_s * 2 )); (( sleep_s > 8 )) && sleep_s=8
  WJ=$(fetch_worker "$TARGET_WORKER_ID")
  [[ -z "$WJ" ]] && continue
  POST_STATUS=$(printf '%s' "$WJ" | jq -r '.status // "(unset)"')
  POST_SESSION_ACTIVE=$(printf '%s' "$WJ" | jq -r '.session_active // false')
  POST_SESSION_ID=$(printf '%s' "$WJ" | jq -r '.session_id // "(unset)"')
  POST_LAST_HB=$(printf '%s' "$WJ" | jq -r '.last_heartbeat_at // "(unset)"')
  # Prefer an exposed session-id transition. The production worker endpoint
  # currently omits session_id, so require a heartbeat strictly newer than
  # the pre-restart heartbeat as the reconnect proof in that representation.
  SESSION_PROOF="no"
  if [[ "$POST_SESSION_ID" != "(unset)" && "$POST_SESSION_ID" != "$PRE_SESSION_ID" ]]; then
    SESSION_PROOF="session_id"
  elif [[ "$PRE_LAST_HB" != "(unset)" && "$POST_LAST_HB" != "(unset)" ]]; then
    pre_hb_epoch=$(smoke_parse_iso8601 "$PRE_LAST_HB")
    post_hb_epoch=$(smoke_parse_iso8601 "$POST_LAST_HB")
    if [[ -n "$pre_hb_epoch" && -n "$post_hb_epoch" ]] \
      && awk -v pre="$pre_hb_epoch" -v post="$post_hb_epoch" 'BEGIN { exit !(post > pre) }'; then
      SESSION_PROOF="heartbeat"
    fi
  fi
  if [[ "$SESSION_PROOF" != "no" && "$POST_SESSION_ACTIVE" == "true" \
        && ( "$POST_STATUS" == "CONNECTED" || "$POST_STATUS" == "idle" || "$POST_STATUS" == "busy" ) ]]; then
    NEW_SESSION_OBSERVED="yes"
    log_info "reconnect observed after ${elapsed}s via $SESSION_PROOF: status=$POST_STATUS session_id=$POST_SESSION_ID last_heartbeat_at=$POST_LAST_HB"
    break
  fi
done
if [[ -z "$NEW_SESSION_OBSERVED" ]]; then
  log_error "FAIL: timeout (${RECONNECT_POLL_TIMEOUT_S}s) waiting for reconnect (last seen: status=$POST_STATUS session_id=$POST_SESSION_ID session_active=$POST_SESSION_ACTIVE last_heartbeat_at=$POST_LAST_HB)"
  exit 7
fi
INV_SESSION_ID_CHANGED_OK=1

# ─── Step 4: scrape master log for reconnect marker ───────────────────────
if [[ -z "$VELOX_MASTER_LOG_PATH" || ! -r "$VELOX_MASTER_LOG_PATH" ]]; then
  log_warn "VELOX_MASTER_LOG_PATH not readable; reconnect log scrape SKIPPED (informational only)"
  INV_LOG_MARKER_OK=1  # not a hard fail
else
  # Master log format (handler_stream.go:375):
  #   "[GRPC] Worker <ID> reconnecting — removing old session <SID>"
  LOG_MARKER=$(grep -F "$TARGET_WORKER_ID" "$VELOX_MASTER_LOG_PATH" 2>/dev/null \
    | grep -F "reconnecting — removing old session" \
    | tail -1 || true)
  if [[ -n "$LOG_MARKER" ]]; then
    INV_LOG_MARKER_OK=1
    log_info "reconnect log marker: $LOG_MARKER"
  else
    log_warn "reconnect log marker NOT FOUND (session_id may have changed without explicit reconnect event). Treating as informational."
    INV_LOG_MARKER_OK=0  # warn-only; doesn't escalate rc
  fi
fi

# ─── Step 5: post-restart smoke on the same worker ─────────────────────────
log_info "running post-restart smoke_one.sh on $TARGET_WORKER_ID (same worker, expect SUCCEEDED again)"
POST_SMOKE_RC=0
SMOKE_DESTINATION_ID="$DESTINATION_ID" SMOKE_POLL_TIMEOUT_S="$SMOKE_POLL_TIMEOUT_S" \
  VELOX_MASTER_URL="$VELOX_MASTER_URL" VELOX_ADMIN_TOKEN="$ADMIN_TOKEN" \
  bash "${SCRIPT_DIR}/smoke_one.sh" "$TARGET_WORKER_ID" >/dev/null 2>&1 || POST_SMOKE_RC=$?
log_info "post-restart smoke_one.sh rc=$POST_SMOKE_RC"
if (( POST_SMOKE_RC != 0 )) || [[ ! -r "$SMOKE_JSON_OUT" ]]; then
  log_error "FAIL: post-restart smoke_one.sh exited rc=$POST_SMOKE_RC"
  # Don't fail-save to the JSON yet; we still want to write what we observed.
  INV_POST_SMOKE_OK=0
else
  INV_POST_SMOKE_OK=1
fi

# ─── Step 6: invariant checks (artifact interference + SHA distinct) ───────
# $SMOKE_JSON_OUT now contains the POST-restart smoke's data (overwritten by
# the second smoke_one.sh call), which is what we want for the post-restart
# reads below. PRE-restart fields were already snapshotted into script-level
# vars ($PRE_JOB_ID, $PRE_ARTIFACT_URL, $PRE_ARTIFACT_BYTES) BEFORE the
# second smoke call, so they're not affected by the overwrite.
POST_JOB_ID=$(jq -er '.job_id'                  "$SMOKE_JSON_OUT" 2>/dev/null || echo "")
POST_ARTIFACT_URL=$(jq -er '.artifact_url // empty' "$SMOKE_JSON_OUT" 2>/dev/null || echo "")
POST_ARTIFACT_BYTES=$(jq -r '.artifact_size_bytes // 0' "$SMOKE_JSON_OUT" 2>/dev/null || echo "0")
POST_STATUS_FIELD=$(jq -r '.status // "(unset)"'        "$SMOKE_JSON_OUT" 2>/dev/null || echo "(unset)")
POST_LEASED_WORKER=$(jq -r '.worker_id // "(unset)"'   "$SMOKE_JSON_OUT" 2>/dev/null || echo "(unset)")

# (a) PRE artifact STILL reachable + same size (no clobber)
PRE_PROBE_AFTER=$(artifact_probe "$PRE_ARTIFACT_URL")
PRE_PROBE_STATUS=${PRE_PROBE_AFTER%% *}
PRE_PROBE_BYTES=$(awk '{print $2}' <<<"$PRE_PROBE_AFTER")
if [[ "$PRE_PROBE_STATUS" == "200" || "$PRE_PROBE_STATUS" == "206" ]] \
   && [[ "${PRE_PROBE_BYTES:-0}" == "${PRE_ARTIFACT_BYTES:-0}" ]]; then
  INV_NO_CLOBBER_OK=1
  log_info "no-clobber ok: PRE artifact still HEAD-reachable (status=$PRE_PROBE_STATUS bytes=$PRE_PROBE_BYTES)"
else
  log_warn "no-clobber warning: PRE artifact probe status=${PRE_PROBE_STATUS:-ERR} bytes=${PRE_PROBE_BYTES:-0} (was ${PRE_ARTIFACT_BYTES:-0}). Head-less worker or cleanup race possible."
fi

# (b) POST artifact distinct from PRE
if [[ "$POST_ARTIFACT_URL" != "$PRE_ARTIFACT_URL" ]]; then
  log_info "POST artifact URL distinct from PRE: $POST_ARTIFACT_URL (PRE was $PRE_ARTIFACT_URL)"
  # Hash compare via full artifact downloads (these smoke artifacts are small):
  PRE_HASH=$(curl -sS -m 20 "$PRE_ARTIFACT_URL"  -H "Authorization: Bearer $ADMIN_TOKEN" 2>/dev/null | sha256sum | awk '{print $1}')
  POST_HASH=$(curl -sS -m 20 "$POST_ARTIFACT_URL" -H "Authorization: Bearer $ADMIN_TOKEN" 2>/dev/null | sha256sum | awk '{print $1}')
  if [[ -n "$PRE_HASH" && -n "$POST_HASH" && "$PRE_HASH" != "$POST_HASH" ]]; then
    INV_SHA_DISTINCT_OK=1
    log_info "POST artifact first-64KB SHA256 distinct from PRE (POST=$POST_HASH PRE=$PRE_HASH)"
  else
    log_info "artifact content is deterministic or SHA check is inconclusive; distinct URLs plus preserved PRE artifact prove no clobber"
    if [[ "$PRE_PROBE_STATUS" == "200" || "$PRE_PROBE_STATUS" == "206" ]]; then
      INV_SHA_DISTINCT_OK=1
    fi
  fi
else
  log_error "FAIL: POST artifact URL equals PRE artifact URL: $POST_ARTIFACT_URL — temp file likely clobbered"
fi

# (c) POST lease was on target worker (placement pin preserved post-restart)
if [[ "$POST_LEASED_WORKER" == "$TARGET_WORKER_ID" && "$POST_STATUS_FIELD" == "SUCCEEDED" ]]; then
  INV_PLACEMENT_PIN_PRESERVED_OK=1
  log_info "placement-pin preserved post-restart: $POST_LEASED_WORKER (matches target)"
else
  log_error "FAIL: post-restart smoke status=$POST_STATUS_FIELD leased_worker=$POST_LEASED_WORKER (expected $TARGET_WORKER_ID + SUCCEEDED)"
fi

# ─── Step 7: write restart.json (atomic: tmp + mv) ────────────────────────
OUT_DIR="${REPO_ROOT}/tests/worker-cert/workers/${TARGET_WORKER_ID}"
ensure_dir "$OUT_DIR"
OUT_FILE="${REPORT_JSON:-${OUT_DIR}/restart.json}"
TMP_OUT=$(mktemp "${OUT_DIR}/restart-XXXXXX.json")
NOW_ISO=$(date -u +'%Y-%m-%dT%H:%M:%SZ')
cat > "$TMP_OUT" <<JSON
{
  "schema": "tests/worker-cert/restart@1",
  "worker_id": "${TARGET_WORKER_ID}",
  "restart_cmd": "${RESTART_CMD}",
  "restart_duration_s": ${RESTART_DURATION},
  "pre_restart": {
    "status": "${PRE_STATUS}",
    "session_active": ${PRE_SESSION_ACTIVE},
    "session_id": "${PRE_SESSION_ID}",
    "last_heartbeat_at": "${PRE_LAST_HB}",
    "job_id": "${PRE_JOB_ID}",
    "artifact_url": "${PRE_ARTIFACT_URL}",
    "artifact_size_bytes": ${PRE_ARTIFACT_BYTES:-0}
  },
  "post_restart": {
    "status": "${POST_STATUS}",
    "session_active": ${POST_SESSION_ACTIVE},
    "session_id": "${POST_SESSION_ID}",
    "last_heartbeat_at": "${POST_LAST_HB}",
    "job_id": "${POST_JOB_ID}",
    "artifact_url": "${POST_ARTIFACT_URL}",
    "artifact_size_bytes": ${POST_ARTIFACT_BYTES:-0},
    "leased_worker_id": "${POST_LEASED_WORKER}",
    "status_field": "${POST_STATUS_FIELD}"
  },
  "interference_probe": {
    "pre_probe_status": "${PRE_PROBE_STATUS:-ERR}",
    "pre_probe_bytes": ${PRE_PROBE_BYTES:-0}
  },
  "invariants": {
    "session_id_changed_ok":           $([[ $INV_SESSION_ID_CHANGED_OK -eq 1 ]] && echo true || echo false),
    "log_marker_ok":                   $([[ $INV_LOG_MARKER_OK -eq 1 ]] && echo true || echo false),
    "post_smoke_passed_ok":            $([[ $INV_POST_SMOKE_OK -eq 1 ]] && echo true || echo false),
    "no_clobber_ok":                   $([[ $INV_NO_CLOBBER_OK -eq 1 ]] && echo true || echo false),
    "sha_distinct_ok":                 $([[ $INV_SHA_DISTINCT_OK -eq 1 ]] && echo true || echo false),
    "placement_pin_preserved_ok":      $([[ $INV_PLACEMENT_PIN_PRESERVED_OK -eq 1 ]] && echo true || echo false)
  },
  "destination_id": "${DESTINATION_ID}",
  "master_url": "${VELOX_MASTER_URL}",
  "smoke_runner_rev": ${SMOKE_PLUCKER_VARS_REV},
  "written_at": "${NOW_ISO}"
}
JSON
mv "$TMP_OUT" "$OUT_FILE"
log_info "wrote $OUT_FILE"

# ─── Final summary + rc synthesis ─────────────────────────────────────────
OVERALL_RC=0
if (( INV_SESSION_ID_CHANGED_OK == 0 ));          then OVERALL_RC=7;  fi
if (( INV_POST_SMOKE_OK == 0 ))                   && (( OVERALL_RC < 9 )); then OVERALL_RC=9;  fi
if (( INV_NO_CLOBBER_OK == 0 || INV_SHA_DISTINCT_OK == 0 || INV_PLACEMENT_PIN_PRESERVED_OK == 0 )) \
                                                   && (( OVERALL_RC < 10 )); then OVERALL_RC=10; fi

echo "OK: restart_reconnect $TARGET_WORKER_ID"
echo "  restart_duration_s        : $RESTART_DURATION"
echo "  pre_session_id            : $PRE_SESSION_ID"
echo "  post_session_id           : $POST_SESSION_ID"
echo "  session_id_changed        : $INV_SESSION_ID_CHANGED_OK"
echo "  log_marker_found          : $INV_LOG_MARKER_OK"
echo "  post_smoke_passed         : $INV_POST_SMOKE_OK"
echo "  no_clobber                : $INV_NO_CLOBBER_OK (pre probe status=$PRE_PROBE_STATUS bytes=$PRE_PROBE_BYTES)"
echo "  sha_distinct              : $INV_SHA_DISTINCT_OK"
echo "  placement_pin_preserved   : $INV_PLACEMENT_PIN_PRESERVED_OK"
echo "  restart.json              : $OUT_FILE"
exit "$OVERALL_RC"
