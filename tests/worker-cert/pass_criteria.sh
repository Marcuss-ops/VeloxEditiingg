#!/usr/bin/env bash
# =============================================================================
# tests/worker-cert/pass_criteria.sh — 10-step PASS criteria flow for one
# worker. Asserts each step with a grep / curl / run probe and emits a
# per-step JSON report + binary verdict on stdout.
# =============================================================================
# Usage:
#   ./tests/worker-cert/pass_criteria.sh <worker_id>
#   VELOX_MASTER_URL=https://velox.example.com \
#     VELOX_ADMIN_TOKEN=... \
#     PASS_DESTINATION_ID=drive-production \
#     ./tests/worker-cert/pass_criteria.sh host_57_131_20_173
#
# The 10 PASS criteria (matches the user-spec checklist):
#   1. CONNECTED          — GET /api/v1/workers/<worker_id>.status == "CONNECTED"
#   2. session_active     — .session_active == true (derived from worker_sessions)
#   3. scene.composite.v1@1 — executors list contains id=scene.composite.v1 version=1
#   4. TaskLeaseGranted   — master log marker for <job_id> + worker_id
#   5. TaskAccepted       — master log marker for <job_id> + accepted
#   6. asset scaricati    — task_attempt_metrics.download_ms > 0 OR blob_size>0
#   7. RUNNING            — runtime_status transition: worker_task_runtime RUNNING
#                           OR master log "TaskRunning for <job_id>"
#   8. rendering          — engine phase progression (engine_render_ms > 0)
#                           OR log marker "render started" / "StageExecutor"
#   9. artifact verificato — verify_artifact.sh on downloaded artifact
#  10. SUCCEEDED          — GET /api/v1/jobs/<job_id>.status == "SUCCEEDED"
#
# Output:
#   * Per-step JSON dump on stdout (one line, jq-discoverable).
#   * Atomic write of workers/<worker_id>/pass_criteria.json (tmp+mv same fs).
#   * "PASS" or "FAIL: step N (<name>)" on stdout as the last line.
#
# Exit codes:
#   0  PASS — all 10 steps satisfied for <worker_id>.
#   2  usage / env (missing admin token / unknown arg / no curl|jq|ffprobe).
#   3  network (curl could not reach the master).
#   4  non-201/202 on M2M provisioning / submit / GET /api/v1/workers invalid.
#   5  poll timeout (SUCCEEDED not reached within PASS_POLL_TIMEOUT_S).
#   10+<step_idx> a specific step regressed (rc=10 for step 1, 11 for step 2,
#                 ... 19 for step 10). Operator-side mismatch is detectable
#                 by exit-code band, not just by per-step JSON.
#
# Backward compat: smoke_one.sh + verify_artifact.sh are NOT modified. This
# script composes them via subprocess invocation for step 9 only.
# =============================================================================

set -uo pipefail  # NOT -e: keep going through each step so the report captures all verdicts

REAL_SCRIPT="$(readlink -f "${BASH_SOURCE[0]}")"
SCRIPT_DIR="$(cd "$(dirname "$REAL_SCRIPT")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# ─── Cross-test helpers ────────────────────────────────────────────────────
# shellcheck source=tests/_lib/sh/_lib.sh
# shellcheck disable=SC1091
source "${REPO_ROOT}/tests/_lib/sh/_lib.sh"
# shellcheck source=tests/worker-cert/lib/pluck.sh
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/lib/pluck.sh"

# ─── Args / env ────────────────────────────────────────────────────────────
usage() {
  sed -n '2,/^# ====/p' "$0" | sed 's/^# \{0,1\}//'
  exit 0
}
if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
  usage
fi
TARGET_WORKER_ID="${1:-}"
if [[ -z "$TARGET_WORKER_ID" ]]; then
  log_error "usage: $0 <worker_id>"; exit 2
fi
ensure_command_available curl  || { log_error "curl missing"; exit 2; }
ensure_command_available jq    || { log_error "jq missing"; exit 2; }
ensure_command_available ffprobe || { log_warn "ffprobe missing: step 9 (artifact verificato) will be marked SKIPPED instead of FAIL"; }
[[ -n "${VELOX_MASTER_URL:-}"   ]] || VELOX_MASTER_URL="http://127.0.0.1:8000"
[[ -n "${PASS_DESTINATION_ID:-}"  ]] || { log_error "PASS_DESTINATION_ID is required; implicit Drive destinations are forbidden"; exit 2; }
[[ -n "${PASS_POLL_TIMEOUT_S:-}"  ]] || PASS_POLL_TIMEOUT_S=180
[[ -n "${PASS_OUT_ROOT:-}"        ]] || PASS_OUT_ROOT="${REPO_ROOT}/tests/worker-cert/workers"
[[ -n "${PASS_ARTIFACT_DIR:-}"    ]] || PASS_ARTIFACT_DIR="${REPO_ROOT}/tests/worker-cert/artifacts"
VELOX_MASTER_URL="${VELOX_MASTER_URL%/}"
PASS_DESTINATION_ID="${PASS_DESTINATION_ID%/}"
log_info "pass_criteria target=$TARGET_WORKER_ID master=$VELOX_MASTER_URL dest=$PASS_DESTINATION_ID timeout=${PASS_POLL_TIMEOUT_S}s"

# ─── Resolve admin token ───────────────────────────────────────────────────
resolve_admin_token() {
  local v=""
  if [[ -n "${VELOX_ADMIN_TOKEN:-}" ]]; then v="$VELOX_ADMIN_TOKEN"
  elif [[ -n "${TOKEN_FILE:-}" && -r "${TOKEN_FILE}" ]]; then
    v=$(grep -E '^VELOX_ADMIN_TOKEN=' "$TOKEN_FILE" | head -1 \
      | sed 's/^[^=]*=//' | tr -d '"' | tr -d "'" | xargs || true)
  fi
  if [[ -z "$v" ]]; then
    log_error "VELOX_ADMIN_TOKEN unset and TOKEN_FILE not provided"; return 2
  fi
  if [[ "$v" == *$'\r'* || "$v" == *$'\n'* ]]; then
    log_error "VELOX_ADMIN_TOKEN contains CR or LF; refusing"; return 2
  fi
  printf '%s' "$v"
}
ADMIN_TOKEN=$(resolve_admin_token) || exit 2

# ─── M2M provisioning + per-step JSON accumulator ──────────────────────────
M2M_BEARER=""
PROVISIONED_CLIENT_ID=""
STEPS_JSON=""
PASS_FAIL_STEP=0   # first failing step number; 0 = none yet

# shellcheck disable=SC2329 # invoked indirectly by the EXIT/INT/TERM trap
on_exit_cleanup() {
  local rc=$?
  [[ -n "$M2M_BEARER" && -n "$PROVISIONED_CLIENT_ID" && -n "$ADMIN_TOKEN" ]] && \
    curl -sS -m 5 -X DELETE \
      -H "Authorization: Bearer $ADMIN_TOKEN" \
      "${VELOX_MASTER_URL}/api/v1/admin/m2m/keys/${PROVISIONED_CLIENT_ID}" \
      >/dev/null 2>&1 || true
  exit "$rc"
}
trap on_exit_cleanup EXIT INT TERM

# append_step <idx> <name> <status> <detail>
# status: PASS | FAIL | SKIPPED
append_step() {
  local idx="$1" name="$2" status="$3" detail="${4:-}"
  local esc="${detail//\\/\\\\}"
  esc="${esc//\"/\\\"}"
  local entry
  entry=$(printf '{"idx":%s,"name":%s,"status":%s,"detail":%s}' \
    "$(jq -n --argjson i "$idx" '$i')" \
    "$(jq -n --arg n "$name" '$n')" \
    "$(jq -n --arg s "$status" '$s')" \
    "$(jq -n --arg d "$esc" '$d')")
  if [[ -z "$STEPS_JSON" ]]; then STEPS_JSON="$entry"; else STEPS_JSON="${STEPS_JSON},${entry}"; fi
  local colour
  case "$status" in
    PASS)    colour=$'\033[32m' ;;  # green
    FAIL)    colour=$'\033[31m' ;;  # red
    SKIPPED) colour=$'\033[33m' ;;  # yellow
    *)       colour=$'\033[0m'  ;;
  esac
  printf '  %s%-2d%s %-22s %s\n' "$colour" "$idx" $'\033[0m' "[$status]" "$name — ${detail:0:120}"
  if [[ "$status" == "FAIL" && "$PASS_FAIL_STEP" == "0" ]]; then
    PASS_FAIL_STEP="$idx"
  fi
}

# Pre-initialise report fields before the writer can be called on any early
# failure path. Empty strings are deliberately valid report values.
JOB_ID=""
TASK_ID=""
ATTEMPT_ID=""
LEASE_ID=""
LEASE_WORKER=""
ARTIFACT_URL=""
ARTIFACT_SIZE_BYTES=0
DOWNLOAD_MS=""
RENDER_MS_ENGINE=""
elapsed=0

# ─── Atomic JSON report function ────────────────────────────────────────
# shellcheck disable=SC2329 # invoked indirectly by the explicit early-exit paths
write_pass_criteria_json() {
  local now_iso verdict out_file tmp_out
  now_iso=$(date -u +'%Y-%m-%dT%H:%M:%SZ')
  if (( PASS_FAIL_STEP == 0 )); then
    verdict="PASS"
  else
    verdict="FAIL"
  fi
  ensure_dir "${PASS_OUT_ROOT}/${TARGET_WORKER_ID}"
  out_file="${PASS_OUT_ROOT}/${TARGET_WORKER_ID}/pass_criteria.json"
  tmp_out=$(mktemp "${PASS_OUT_ROOT}/${TARGET_WORKER_ID}/pass-XXXXXX.json")
  if ! jq -n \
    --arg schema "tests/worker-cert/pass_criteria@1" \
    --arg worker_id "$TARGET_WORKER_ID" \
    --arg verdict "$verdict" \
    --arg job_id "$JOB_ID" \
    --arg task_id "$TASK_ID" \
    --arg attempt_id "$ATTEMPT_ID" \
    --arg lease_id "$LEASE_ID" \
    --arg lease_worker "$LEASE_WORKER" \
    --arg artifact_url "$ARTIFACT_URL" \
    --arg download_ms "$DOWNLOAD_MS" \
    --arg render_ms_engine "$RENDER_MS_ENGINE" \
    --arg master_url "$VELOX_MASTER_URL" \
    --arg destination_id "$PASS_DESTINATION_ID" \
    --arg checked_at "$now_iso" \
    --argjson first_failing_step "${PASS_FAIL_STEP:-0}" \
    --argjson artifact_size_bytes "${ARTIFACT_SIZE_BYTES:-0}" \
    --argjson poll_timeout_s "$PASS_POLL_TIMEOUT_S" \
    --argjson elapsed_s "$elapsed" \
    --argjson steps "[${STEPS_JSON}]" \
    '{schema: $schema, worker_id: $worker_id, verdict: $verdict,
      first_failing_step: $first_failing_step, job_id: $job_id,
      task_id: $task_id, attempt_id: $attempt_id, lease_id: $lease_id,
      lease_worker: $lease_worker, artifact_url: $artifact_url,
      artifact_size_bytes: $artifact_size_bytes, download_ms: $download_ms,
      render_ms_engine: $render_ms_engine, master_url: $master_url,
      destination_id: $destination_id, poll_timeout_s: $poll_timeout_s,
      elapsed_s: $elapsed_s, checked_at: $checked_at, steps: $steps}' \
    >"$tmp_out"; then
    rm -f "$tmp_out"
    log_error "could not render pass_criteria report"
    return 1
  fi
  mv -f "$tmp_out" "$out_file"
  log_info "wrote $out_file"
  printf '%s\n' "REPORT_JSON=${out_file}"
  printf '%s\n' "VERDICT=${verdict}"
  printf '%s\n' "FIRST_FAILING_STEP=${PASS_FAIL_STEP:-0}"
}

# ─── M2M + workers pre-flight ─────────────────────────────────────────────
if ! smoke_mint_m2m "$ADMIN_TOKEN" "$VELOX_MASTER_URL"; then
  log_error "M2M provisioning failed"; exit 4
fi
log_info "M2M provisioned: client_id=$PROVISIONED_CLIENT_ID"

if ! smoke_workers_list "$ADMIN_TOKEN" "$VELOX_MASTER_URL"; then
  log_error "could not list workers"; exit 3
fi
TARGET_RECORD=$(smoke_worker_by_id "$TARGET_WORKER_ID")
if [[ -z "$TARGET_RECORD" ]]; then
  log_error "target worker not in /api/v1/workers: $TARGET_WORKER_ID"; exit 4
fi

# ─── Step 1 — CONNECTED ────────────────────────────────────────────────────
T_STATUS=$(printf '%s' "$TARGET_RECORD" | jq -r '.status // "(unset)"')
if [[ "$T_STATUS" == "CONNECTED" ]]; then
  append_step 1 "CONNECTED"          "PASS" "status=CONNECTED"
else
  append_step 1 "CONNECTED"          "FAIL" "status=$T_STATUS (expected CONNECTED)"
fi

# ─── Step 2 — session_active ───────────────────────────────────────────────
T_SESSION=$(printf '%s' "$TARGET_RECORD" | jq -r '.session_active // false')
if [[ "$T_SESSION" == "true" ]]; then
  append_step 2 "session_active"     "PASS" "session_active=true"
else
  append_step 2 "session_active"     "FAIL" "session_active=$T_SESSION (expected true)"
fi

# ─── Step 3 — scene.composite.v1@1 advertised ──────────────────────────────
EXECUTORS=$(printf '%s' "$TARGET_RECORD" | jq -r '.executors[]? | "\(.id)@\(.version)"' 2>/dev/null \
  || printf '')
if printf '%s' "$EXECUTORS" | grep -qx 'scene.composite.v1@1'; then
  append_step 3 "scene.composite.v1@1" "PASS" "scene.composite.v1@1 advertised"
else
  append_step 3 "scene.composite.v1@1" "FAIL" "advertised=[$(printf '%s' "$EXECUTORS" | tr '\n' ',' | sed 's/,$//')]"
fi

# Bail early if step 1/2/3 already failed — submitting a job to a non-conforming
# worker would just produce noise. We still write the report with FAIL steps
# so the per-step JSON dump is the canonical post-mortem.
if (( PASS_FAIL_STEP > 0 )); then
  log_error "aborting before submit: worker not in required state (step $PASS_FAIL_STEP failed)"
  write_pass_criteria_json
  exit $((10 + PASS_FAIL_STEP))
fi

# ─── Build real-asset payload (canonical pattern, scene.composite.v1@1) ───
ASSETS_FILE="${SCRIPT_DIR}/fixtures/assets.json"
[[ -r "$ASSETS_FILE" ]] || { log_error "fixtures not readable: $ASSETS_FILE"; exit 2; }
EPOCH=$(date +%s)
IDEM_KEY="pass-criteria-${TARGET_WORKER_ID}-${EPOCH}"
PAYLOAD_FILE=$(mktemp)
if ! python3 "${REPO_ROOT}/tests/worker-cert/build_real_payload.py" \
      --fixtures "$ASSETS_FILE" \
      --worker-id "$TARGET_WORKER_ID" \
      --placement-pin-worker-id "$TARGET_WORKER_ID" \
      --destination "$PASS_DESTINATION_ID" \
      --strict \
      --output "$PAYLOAD_FILE" >/dev/null 2>&1; then
  log_error "canonical payload builder failed"; rm -f "$PAYLOAD_FILE"; exit 4
fi
PAYLOAD=$(jq --arg idem "$IDEM_KEY" '.idempotency_key = $idem' "$PAYLOAD_FILE")
rm -f "$PAYLOAD_FILE"

TMP_HDRS=$(mktemp); TMP_BODY=$(mktemp)
curl -sS -m 30 -X POST \
  -H "Authorization: Bearer $M2M_BEARER" \
  -H "Content-Type: application/json" \
  -H "X-Request-ID: pass-criteria-${TARGET_WORKER_ID}-${EPOCH}" \
  --data-raw "$PAYLOAD" \
  "${VELOX_MASTER_URL}/api/v1/jobs" \
  -D "$TMP_HDRS" -o "$TMP_BODY" 2>/dev/null
POST_STATUS=$(awk 'NR==1 && $1 ~ /^[Hh][Tt][Tt][Pp]\// {print $2; exit}' "$TMP_HDRS")
POST_BODY=$(cat "$TMP_BODY")
if [[ "$POST_STATUS" != "202" ]]; then
  log_error "POST /api/v1/jobs returned HTTP $POST_STATUS"
  log_error "  body: $(printf '%s' "$POST_BODY" | head -c 400)"
  exit 4
fi
JOB_ID=$(printf '%s' "$POST_BODY" | jq -er '.job_id // empty') || { log_error "missing job_id"; exit 4; }
log_info "submitted job_id=$JOB_ID"

# ─── Poll loop with embedded log scraping for steps 4-8 ────────────────────
# We scrape the master log (or journalctl) on every iteration so transient
# marker emissions (TaskLeaseGranted, TaskAccepted, RUNNING, rendering) get
# captured at the right moment. We track each step as a boolean — the
# first observation is recorded, repeat matches don't overwrite (idempotent).
TASK_ID=""
ATTEMPT_ID=""
LEASE_ID=""
LEASE_WORKER=""
ARTIFACT_URL=""
ARTIFACT_SIZE_BYTES=0
DOWNLOAD_MS=""
RENDER_MS_ENGINE=""
LEASE_SEEN=false
ACCEPTED_SEEN=false
DOWNLOAD_SEEN=false
RUNNING_SEEN=false
RENDERING_SEEN=false

# Detect log source once.
LOG_SRC=""
if [[ -n "${VELOX_MASTER_LOG_PATH:-}" && -r "${VELOX_MASTER_LOG_PATH:-}" ]]; then
  LOG_SRC="path:${VELOX_MASTER_LOG_PATH}"
elif command -v journalctl >/dev/null 2>&1; then
  LOG_SRC="journalctl:-u velox-server"
fi
log_info "log source: ${LOG_SRC:-<none>}"

scrape_log_marker() {
  # Sets booleans + IDs by grepping the master log source for our job_id.
  local needle="$1" pattern="$2"
  local line=""
  if [[ "$LOG_SRC" == path:* ]]; then
    line=$(grep -F "$needle" "${VELOX_MASTER_LOG_PATH}" 2>/dev/null | grep -E "$pattern" | tail -1 || true)
  elif [[ "$LOG_SRC" == journalctl:* ]]; then
    line=$(journalctl -u velox-server -n 5000 --no-pager 2>/dev/null \
      | grep -F "$needle" | grep -E "$pattern" | tail -1 || true)
  fi
  printf '%s' "$line"
}

elapsed=0
sleep_s=1
last_body=""
terminal_state=""
while (( elapsed < PASS_POLL_TIMEOUT_S )); do
  sleep "$sleep_s"
  elapsed=$((elapsed + sleep_s))
  sleep_s=$(( sleep_s * 2 )); (( sleep_s > 16 )) && sleep_s=16

  # ── Master GET /api/v1/jobs/<job_id> ──
  curl -sS -m 10 -H "Authorization: Bearer $M2M_BEARER" \
    "${VELOX_MASTER_URL}/api/v1/jobs/${JOB_ID}" \
    >"$TMP_BODY" 2>/dev/null || { sleep_s=1; continue; }
  RESP_BODY=$(cat "$TMP_BODY")
  last_body="$RESP_BODY"
  sv=$(printf '%s' "$RESP_BODY" | jq -er '.status // empty' 2>/dev/null || true)
  case "$sv" in
    SUCCEEDED) terminal_state="$sv"; break ;;
    FAILED|CANCELLED)
      terminal_state="$sv"
      log_error "terminal-fail state $sv after ${elapsed}s"
      log_error "  body: $(printf '%s' "$RESP_BODY" | head -c 400)"
      break ;;
  esac

  # ── Step 4: TaskLeaseGranted emitted by handler_stream.go ──
  if ! $LEASE_SEEN; then
    line=$(scrape_log_marker "$JOB_ID" 'TaskLeaseGranted' || true)
    if [[ -n "$line" ]]; then
      LEASE_SEEN=true
      TASK_ID=$(printf '%s' "$line" | sed -n 's/.*task=\([^ ]*\).*/\1/p' | head -1)
      ATTEMPT_ID=$(printf '%s' "$line" | sed -n 's/.*attempt=\([^ ]*\).*/\1/p' | head -1)
      LEASE_ID=$(printf '%s' "$line" | sed -n 's/.*lease=\([^ ]*\).*/\1/p' | head -1)
      LEASE_WORKER=$(printf '%s' "$line" | sed -n 's/.*sent to worker \([^ ]*\).*/\1/p' | head -1)
      if [[ "$LEASE_WORKER" == "$TARGET_WORKER_ID" ]]; then
        append_step 4 "TaskLeaseGranted" "PASS" "task=$TASK_ID attempt=$ATTEMPT_ID worker=$LEASE_WORKER"
      else
        append_step 4 "TaskLeaseGranted" "FAIL" "leased_worker=${LEASE_WORKER:-<empty>} target=$TARGET_WORKER_ID"
      fi
    fi
  fi

  # ── Step 5: TaskAccepted (worker -> master) ──
  if ! $ACCEPTED_SEEN; then
    line=$(scrape_log_marker "$JOB_ID" 'TaskAccepted' || true)
    if [[ -n "$line" ]]; then
      ACCEPTED_SEEN=true
      append_step 5 "TaskAccepted" "PASS" "task=$TASK_ID accepted"
    fi
  fi

  # ── Step 6: asset download ──
  if ! $DOWNLOAD_SEEN; then
    # Master log marker: "asset downloaded for task=..." OR successful TASK_ACCEPTED echo.
    line=$(scrape_log_marker "$JOB_ID" 'asset.download|asset_downloaded|TaskAccepted.*asset_paths' || true)
    if [[ -n "$line" ]]; then
      DOWNLOAD_MS=$(printf '%s' "$line" | sed -n 's/.*download_ms=\([0-9]\+\).*/\1/p' | head -1)
      DOWNLOAD_SEEN=true
      append_step 6 "asset scaricati" "PASS" "asset download marker present (download_ms=$DOWNLOAD_MS)"
    fi
  fi

  # ── Step 7: RUNNING (worker_task_runtime OR master log marker) ──
  if ! $RUNNING_SEEN; then
    line=$(scrape_log_marker "$JOB_ID" 'runtime_status.*RUNNING|state.*RUNNING' || true)
    if [[ -n "$line" ]]; then
      RUNNING_SEEN=true
      append_step 7 "RUNNING" "PASS" "runtime_status=RUNNING"
    fi
  fi

  # ── Step 8: rendering (engine phase progression) ──
  if ! $RENDERING_SEEN; then
    line=$(scrape_log_marker "$JOB_ID" 'engine_render_started|render_started|RenderPipelineStart|StageExecutor' || true)
    if [[ -n "$line" ]]; then
      RENDERING_SEEN=true
      RENDER_MS_ENGINE=$(printf '%s' "$line" | sed -n 's/.*render_ms=\([0-9]\+\).*/\1/p' | head -1)
      append_step 8 "rendering" "PASS" "engine render started (render_ms=$RENDER_MS_ENGINE)"
    fi
  fi
done

if [[ "$terminal_state" != "SUCCEEDED" ]]; then
  log_error "poll timeout after ${PASS_POLL_TIMEOUT_S}s (last status=${sv:-<none>})"
  if (( PASS_FAIL_STEP == 0 )); then
    # Mark every unimpressed transient step as TIMEOUT (subtype of FAIL).
    [[ "$LEASE_SEEN"      == "true" ]] || append_step 4 "TaskLeaseGranted" "FAIL" "TIMEOUT — no marker in ${PASS_POLL_TIMEOUT_S}s"
    [[ "$ACCEPTED_SEEN"   == "true" ]] || append_step 5 "TaskAccepted"     "FAIL" "TIMEOUT — no marker in ${PASS_POLL_TIMEOUT_S}s"
    [[ "$DOWNLOAD_SEEN"   == "true" ]] || append_step 6 "asset scaricati"  "FAIL" "TIMEOUT — no marker in ${PASS_POLL_TIMEOUT_S}s"
    [[ "$RUNNING_SEEN"    == "true" ]] || append_step 7 "RUNNING"          "FAIL" "TIMEOUT — no marker in ${PASS_POLL_TIMEOUT_S}s"
    [[ "$RENDERING_SEEN"  == "true" ]] || append_step 8 "rendering"        "FAIL" "TIMEOUT — no marker in ${PASS_POLL_TIMEOUT_S}s"
  fi
  write_pass_criteria_json
  exit $((10 + ${PASS_FAIL_STEP:-0}))
fi
log_info "job SUCCEEDED after ${elapsed}s"

# ─── Step 9 — artifact verificato (use verify_artifact.sh) ────────────────
ARTIFACT_URL=$(printf '%s' "$last_body" | jq -er '.artifact_url // .artifact_path // .output_path // empty')
ARTIFACT_SIZE_BYTES=$(smoke_artifact_size "$ARTIFACT_URL" "$M2M_BEARER")
ARTIFACT_STATUS="FAIL"
ARTIFACT_DETAIL="artifact_url=${ARTIFACT_URL:-<empty>} size_bytes=${ARTIFACT_SIZE_BYTES:-0}"
if [[ -n "$ARTIFACT_URL" && "${ARTIFACT_SIZE_BYTES:-0}" -gt 0 ]]; then
  ensure_dir "$PASS_ARTIFACT_DIR"
  TMP_ARTIFACT="${PASS_ARTIFACT_DIR}/${TARGET_WORKER_ID}-${JOB_ID}.mp4"
  if curl -sS -m 60 -H "Authorization: Bearer $M2M_BEARER" "$ARTIFACT_URL" \
        -o "$TMP_ARTIFACT" 2>/dev/null \
     && [[ -s "$TMP_ARTIFACT" ]]; then
    # invoke verify_artifact.sh subprocess on the downloaded artifact
    VERIFY_RC=0
    # The polling loop above already recorded the authoritative SUCCEEDED
    # transition. Do not repeat the master-side GET here: after finalization
    # some deployments retire the creator-forwarding lookup row, so a second
    # GET can return job_not_found even though the artifact and terminal
    # status are valid. Step 9 is the artifact/media gate; step 10 is the
    # terminal-status gate.
      bash "${SCRIPT_DIR}/verify_artifact.sh" "$TMP_ARTIFACT" \
        --min-duration-s 1.0 \
        --max-duration-s 86400.0 \
        --min-width 480 --min-height 320 --min-fps 23.976 \
        --required-video-codec h264 --required-audio-codec aac \
      >/dev/null 2>&1 || VERIFY_RC=$?
    if (( VERIFY_RC == 0 )); then
      ARTIFACT_STATUS="PASS"
      ARTIFACT_DETAIL="verify_artifact.sh rc=0 artifact=${TMP_ARTIFACT}"
    else
      ARTIFACT_DETAIL="verify_artifact.sh rc=$VERIFY_RC artifact=${TMP_ARTIFACT}"
    fi
  else
    ARTIFACT_DETAIL="download failed: artifact_url=${ARTIFACT_URL}"
  fi
fi
append_step 9 "artifact verificato" "$ARTIFACT_STATUS" "$ARTIFACT_DETAIL"

# ─── Step 10 — SUCCEEDED ───────────────────────────────────────────────────
if [[ "$terminal_state" == "SUCCEEDED" ]]; then
  append_step 10 "SUCCEEDED" "PASS" "status=SUCCEEDED"
else
  append_step 10 "SUCCEEDED" "FAIL" "terminal_state=${terminal_state:-<empty>}"
fi

# ─── Atomic JSON report + binary verdict ───────────────────────────────────
write_pass_criteria_json

if (( PASS_FAIL_STEP == 0 )); then
  printf '%s\n' "PASS: ${TARGET_WORKER_ID} — all 10 criteria satisfied"
  exit 0
else
  printf '%s\n' "FAIL: ${TARGET_WORKER_ID} — step ${PASS_FAIL_STEP} regressed"
  exit $((10 + PASS_FAIL_STEP))
fi
