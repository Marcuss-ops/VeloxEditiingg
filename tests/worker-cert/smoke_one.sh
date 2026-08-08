#!/usr/bin/env bash
# =============================================================================
# tests/worker-cert/smoke_one.sh — Per-worker SUCCEEDED smoke for VeloxEditingg.
# =============================================================================
# Usage:
#   ./tests/worker-cert/smoke_one.sh <worker_id>
#   VELOX_MASTER_URL=https://velox.example.com \
#     VELOX_ADMIN_TOKEN=... \
#     SMOKE_DESTINATION_ID=drive-production \
#     ./tests/worker-cert/smoke_one.sh host_57_131_20_173
#
# What the script does:
#   1. Sources tests/_lib/sh/_lib.sh (logging + pid-trap + ensure +
#      aggregate) and tests/worker-cert/lib/pluck.sh (smoke-local helpers).
#   2. Mints an ephemeral M2M client via POST /api/v1/admin/m2m/keys; DELETEs
#      it on exit (best-effort, see trap).
#   3. GET /api/v1/workers, asserts that the target <worker_id> is the only
#      CONNECTED worker (placement pin clarity WARN) — the actual enforcement
#      is at the post-SUCCEEDED step where the script verifies the lease was
#      granted to <worker_id>.
#   4. POST /api/v1/jobs with a real-asset payload (velox-asset://<asset_id>,
#      the explicitly selected destination, scene.composite.v1@1.
#      submit path). Uses jobs_smoke.sh's idempotency_key scheme.
#   5. Polls GET /api/v1/jobs/<job_id> until SUCCEEDED (exponential backoff
#      1→2→4→8→16s, cap SMOKE_POLL_TIMEOUT_S, default 180s).
#   6. Scrapes the most recent TaskLeaseGranted line for <job_id> from
#      $VELOX_MASTER_LOG_PATH (fallback: journalctl -u velox-server).
#   7. Computes render_time_ms from QueueItem.started_at - completed_at and
#      probes the artifact URL HEAD for artifact_size_bytes.
#   8. Asserts the lease string was issued to <worker_id>; FAIL otherwise.
#   9. Writes workers/<worker_id>/smoke.json (atomic via tmp+mv) and prints a
#      summary on stdout.
#
# Exit codes:
#   0  success — worker placed, job SUCCEEDED, smoke.json written.
#   2  usage / env (missing admin token / unknown arg / no curl|jq).
#   3  network (curl could not reach the master).
#   4  non-201 on M2M provisioning / 4xx 5xx on submit.
#   5  poll timeout without reaching terminal state.
#   6  terminal-fail: FAILED or CANCELLED.
#   7  worker-mismatch: SUCCEEDED but lease was NOT granted to <worker_id>.
# =============================================================================

set -uo pipefail  # NOT -e (intentional: keep going through polling and log scrape)

REAL_SCRIPT="$(readlink -f "${BASH_SOURCE[0]}")"
SCRIPT_DIR="$(cd "$(dirname "$REAL_SCRIPT")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# ─── Cross-test helpers ────────────────────────────────────────────────────
# tests/_lib/sh/ lives at the repo root, one level above this script's dir.
# Use REPO_ROOT rather than chasing relative paths so a future move of this
# script under tests/e2e/... would not silently break sourcing.
# shellcheck source=tests/_lib/sh/_lib.sh
source "${REPO_ROOT}/tests/_lib/sh/_lib.sh"
# shellcheck source=tests/worker-cert/lib/pluck.sh
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
[[ -n "${VELOX_MASTER_URL:-}"  ]] || VELOX_MASTER_URL="http://127.0.0.1:8000"
[[ -n "${SMOKE_DESTINATION_ID:-}" ]] || { log_error "SMOKE_DESTINATION_ID is required; implicit Drive destinations are forbidden"; exit 2; }
[[ -n "${SMOKE_POLL_TIMEOUT_S:-}" ]] || SMOKE_POLL_TIMEOUT_S=180
[[ -n "${SMOKE_OUT_ROOT:-}"   ]] || SMOKE_OUT_ROOT="${REPO_ROOT}/tests/worker-cert/workers"
VELOX_MASTER_URL="${VELOX_MASTER_URL%/}"
SMOKE_DESTINATION_ID="${SMOKE_DESTINATION_ID%/}"
log_info "smoke_one target=$TARGET_WORKER_ID master=$VELOX_MASTER_URL dest=$SMOKE_DESTINATION_ID timeout=${SMOKE_POLL_TIMEOUT_S}s"

# ─── Resolve admin token (env > TOKEN_FILE) ────────────────────────────────
resolve_admin_token() {
  local v=""
  if [[ -n "${VELOX_ADMIN_TOKEN:-}" ]]; then v="$VELOX_ADMIN_TOKEN";
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

# ─── M2M provisioning ─────────────────────────────────────────────────────
M2M_BEARER=""
PROVISIONED_CLIENT_ID=""
TMP_HDRS=""
TMP_BODY=""
cleanup_smoke_one() {
  [[ -n "$TMP_HDRS" && -e "$TMP_HDRS" ]] && rm -f "$TMP_HDRS"
  [[ -n "$TMP_BODY" && -e "$TMP_BODY" ]] && rm -f "$TMP_BODY"
}
on_signals() {
  cleanup_smoke_one
  if [[ -n "$PROVISIONED_CLIENT_ID" && -n "$ADMIN_TOKEN" ]]; then
    curl -sS -m 5 -X DELETE \
      -H "Authorization: Bearer $ADMIN_TOKEN" \
      "${VELOX_MASTER_URL}/api/v1/admin/m2m/keys/${PROVISIONED_CLIENT_ID}" \
      >/dev/null 2>&1 || true
  fi
}
trap 'on_signals' EXIT INT TERM

if ! smoke_mint_m2m "$ADMIN_TOKEN" "$VELOX_MASTER_URL"; then
  log_error "M2M provisioning failed"; exit 4
fi
log_info "M2M provisioned: client_id=$PROVISIONED_CLIENT_ID"

# ─── Worker list pre-flight ────────────────────────────────────────────────
# shellcheck disable=SC2034 # populated by smoke_workers_list in sourced pluck.sh
WORKERS_JSON=""
if ! smoke_workers_list "$ADMIN_TOKEN" "$VELOX_MASTER_URL"; then
  log_error "could not list workers"; exit 3
fi
TARGET_RECORD=$(smoke_worker_by_id "$TARGET_WORKER_ID")
if [[ -z "$TARGET_RECORD" ]]; then
  log_error "target worker not in /api/v1/workers: $TARGET_WORKER_ID"; exit 4
fi
T_STATUS=$(printf '%s' "$TARGET_RECORD" | jq -r '.status // "(unset)"')
T_SESSION=$(printf '%s' "$TARGET_RECORD" | jq -r '.session_active // false')
log_info "target worker status=$T_STATUS session_active=$T_SESSION"
if [[ "$T_STATUS" != "CONNECTED" || "$T_SESSION" != "true" ]]; then
  log_error "target worker not CONNECTED + session_active; cannot run deterministic smoke"
  exit 4
fi
smoke_assert_pin_clarity "$TARGET_WORKER_ID"

# ─── Build real-asset payload ──────────────────────────────────────────────
ASSETS_FILE="${SCRIPT_DIR}/fixtures/assets.json"
if ! [[ -r "$ASSETS_FILE" ]]; then
  log_error "fixtures not readable: $ASSETS_FILE"; exit 2
fi
ASSET_VO=$(jq -er '.voiceover[0].asset_id' "$ASSETS_FILE")
ASSET_CLIP_A=$(jq -er '.clips[0].asset_id' "$ASSETS_FILE")
ASSET_CLIP_B=$(jq -er '.clips[1].asset_id' "$ASSETS_FILE")
ASSET_SUB=$(jq -er '.subtitles[0].asset_id' "$ASSETS_FILE")
log_info "assets: vo=$ASSET_VO clip_a=$ASSET_CLIP_A clip_b=$ASSET_CLIP_B sub=$ASSET_SUB"

EPOCH=$(date +%s)
IDEM_KEY="smoke-one-${TARGET_WORKER_ID}-${EPOCH}"
# Build the strict canonical SubmitJobRequest shape. The shared builder
# attaches clip + voiceover to each scene and emits the technical envelope;
# no positional voiceover_paths or scene.clip_link aliases cross the wire.
PAYLOAD_FILE=$(mktemp)
if ! python3 "${REPO_ROOT}/tests/worker-cert/build_real_payload.py" \
      --fixtures "$ASSETS_FILE" \
      --worker-id "$TARGET_WORKER_ID" \
      --placement-pin-worker-id "$TARGET_WORKER_ID" \
      --destination "$SMOKE_DESTINATION_ID" \
      --strict \
      --output "$PAYLOAD_FILE" >/dev/null 2>&1; then
  log_error "canonical payload builder failed"; rm -f "$PAYLOAD_FILE"; exit 4
fi
PAYLOAD=$(cat "$PAYLOAD_FILE")
rm -f "$PAYLOAD_FILE"
# Preserve the exact smoke idempotency key while keeping the builder's
# canonical placement pin and nested scene assets.
PAYLOAD=$(printf '%s' "$PAYLOAD" | jq --arg idem "$IDEM_KEY" ".idempotency_key = \$idem")

# ─── POST /api/v1/jobs ─────────────────────────────────────────────────────
TMP_HDRS=$(mktemp); TMP_BODY=$(mktemp)
curl -sS -m 30 -X POST \
  -H "Authorization: Bearer $M2M_BEARER" \
  -H "Content-Type: application/json" \
  -H "X-Request-ID: smoke-one-${TARGET_WORKER_ID}-${EPOCH}" \
  --data-raw "$PAYLOAD" \
  "${VELOX_MASTER_URL}/api/v1/jobs" \
  -D "$TMP_HDRS" -o "$TMP_BODY" 2>/dev/null
POST_STATUS=$(awk 'NR==1 && $1 ~ /^[Hh][Tt][Tt][Pp]\// {print $2; exit}' "$TMP_HDRS")
POST_BODY=$(cat "$TMP_BODY")
cleanup_smoke_one
if [[ "$POST_STATUS" != "202" ]]; then
  log_error "POST /api/v1/jobs returned HTTP $POST_STATUS"
  log_error "  body: $(printf '%s' "$POST_BODY" | head -c 400)"
  exit 4
fi
JOB_ID=$(printf '%s' "$POST_BODY" | jq -er '.job_id // empty') || {
  log_error "missing job_id in 202 body"; exit 4; }
log_info "submitted job_id=$JOB_ID"

# ─── Poll until SUCCEEDED ──────────────────────────────────────────────────
elapsed=0
sleep_s=1
last_status=""
last_body=""
while (( elapsed < SMOKE_POLL_TIMEOUT_S )); do
  sleep "$sleep_s"
  elapsed=$((elapsed + sleep_s))
  sleep_s=$(( sleep_s * 2 )); (( sleep_s > 16 )) && sleep_s=16
  curl -sS -m 10 \
    -H "Authorization: Bearer $M2M_BEARER" \
    "${VELOX_MASTER_URL}/api/v1/jobs/${JOB_ID}" \
    >"$TMP_BODY" 2>/dev/null || { sleep_s=1; continue; }
  RESP_BODY=$(cat "$TMP_BODY")
  sv=$(printf '%s' "$RESP_BODY" | jq -er '.status // empty' 2>/dev/null || true)
  if [[ -n "$sv" ]]; then
    last_status="$sv"; last_body="$RESP_BODY"
  fi
  case "$sv" in
    SUCCEEDED) break ;;
    FAILED|CANCELLED)
      log_error "terminal-fail state $sv after ${elapsed}s"
      log_error "  body: $(printf '%s' "$RESP_BODY" | head -c 400)"
      exit 6 ;;
  esac
done
rm -f "$TMP_BODY"
if [[ "$last_status" != "SUCCEEDED" ]]; then
  if [[ -z "$last_status" ]]; then
    log_error "poll timeout after ${SMOKE_POLL_TIMEOUT_S}s (no successful GET in window)"
  else
    log_error "poll timeout after ${SMOKE_POLL_TIMEOUT_S}s (last observed status=$last_status)"
  fi
  exit 5
fi
log_info "job SUCCEEDED after ${elapsed}s"

# ─── Scrape lease + assert worker ──────────────────────────────────────────
# NOTE: by the time SUCCEEDED is observed, the worker's current_task_id slot
# is already cleared (race). The authoritative placement-pin evidence is the
# master log line `TaskLeaseGranted sent to worker <ID> (...)`. We scrape it
# directly and compare <ID> to $TARGET_WORKER_ID.
LEASE_JSON=$(smoke_scrape_lease "$JOB_ID" "${VELOX_MASTER_LOG_PATH:-}")
LEASED_WORKER=$(printf '%s' "$LEASE_JSON" | jq -er '.worker_id // empty')
TASK_ID=$(printf '%s' "$LEASE_JSON" | jq -er '.task_id // empty')
ATTEMPT_ID=$(printf '%s' "$LEASE_JSON" | jq -er '.attempt_id // empty')
LEASE_ID=$(printf '%s' "$LEASE_JSON" | jq -er '.lease_id  // empty')
log_info "lease worker=$LEASED_WORKER task=$TASK_ID attempt=$ATTEMPT_ID lease_id=$LEASE_ID"

# ─── Compute render_time_ms + artifact size ────────────────────────────────
# GET /api/v1/jobs/{id} now returns started_at, completed_at, artifact_url,
# artifact_size_bytes, worker_id, task_id, attempt_id, lease_id (commit 5b976f3).
STARTED_AT=$(printf '%s' "$last_body"  | jq -er '.started_at   // empty' 2>/dev/null || true)
COMPLETED_AT=$(printf '%s' "$last_body" | jq -er '.completed_at // empty' 2>/dev/null || true)
ARTIFACT_URL=$(printf '%s' "$last_body" | jq -er '.artifact_url // empty' 2>/dev/null || true)
ARTIFACT_SIZE_FROM_API=$(printf '%s' "$last_body" | jq -er '.artifact_size_bytes // 0' 2>/dev/null || echo "0")

if [[ -z "$STARTED_AT" || -z "$COMPLETED_AT" ]]; then
  log_error "started_at or completed_at missing from GET /api/v1/jobs/{id} response — endpoint regression?"
  render_time_ms="0"
else
  s_epoch=$(smoke_parse_iso8601 "$STARTED_AT")
  c_epoch=$(smoke_parse_iso8601 "$COMPLETED_AT")
  render_time_ms="0"
  if [[ -n "$s_epoch" && -n "$c_epoch" ]]; then
    render_time_ms=$(awk -v a="$s_epoch" -v b="$c_epoch" 'BEGIN{printf "%.0f", (b-a)*1000}')
  fi
fi

# Prefer artifact_size_bytes from the API response (no extra HEAD roundtrip).
# Fall back to smoke_artifact_size HEAD probe only if the API field is absent.
if [[ -n "$ARTIFACT_SIZE_FROM_API" && "$ARTIFACT_SIZE_FROM_API" != "0" ]]; then
  artifact_size_bytes="$ARTIFACT_SIZE_FROM_API"
elif [[ -n "$ARTIFACT_URL" ]]; then
  artifact_size_bytes=$(smoke_artifact_size "$ARTIFACT_URL" "$M2M_BEARER")
else
  artifact_size_bytes="0"
fi
log_info "render_time_ms=$render_time_ms artifact_bytes=$artifact_size_bytes"

# ─── Worker-on-leased assertion (placement-pin enforcement) ────────────────
# Authoritative signal: the worker_id embedded by the master in the
# TaskLeaseGranted log line, NOT the per-worker current_task_id (race-prone
# post-SUCCEEDED). The fallback to current_task_id remains as a weaker
# cross-check when log scraping returns an empty worker_id.
PIN_OK=false
if [[ -n "$LEASED_WORKER" && "$LEASED_WORKER" == "$TARGET_WORKER_ID" ]]; then
  PIN_OK=true
else
  POST_WORKER=$(smoke_worker_by_id "$TARGET_WORKER_ID")
  CUR_TASK=$(printf '%s' "$POST_WORKER" | jq -er '.current_task_id // empty' 2>/dev/null || true)
  if [[ -n "$TASK_ID" && "$CUR_TASK" == "$TASK_ID" ]]; then
    PIN_OK=true
  fi
fi
if ! $PIN_OK; then
  log_error "worker-mismatch: SUCCEEDED but lease did not pin to <$TARGET_WORKER_ID>"
  log_error "  scraped worker_id=<${LEASED_WORKER:-<empty>}>, expected=${TARGET_WORKER_ID}"
  # Still write the JSON for operator post-mortem, but with status=MISMATCH
  STATUS_FIELD="WORKER_MISMATCH"
else
  STATUS_FIELD="SUCCEEDED"
fi

# ─── Write smoke.json (atomic: tmp + mv) ───────────────────────────────────
OUT_DIR="${SMOKE_OUT_ROOT}/${TARGET_WORKER_ID}"
ensure_dir "$OUT_DIR"
OUT_FILE="${OUT_DIR}/smoke.json"
TMP_OUT=$(mktemp "${OUT_DIR}/smoke-XXXXXX.json")
NOW_ISO=$(date -u +'%Y-%m-%dT%H:%M:%SZ')
cat > "$TMP_OUT" <<JSON
{
  "schema": "tests/worker-cert/smoke@1",
  "worker_id": "${TARGET_WORKER_ID}",
  "job_id": "${JOB_ID}",
  "task_id": "${TASK_ID}",
  "attempt_id": "${ATTEMPT_ID}",
  "lease_id": "${LEASE_ID}",
  "status": "${STATUS_FIELD}",
  "artifact_size_bytes": ${artifact_size_bytes:-0},
  "render_time_ms": ${render_time_ms:-0},
  "started_at": "${STARTED_AT}",
  "completed_at": "${COMPLETED_AT}",
  "artifact_url": "${ARTIFACT_URL}",
  "destination_id": "${SMOKE_DESTINATION_ID}",
  "master_url": "${VELOX_MASTER_URL}",
  "smoke_runner_rev": ${SMOKE_PLUCKER_VARS_REV},
  "written_at": "${NOW_ISO}"
}
JSON
mv "$TMP_OUT" "$OUT_FILE"
log_info "wrote $OUT_FILE"

# ─── Final summary ─────────────────────────────────────────────────────────
echo "OK: smoke_one $TARGET_WORKER_ID"
echo "  job_id           : $JOB_ID"
echo "  status           : $STATUS_FIELD"
echo "  task_id          : $TASK_ID"
echo "  attempt_id       : $ATTEMPT_ID"
echo "  render_time_ms   : $render_time_ms"
echo "  artifact_bytes   : $artifact_size_bytes"
[[ "$STATUS_FIELD" == "WORKER_MISMATCH" ]] && exit 7 || exit 0
