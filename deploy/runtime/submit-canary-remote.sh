#!/usr/bin/env bash
# deploy/runtime/submit-canary-remote.sh
# ─────────────────────────────────────────────────────────────────────────────
# Remote end-to-end canary: prove that a remote Velox worker can download
# real HTTP/Drive assets, render a scene.composite.v1 job, and return a
# verified artifact to the master.
#
# This script is the REMOTE counterpart of submit-canary-local.sh. It does
# NOT use docker exec, does NOT generate fixtures inside a worker container,
# does NOT reference file:// paths, and does NOT access the master's SQLite
# database directly. Everything goes through the public master HTTP API and
# uses real remote URLs (Drive/HTTP) for clips and voiceovers.
#
# Workflow:
#   1. Pre-flight tools + env + master reachability.
#   2. Verify at least one remote worker advertises scene.composite.v1@1.
#   3. POST the real generate payload
#      (ops/jobs/jackie_chan_doc_voiceover.generate.json).
#   4. Poll /api/v1/script/jobs/:job_id/full until terminal state.
#   5. Verify the job reached SUCCEEDED and surface key metadata.
#   6. Emit ONE of: "PASS: ...", "FAIL: ...", "SKIP: ..." on stdout/stderr.
#      Exit codes: 0 PASS, 1 FAIL, 255 SKIP.
#
# Required env:
#   VELOX_MASTER_URL    e.g. http://127.0.0.1:8000
#   VELOX_ADMIN_TOKEN   bearer token for AdminAuthMiddleware
#
# Optional env:
#   VELOX_CANARY_PAYLOAD_FILE  path to the JSON payload to submit
#                              (default: ops/jobs/jackie_chan_doc_voiceover.generate.json)
#   VELOX_CANARY_TIMEOUT       max seconds to wait for terminal status
#                              (default: 600)
#   VELOX_CANARY_INTERVAL      seconds between polls (default: 10)
#
# Exit codes:
#   0   canary submitted and job reached SUCCEEDED
#   1   canary FAILED — pre-flight, worker registration, submit, or terminal
#       failure
#   255 canary was SKIPPED — missing tools or env

set -euo pipefail

emit_pass() { printf 'PASS: %s\n' "$*"; }
emit_fail() { printf 'FAIL: %s\n' "$*" >&2; }
emit_skip() { printf 'SKIP: %s\n' "$*" >&2; }

# ── 1. Pre-flight: tools ─────────────────────────────────────────────────────
for tool in curl jq; do
    command -v "$tool" >/dev/null 2>&1 \
        || { emit_skip "infrastructure tool not in PATH: $tool"; exit 255; }
done

# ── 2. Pre-flight: env ─────────────────────────────────────────────────────
: "${VELOX_MASTER_URL:?VELOX_MASTER_URL not set (e.g. http://127.0.0.1:8000)}"
: "${VELOX_ADMIN_TOKEN:?VELOX_ADMIN_TOKEN not set}"

MASTER_URL="${VELOX_MASTER_URL%/}"
ADMIN_HEADER="Authorization: Bearer ${VELOX_ADMIN_TOKEN}"

PAYLOAD_FILE="${VELOX_CANARY_PAYLOAD_FILE:-ops/jobs/jackie_chan_doc_voiceover.generate.json}"
POLL_TIMEOUT="${VELOX_CANARY_TIMEOUT:-600}"
POLL_INTERVAL="${VELOX_CANARY_INTERVAL:-10}"

if ! [[ "$POLL_TIMEOUT" =~ ^[0-9]+$ ]] || (( POLL_TIMEOUT <= 0 )); then
    emit_fail "VELOX_CANARY_TIMEOUT must be a positive integer (got: ${POLL_TIMEOUT})"
    exit 1
fi
if ! [[ "$POLL_INTERVAL" =~ ^[0-9]+$ ]] || (( POLL_INTERVAL <= 0 )); then
    emit_fail "VELOX_CANARY_INTERVAL must be a positive integer (got: ${POLL_INTERVAL})"
    exit 1
fi

# Resolve payload file relative to the repository root when the script is
# invoked from deploy/runtime/.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ -r "${SCRIPT_DIR}/${PAYLOAD_FILE}" ]]; then
    PAYLOAD_FILE="${SCRIPT_DIR}/${PAYLOAD_FILE}"
elif [[ -r "${SCRIPT_DIR}/../../${PAYLOAD_FILE}" ]]; then
    PAYLOAD_FILE="${SCRIPT_DIR}/../../${PAYLOAD_FILE}"
fi

[[ -r "$PAYLOAD_FILE" ]] \
    || { emit_fail "payload file not readable: $PAYLOAD_FILE"; exit 1; }

# ── 3. Master health check ─────────────────────────────────────────────────
if ! curl -fsS --max-time 10 -H "$ADMIN_HEADER" "${MASTER_URL}/health/ready" >/dev/null 2>&1; then
    emit_fail "master not ready at ${MASTER_URL}/health/ready"
    exit 1
fi

# ── 4. Verify a remote worker with scene.composite.v1@1 is registered ────────
WORKERS_JSON="$(curl -fsS --max-time 10 -H "$ADMIN_HEADER" "${MASTER_URL}/api/v1/workers")"
if ! jq -e '.workers | type == "array"' <<<"$WORKERS_JSON" >/dev/null 2>&1; then
    emit_fail "master /api/v1/workers did not return a workers array"
    exit 1
fi

WORKER_MATCH="$(jq -r '.workers[]? | select(any(.executors[]?; .id == "scene.composite.v1" and .version == 1)) | .worker_id' <<<"$WORKERS_JSON" | head -1)"
if [[ -z "$WORKER_MATCH" ]]; then
    emit_fail "no worker advertises executor scene.composite.v1 version 1"
    exit 1
fi

# Ensure the matched worker is actually connected and active.
WORKER_SESSION="$(jq -r --arg id "$WORKER_MATCH" '.workers[]? | select(.worker_id == $id) | .session_active' <<<"$WORKERS_JSON")"
WORKER_STATUS="$(jq -r --arg id "$WORKER_MATCH" '.workers[]? | select(.worker_id == $id) | .status' <<<"$WORKERS_JSON")"
if [[ "$WORKER_SESSION" != "true" || "$WORKER_STATUS" != "CONNECTED" ]]; then
    emit_fail "worker ${WORKER_MATCH} advertises scene.composite.v1@1 but is not connected (session_active=${WORKER_SESSION} status=${WORKER_STATUS})"
    exit 1
fi

# ── 5. Submit the real generate payload ─────────────────────────
SUBMIT_RESP="$(curl -fsS --max-time 30 \
    -X POST "${MASTER_URL}/api/v1/script/generate" \
    -H "$ADMIN_HEADER" \
    -H "Content-Type: application/json" \
    --data-binary "@${PAYLOAD_FILE}")"

JOB_ID="$(jq -r '.job_id // empty' <<<"$SUBMIT_RESP")"
[[ -n "$JOB_ID" ]] \
    || { emit_fail "submit returned no job_id (response: ${SUBMIT_RESP:0:240})"; exit 1; }

# Whitelist JOB_ID before interpolating it into URLs.
[[ "$JOB_ID" =~ ^[A-Za-z0-9._-]{8,128}$ ]] \
    || { emit_fail "JOB_ID has unexpected shape; refused to use in URL (got: ${JOB_ID:0:16}...)"; exit 1; }

# ── 6. Poll /api/v1/script/jobs/:job_id/full for terminal status ───────────
POLL_DEADLINE=$(( $(date +%s) + POLL_TIMEOUT ))
LAST_STATUS=""
while (( $(date +%s) < POLL_DEADLINE )); do
    JOB_RESP="$(curl -fsS --max-time 10 -H "$ADMIN_HEADER" "${MASTER_URL}/api/v1/script/jobs/${JOB_ID}/full")"
    STATUS="$(jq -r '.status // empty' <<<"$JOB_RESP")"
    LAST_STATUS="$STATUS"

    case "$STATUS" in
        SUCCEEDED) break ;;
        FAILED|CANCELLED)
            ERROR_MSG="$(jq -r '.error // .job.error // empty' <<<"$JOB_RESP")"
            emit_fail "job ${JOB_ID} reached terminal failure: ${STATUS}${ERROR_MSG:+ (${ERROR_MSG})}"
            exit 1
            ;;
    esac

    sleep "$POLL_INTERVAL"
done

[[ "$LAST_STATUS" == "SUCCEEDED" ]] \
    || { emit_fail "poll timeout (${POLL_TIMEOUT}s); last status: ${LAST_STATUS:-<none>}"; exit 1; }

# ── 7. Surface success metadata ─────────────────────────────────────────────
OUTPUT_PATH="$(jq -r '.output_path // empty' <<<"$JOB_RESP")"
COMPLETED_AT="$(jq -r '.completed_at // empty' <<<"$JOB_RESP")"
RESULT_KIND="$(jq -r '.job.result.kind // .result.kind // empty' <<<"$JOB_RESP")"

[[ -n "$OUTPUT_PATH" ]] \
    || { emit_fail "job ${JOB_ID} succeeded but no output_path was returned"; exit 1; }

DETAIL="job_id=${JOB_ID} status=SUCCEEDED output_path=${OUTPUT_PATH}"
[[ -n "$COMPLETED_AT" ]] && DETAIL="${DETAIL} completed_at=${COMPLETED_AT}"
[[ -n "$RESULT_KIND" ]] && DETAIL="${DETAIL} result_kind=${RESULT_KIND}"

emit_pass "$DETAIL"
