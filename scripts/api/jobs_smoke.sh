#!/usr/bin/env bash
# =============================================================================
# jobs_smoke.sh — End-to-end smoke for /api/v1/jobs (POST + polling GET).
#
# Purpose:
#   1. Use $VELOX_ADMIN_TOKEN to mint an ephemeral M2M client via
#      POST /api/v1/admin/m2m/keys (operator-only admin surface).
#   2. POST /api/v1/jobs with the plaintext_secret returned in step 1 as the
#      M2M bearer; capture job_id from the 202 response + Location header.
#   3. GET /api/v1/jobs/{job_id} with exponential backoff (1s → 2s → 4s →
#      8s → 16s, capped at 60s) until the lifecycle reaches SUCCEEDED,
#      FAILED, or CANCELLED.
#   4. Delete the ephemeral M2M key (best-effort cleanup, runs on every exit
#      path including middle-of-loop failures).
#
# This smoke is OPERATOR-CENTRIC WIRE PROOFING. It validates the live master
# from outside the codebase the way an actual external client would — via curl,
# with a M2M bearer that was minted via the admin surface, against the same
# polling endpoint the e2e tests exercise. It is NOT a substitute for the Go
# e2e tests (job_submit_e2e_test.go + creator_push_e2e_test.go): those cover
# the full resolver/forwarding lifecycle WITH real SQLite + assertion edges;
# this smoke covers the wiring on a deployed master under load balancer,
# ingress, network egress, real-SQLite auth lookups, etc.
#
# Endpoint contract (VeloxEditiingg):
#   POST   {VELOX_MASTER_URL}/api/v1/admin/m2m/keys
#     Authorization: Bearer ${VELOX_ADMIN_TOKEN}
#     Body: { "client_id": "...", "scopes": ["jobs.submit"], ... }
#     201   → { "client_id":..., "plaintext_secret":..., "secret_hash":..., ... }
#
#   POST   {VELOX_MASTER_URL}/api/v1/jobs
#     Authorization: Bearer <plaintext_secret from admin POST>
#     Body:    { "idempotency_key": "...", "scenes": [...], ... }
#     202     → { "ok":true, "job_id":"...", "accepted_from":"api_v1_jobs",
#                 "status_url":"/api/v1/jobs/..." }
#     Location header → /api/v1/jobs/{job_id}
#
#   GET    {VELOX_MASTER_URL}/api/v1/jobs/{job_id}
#     Authorization: Bearer <plaintext_secret>
#     200     → { "ok":true, "job_id":"...", "status":"PENDING|...", ... }
#     404     → { "ok":false, "error":"job_not_found", ... }
#
# Environment contract:
#   VELOX_MASTER_URL    (optional) Base URL of the Velox server.
#                       Default: http://127.0.0.1:8080
#   VELOX_ADMIN_TOKEN   (mandatory) Bearer for the admin surface. Source
#                       precedence: 1. VELOX_ADMIN_TOKEN env var,
#                       2. TOKEN_FILE env var (path to dotenv).
#                       Both unset → exit 2.
#   JOBS_IDEM_KEY       (optional) Stable idempotency key override. Default:
#                       smoke-<hostname>-<epoch_seconds>. Override for CI
#                       matrices that re-assert the same job on rolling deploys.
#   JOBS_POLL_TIMEOUT_S (optional) Polling cap in seconds. Default: 60.
#                       Max single sleep during exp backoff: 16s.
#   JOBS_SMOKE_DEBUG    (optional) "1" → curl verbose + response dump.
#
# Exit codes:
#   0  success — POST 202 + GET terminal state SUCCEEDED
#   2  usage / env error (missing admin token, unknown flag, no curl/jq)
#   3  network error (curl could not reach server — provisioning OR polling)
#   4  HTTP non-202 on POST (rejected at intake — 401, 409 idempotency, 422 payload, ...)
#   5  HTTP 202 but response body did not carry a populated job_id
#   6  POST + GET succeeded but reached FAILED or CANCELLED (terminal-fail)
#   7  polling timeout — neither reached a terminal state within
#      $JOBS_POLL_TIMEOUT_S (default 60s; smoke budget is short)
#   8  HTTP non-200 on GET (server-rejected during polling — distinct from
#      terminal-fail so an operator can investigate)
#
# Usage:
#   ./scripts/api/jobs_smoke.sh
#   VELOX_MASTER_URL=https://velox.example.com ./scripts/api/jobs_smoke.sh
#   JOBS_IDEM_KEY=stable-jobs-smoke-rfc-12345 ./scripts/api/jobs_smoke.sh
#   JOBS_POLL_TIMEOUT_S=15 ./scripts/api/jobs_smoke.sh
#   ./scripts/api/jobs_smoke.sh --help
# =============================================================================

set -euo pipefail

# ---- usage ------------------------------------------------------------------
usage() {
  sed -n '2,/^# ====/p' "$0" | sed 's/^# \{0,1\}//'
  exit 0
}

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
  usage
fi

# ---- prerequisites ----------------------------------------------------------
for bin in curl jq; do
  if ! command -v "$bin" >/dev/null 2>&1; then
    echo "FATAL: required binary not found in PATH: $bin" >&2
    exit 2
  fi
done

# ---- admin token resolution (env > TOKEN_FILE dotenv) ----------------------
# Mirrors scripts/creator_push_smoke.sh::resolve_token so an operator can
# reuse one TOKEN_FILE across both smokes.
resolve_token() {
  local value=""
  if [[ -n "${VELOX_ADMIN_TOKEN:-}" ]]; then
    value="$VELOX_ADMIN_TOKEN"
  elif [[ -n "${TOKEN_FILE:-}" && -r "${TOKEN_FILE}" ]]; then
    value=$(grep -E '^VELOX_ADMIN_TOKEN=' "$TOKEN_FILE" | head -1 \
      | sed 's/^[^=]*=//' \
      | tr -d '"' | tr -d "'" | xargs || true)
  fi
  if [[ -z "$value" ]]; then
    echo "FATAL: VELOX_ADMIN_TOKEN unset and TOKEN_FILE not provided / unreadable" >&2
    echo "  Remediation: export VELOX_ADMIN_TOKEN=... or set TOKEN_FILE=/path/to/.env" >&2
    return 1
  fi
  if [[ "$value" == *$'\r'* || "$value" == *$'\n'* ]]; then
    echo "FATAL: VELOX_ADMIN_TOKEN contains CR or LF; refusing to use it" >&2
    return 1
  fi
  printf '%s' "$value"
}

ADMIN_TOKEN=$(resolve_token) || exit 2

# ---- server URL -------------------------------------------------------------
MASTER_URL="${VELOX_MASTER_URL:-http://127.0.0.1:8080}"
MASTER_URL="${MASTER_URL%/}"   # strip trailing slash
ADMIN_KEYS_URL="${MASTER_URL}/api/v1/admin/m2m/keys"
JOBS_URL="${MASTER_URL}/api/v1/jobs"

# ---- ephemeral client provisioning ------------------------------------------
# client_id derives from epoch seconds so back-to-back smokes get distinct
# clients (a re-run with the same minute would 409 on the unique constraint;
# the script retries once with +1 second if that happens).
EPOCH=$(date +%s)
CLIENT_ID="smoke-cli-${EPOCH}-$$"
IDEM_KEY="${JOBS_IDEM_KEY:-smoke-${HOSTNAME:-localhost}-${EPOCH}}"
POLL_TIMEOUT="${JOBS_POLL_TIMEOUT_S:-60}"

# Issuance request: minimal payload. The defaults from IssueM2MKey give
# scopes=["jobs.submit"], rate_limit_rps=0 → cfg.DefaultRPS, burst=0 →
# cfg.DefaultBurst, quotas=0 → cfg.MaxScenes/DefaultMaxTotalDurationS. For
# smoke we override rate limit to a comfortable burst so the script doesn't
# trip its OWN rate limiter between provisioning and the first POST.
ISSUE_REQ=$(cat <<JSON
{
  "client_id": "${CLIENT_ID}",
  "description": "jobs_smoke ephemeral client (auto-cleaned)",
  "scopes": ["jobs.submit"],
  "rate_limit_rps": 5,
  "rate_limit_burst": 10,
  "quota_max_scenes": 100,
  "quota_max_total_secs": 600
}
JSON
)

# Track cleanup state. trap fires on every exit (including errexit + signals).
M2M_BEARER=""
PROVISIONED_CLIENT_ID=""
cleanup() {
  if [[ -n "$PROVISIONED_CLIENT_ID" && -n "$ADMIN_TOKEN" ]]; then
    # Best-effort DELETE; never let cleanup mask the original exit code.
    curl -sS -m 5 -X DELETE \
      -H "Authorization: Bearer ${ADMIN_TOKEN}" \
      "${ADMIN_KEYS_URL}/${PROVISIONED_CLIENT_ID}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

TMP_BODY=$(mktemp)
TMP_HDRS=$(mktemp)
TMP_TRACE=$(mktemp)
trap 'rm -f "$TMP_BODY" "$TMP_HDRS" "$TMP_TRACE"; cleanup' EXIT

post_admin_issue() {
  local rc=0
  curl -sS -m 15 -X POST -H "Authorization: Bearer ${ADMIN_TOKEN}" \
    -H "Content-Type: application/json" \
    --data-raw "$ISSUE_REQ" \
    -D "$TMP_HDRS" -o "$TMP_BODY" 2>"$TMP_TRACE" || rc=$?
  return $rc
}

issue_status=$(awk 'NR==1 && $1 ~ /^[Hh][Tt][Tt][Pp]\// {print $2; exit}' "$TMP_HDRS" || true)
# We don't have an issue_status yet — first call.
issue_body=""
echo "--- provisioning ephemeral M2M client: ${CLIENT_ID} ---"
if ! post_admin_issue; then
  echo "FATAL: curl could not reach ${ADMIN_KEYS_URL} during M2M provisioning" >&2
  echo "  Hints:" >&2
  echo "    - is VELOX_MASTER_URL set correctly?" >&2
  echo "    - is VELOX_ADMIN_TOKEN valid against the master? (401 = mismatch)" >&2
  exit 3
fi
issue_status=$(awk 'NR==1 && $1 ~ /^[Hh][Tt][Tt][Pp]\// {print $2; exit}' "$TMP_HDRS")
issue_body=$(cat "$TMP_BODY")

# If we collided on the unique constraint (concurrent re-runs in the same
# second), retry once with +1 second in client_id. This is the canonical
# 409 idempotency path on the admin surface; the retry is purely cosmetic
# for the smoke.
if [[ "$issue_status" == "409" ]] || [[ "$issue_status" == "400" ]]; then
  CLIENT_ID="smoke-cli-$((${EPOCH}+1))-$$"
  ISSUE_REQ=$(echo "$ISSUE_REQ" | sed "s/${EPOCH})/$((${EPOCH}+1))/")
  if ! post_admin_issue; then
    echo "FATAL: retry of M2M provisioning also failed at network level" >&2
    exit 3
  fi
  issue_status=$(awk 'NR==1 && $1 ~ /^[Hh][Tt][Tt][Pp]\// {print $2; exit}' "$TMP_HDRS")
  issue_body=$(cat "$TMP_BODY")
fi

if [[ "$issue_status" != "201" ]]; then
  echo "FATAL: provisioning M2M key returned HTTP ${issue_status:-?}" >&2
  echo "  Endpoint: POST ${ADMIN_KEYS_URL}" >&2
  echo "  Response body:" >&2
  echo "$issue_body" | sed 's/^/    /' >&2
  if [[ "$issue_status" == "401" ]]; then
    echo "  Hint: VELOX_ADMIN_TOKEN mismatch with server cfg.Auth.AdminToken." >&2
  fi
  exit 4
fi

M2M_BEARER=$(printf '%s' "$issue_body" | jq -er '.plaintext_secret // empty') || true
if [[ -z "$M2M_BEARER" ]]; then
  echo "FATAL: provisioning M2M key returned 201 but plaintext_secret is missing" >&2
  echo "  Response body:" >&2
  echo "$issue_body" | sed 's/^/    /' >&2
  exit 5
fi
PROVISIONED_CLIENT_ID="$CLIENT_ID"

echo "  client_id          : ${PROVISIONED_CLIENT_ID}"
echo "  plaintext_secret   : ********** (redacted)"

# ---- POST /api/v1/jobs ------------------------------------------------------
JOBS_PAYLOAD=$(cat <<JSON
{
  "idempotency_key": "${IDEM_KEY}",
  "video_name": "jobs_smoke at ${EPOCH}",
  "script_text": "Smoke-test scenario: voiceover + clip + scene.",
  "scenes": [
    {
      "text": "Smoke scene 1",
      "clip": { "url": "velox-asset://clips/smoke-${EPOCH}-01.mp4", "duration_ms": 3000 },
      "voiceover": { "url": "velox-asset://voiceovers/smoke-${EPOCH}.mp3", "duration_ms": 3000 },
      "duration_seconds": 3
    },
    {
      "text": "Smoke scene 2",
      "clip": { "url": "velox-asset://clips/smoke-${EPOCH}-02.mp4", "duration_ms": 3000 },
      "voiceover": { "url": "velox-asset://voiceovers/smoke-${EPOCH}.mp3", "duration_ms": 3000 },
      "duration_seconds": 3
    }
  ],
  "delivery_plan": [
    { "destination_id": "drive", "priority": 1, "retry_budget": 1 }
  ]
}
JSON
)

echo "--- POST /api/v1/jobs (idempotency_key=${IDEM_KEY}) ---"
CURL_RC=0
curl -sS -m 30 -X POST \
  -H "Authorization: Bearer ${M2M_BEARER}" \
  -H "Content-Type: application/json" \
  -H "X-Request-ID: jobs-smoke-${EPOCH}" \
  --data-raw "$JOBS_PAYLOAD" \
  "${JOBS_URL}" \
  -D "$TMP_HDRS" -o "$TMP_BODY" 2>"$TMP_TRACE" || CURL_RC=$?

if [[ "${JOBS_SMOKE_DEBUG:-0}" == "1" ]]; then
  echo "DEBUG: POST headers:" >&2
  cat "$TMP_HDRS" >&2
  echo "DEBUG: POST body:" >&2
  cat "$TMP_BODY" >&2
fi

if [[ $CURL_RC -ne 0 ]]; then
  echo "FATAL: curl could not reach ${JOBS_URL} (exit=${CURL_RC})" >&2
  exit 3
fi

post_status=$(awk 'NR==1 && $1 ~ /^[Hh][Tt][Tt][Pp]\// {print $2; exit}' "$TMP_HDRS")
post_body=$(cat "$TMP_BODY")

if [[ -z "$post_status" ]]; then
  echo "FATAL: could not parse HTTP status from POST response headers" >&2
  exit 3
fi

if [[ "$post_status" != "202" ]]; then
  echo "FATAL: expected HTTP 202 but got ${post_status}" >&2
  echo "  Endpoint: POST ${JOBS_URL}" >&2
  echo "  Response body:" >&2
  echo "$post_body" | sed 's/^/    /' >&2
  if [[ "$post_status" == "401" ]]; then
    echo "  Hint: M2M bearer lookup failed — was the key rotation/disable-out-of-band?" >&2
  elif [[ "$post_status" == "409" ]]; then
    echo "  Hint: idempotency_key already used (different payload). Drop JOBS_IDEM_KEY or freshen it." >&2
  elif [[ "$post_status" == "422" ]]; then
    echo "  Hint: cross-field validation rejected. Mirrors doc/API-JOBS.md field rules." >&2
  fi
  exit 4
fi

# ---- extract job_id + canonical status_url --------------------------------
JOB_ID=""
JOB_ID_RC=0
JOB_ID=$(printf '%s' "$post_body" | jq -er '.job_id // empty') || JOB_ID_RC=$?
if [[ $JOB_ID_RC -ne 0 || -z "$JOB_ID" ]]; then
  echo "FATAL: HTTP 202 received but job_id is missing or empty" >&2
  echo "  Response body:" >&2
  echo "$post_body" | sed 's/^/    /' >&2
  exit 5
fi

# Location header is the canonical polling URL; fall back to status_url field
# if upstream gateway stripped it (rare). The path itself is what we need for
# the polling loop.
LOCATION_HEADER=$(awk 'tolower($1) == "location:" { sub(/\r$/, ""); print $2; exit }' "$TMP_HDRS")
STATUS_URL_FIELD=$(printf '%s' "$post_body" | jq -er '.status_url // empty') || true
STATUS_URL="${LOCATION_HEADER:-${STATUS_URL_FIELD:-/api/v1/jobs/${JOB_ID}}}"

ACCEPTED_FROM=$(printf '%s' "$post_body" | jq -r '.accepted_from // "(unset)"')
DISPATCH_STATUS=$(printf '%s' "$post_body" | jq -r '.dispatch_status // "(unset)"')

echo "OK: POST /api/v1/jobs accepted"
echo "  http_status      : ${post_status}"
echo "  job_id           : ${JOB_ID}"
echo "  accepted_from    : ${ACCEPTED_FROM}"
echo "  dispatch_status  : ${DISPATCH_STATUS}"
echo "  status_url       : ${STATUS_URL}"
echo "  Location header  : ${LOCATION_HEADER:-(stripped by upstream gateway)}"

# ---- polling loop: GET /api/v1/jobs/{job_id} ------------------------------
# Exponential backoff 1s → 2s → 4s → 8s → 16s capped at JOBS_POLL_TIMEOUT_S.
# The cap protects against ANRs when the master is unresponsive (operator
# sees exit 7 in the CI console instead of a 5-minute wait).
echo "--- polling GET /api/v1/jobs/${JOB_ID} (timeout=${POLL_TIMEOUT}s) ---"

elapsed=0
sleep_s=1
last_status=""
last_body=""

while (( elapsed < POLL_TIMEOUT )); do
  sleep "$sleep_s"
  elapsed=$((elapsed + sleep_s))
  sleep_s=$(( sleep_s * 2 ))
  if (( sleep_s > 16 )); then sleep_s=16; fi

  GET_RC=0
  curl -sS -m 10 \
    -H "Authorization: Bearer ${M2M_BEARER}" \
    "${JOBS_URL}/${JOB_ID}" \
    -D "$TMP_HDRS" -o "$TMP_BODY" 2>"$TMP_TRACE" || GET_RC=$?

  if [[ $GET_RC -ne 0 ]]; then
    echo "FATAL: curl could not reach ${JOBS_URL}/${JOB_ID} during poll (exit=${GET_RC})" >&2
    exit 3
  fi
  get_status=$(awk 'NR==1 && $1 ~ /^[Hh][Tt][Tt][Pp]\// {print $2; exit}' "$TMP_HDRS")
  get_body=$(cat "$TMP_BODY")
  get_status_value=$(printf '%s' "$get_body" | jq -er '.status // empty') || true
  if [[ -n "$get_status_value" ]]; then
    last_status="$get_status_value"
    last_body="$get_body"
  fi

  case "$get_status_value" in
    SUCCEEDED)
      echo "OK: terminal state SUCCEEDED after ${elapsed}s"
      break
      ;;
    FAILED|CANCELLED)
      echo "FATAL: terminal state ${get_status_value} after ${elapsed}s" >&2
      echo "  Last response body:" >&2
      echo "$get_body" | sed 's/^/    /' >&2
      exit 6
      ;;
  esac

  if [[ "$get_status" != "200" ]]; then
    echo "FATAL: GET /api/v1/jobs/${JOB_ID} returned HTTP ${get_status}" >&2
    echo "  Response body:" >&2
    echo "$get_body" | sed 's/^/    /' >&2
    exit 8
  fi
done

if [[ "$last_status" != "SUCCEEDED" ]]; then
  echo "FATAL: polling exhausted after ${POLL_TIMEOUT}s without reaching terminal state" >&2
  echo "  Last observed status: ${last_status:-${get_status_value:-(none)}}" >&2
  echo "  Last response body:" >&2
  echo "$last_body" | sed 's/^/    /' >&2
  echo "  Hint: raise JOBS_POLL_TIMEOUT_S or check master log for the dispatched job." >&2
  exit 7
fi

# ---- success banner --------------------------------------------------------
STATUS=$(printf '%s' "$last_body" | jq -r '.status // "(unset)"')
CREATED=$(printf '%s' "$last_body" | jq -r '.created // "(unset)"')

echo "OK: jobs_smoke completed end-to-end"
echo "  job_id     : ${JOB_ID}"
echo "  status     : ${STATUS}"
echo "  created    : ${CREATED}"
echo "  elapsed_s  : ${elapsed}"
echo "  cleanup    : ephemeral M2M client ${PROVISIONED_CLIENT_ID} will be DELETED on exit"
exit 0
