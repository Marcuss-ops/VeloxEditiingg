#!/usr/bin/env bash
# =============================================================================
# creator_push_smoke.sh — Creator-push endpoint smoke test.
#
# Purpose:
#   POST a canonical Creator scenario (voiceover + stock + clip + scene) to
#   /api/v1/creator/jobs using the operator admin bearer, then verify the
#   endpoint returned HTTP 202 and that the response body contains a populated
#   `job_id`. Exits 0 on success, non-zero with a banner on any failure.
#
# Endpoint contract (VeloxEditiingg):
#   POST {VELOX_MASTER_URL}/api/v1/creator/jobs
#     Authorization: Bearer ${VELOX_ADMIN_TOKEN}
#     Content-Type:  application/json
#   Expected: HTTP 202 Accepted with JSON body { ok, accepted_from, job_id, ... }
#
# Environment contract:
#   VELOX_ADMIN_TOKEN   (mandatory) Bearer for AdminAuthMiddleware. MUST NOT be
#                       logged or echoed. Source precedence:
#                         1. VELOX_ADMIN_TOKEN env var
#                         2. TOKEN_FILE env var (path to dotenv with KEY=VALUE)
#                       Either is sufficient; both unset → exit 2.
#   VELOX_MASTER_URL    (optional) Base URL of the Velox server.
#                       Default: http://127.0.0.1:8080
#   CREATOR_PUSH_DEBUG  (optional) If set to "1", curl emits verbose trace and
#                       the raw response body is dumped to ${CREATOR_PUSH_DEBUG_LOG}.
#
# Exit codes:
#   0  success — HTTP 202 AND job_id populated
#   2  usage / env error (missing token, unknown flag, no curl/jq)
#   3  network error (curl could not reach server)
#   4  HTTP non-202 (server reached but rejected — 401, 422, 5xx, ...)
#   5  HTTP 202 but response body does not carry a populated job_id
#
# Usage:
#   ./scripts/creator_push_smoke.sh                       # default URL
#   VELOX_MASTER_URL=https://velox.example.com ./scripts/creator_push_smoke.sh
#   ./scripts/creator_push_smoke.sh --help
#
# The example payload is intentionally identical to the one used in
# docs/CREATOR-PUSH.md and DataServer/internal/handlers/server/pipeline/
# creator_push_e2e_test.go so that the smoke + the E2E suite exercise the same
# canonical Creator scenario.
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

# ---- token resolution (env > TOKEN_FILE dotenv) -----------------------------
resolve_token() {
  local value=""
  if [[ -n "${VELOX_ADMIN_TOKEN:-}" ]]; then
    value="$VELOX_ADMIN_TOKEN"
  elif [[ -n "${TOKEN_FILE:-}" && -r "${TOKEN_FILE}" ]]; then
    # Use sed to keep everything from the first '=' to end-of-line. The previous
    # `cut -d= -f2-` form truncated at the first '=', which silently broke any
    # base64-padded token (or any token containing '=' as a literal byte).
    value=$(grep -E '^VELOX_ADMIN_TOKEN=' "$TOKEN_FILE" | head -1 \
      | sed 's/^[^=]*=//' \
      | tr -d '"' | tr -d "'" | xargs || true)
  fi
  if [[ -z "$value" ]]; then
    echo "FATAL: VELOX_ADMIN_TOKEN unset and TOKEN_FILE not provided / unreadable" >&2
    echo "  Remediation: export VELOX_ADMIN_TOKEN=... or set TOKEN_FILE=/path/to/.env" >&2
    return 1
  fi
  # Reject multi-line tokens (CRLF would otherwise let a corrupted .env inject
  # extra HTTP headers via the Authorization line).
  if [[ "$value" == *$'\r'* || "$value" == *$'\n'* ]]; then
    echo "FATAL: VELOX_ADMIN_TOKEN contains CR or LF; refusing to use it" >&2
    return 1
  fi
  printf '%s' "$value"
}

ADMIN_TOKEN=$(resolve_token) || exit 2

# ---- server URL -------------------------------------------------------------
MASTER_URL="${VELOX_MASTER_URL:-http://127.0.0.1:8080}"
# strip trailing slash to keep URL building deterministic
MASTER_URL="${MASTER_URL%/}"
ENDPOINT="${MASTER_URL}/api/v1/creator/jobs"

# ---- payload ----------------------------------------------------------------
# Canonical Creator scenario (mirrors docs/CREATOR-PUSH.md + the E2E test).
# Source_provider identifies the Creator host; source_job_id is the upstream
# Creator-side job id the worker can correlate against. The Resolver canonical
# identity is (source_provider, source_job_id, target_executor_id).
PAYLOAD=$(cat <<'JSON'
{
  "source_provider": "creator_pc_1",
  "source_job_id": "creator-job-001",
  "target_executor_id": "scene.composite.v1",
  "payload": {
    "status": "completed",
    "job_id": "creator-job-001",
    "video_name": "Smoke-test video",
    "script_text": "Creator-push smoke test: voiceover + stock + clip + scene scenario.",
    "scenes": [
      {
        "text": "Smoke scene 1",
        "duration_seconds": 5,
        "clip": {"url": "velox-asset://clips/smoke-clip-01.mp4", "duration_ms": 5000},
        "voiceover": {"url": "velox-asset://voiceovers/smoke-audio.mp3", "duration_ms": 5000}
      },
      {
        "text": "Smoke scene 2",
        "duration_seconds": 5,
        "clip": {"url": "velox-asset://clips/smoke-clip-02.mp4", "duration_ms": 5000},
        "voiceover": {"url": "velox-asset://voiceovers/smoke-audio.mp3", "duration_ms": 5000}
      }
    ],
    "delivery_plan": [
      { "destination_id": "drive", "priority": 1, "retry_budget": 3 }
    ]
  }
}
JSON
)

# ---- curl invocation --------------------------------------------------------
# -sS   : silent-on-progress, still print errors
# -D    : dump response headers to TMP_HDRS (canonical status extraction; avoids
#          string-marker collisions in the body)
# --data-raw : send the JSON body verbatim (unlike --data, no URL-encoding of
#              & and friends that would silently mangle future payloads)
# -m 30 : hard timeout so a hung server can't pin the script
CURL_OPTS=(-sS -m 30 -X POST \
  -H "Authorization: Bearer ${ADMIN_TOKEN}" \
  -H "Content-Type: application/json" \
  --data-raw "${PAYLOAD}" \
  "${ENDPOINT}")

if [[ "${CREATOR_PUSH_DEBUG:-0}" == "1" ]]; then
  echo "DEBUG: POST ${ENDPOINT}" >&2
  echo "DEBUG: Authorization: Bearer ********** (redacted)" >&2
  CURL_OPTS=(-v "${CURL_OPTS[@]}")
fi

# Capture body, headers, and curl trace in separate files (set -e friendly,
# no subshell). TMP_TRACE keeps the verbose -v output from being mixed with
# the JSON response body in DEBUG mode.
TMP_BODY=$(mktemp)
TMP_HDRS=$(mktemp)
TMP_TRACE=$(mktemp)
trap 'rm -f "$TMP_BODY" "$TMP_HDRS" "$TMP_TRACE"' EXIT

# Capture curl's exit code explicitly: the `|| CURL_RC=$?` is required because
# `set -e` would otherwise abort the script before we can read $?. A future
# refactor that drops the `||` would reintroduce the bug silently.
CURL_RC=0
curl "${CURL_OPTS[@]}" -D "$TMP_HDRS" -o "$TMP_BODY" 2>"$TMP_TRACE" || CURL_RC=$?

if [[ "${CREATOR_PUSH_DEBUG:-0}" == "1" ]]; then
  echo "DEBUG: response headers:" >&2
  cat "$TMP_HDRS" >&2
  echo "DEBUG: response body:" >&2
  cat "$TMP_BODY" >&2
  echo "DEBUG: curl trace (stderr):" >&2
  cat "$TMP_TRACE" >&2
fi

# Network-level failure (server unreachable, DNS, TLS handshake, timeout).
if [[ $CURL_RC -ne 0 ]]; then
  echo "FATAL: curl could not reach ${ENDPOINT} (exit=${CURL_RC})" >&2
  echo "  Hints:" >&2
  echo "    - is the Velox server running and listening on ${MASTER_URL}?" >&2
  echo "    - is VELOX_MASTER_URL set correctly?" >&2
  exit 3
fi

# Extract HTTP status from the response status line (HTTP/1.1 202 Accepted
# or HTTP/2 202). The regex is case-insensitive because some proxies / load
# balancers / mitm layers normalize the protocol token to lowercase, which
# would otherwise produce an empty $HTTP_STATUS and an exit-3 false positive.
HTTP_STATUS=$(awk 'NR==1 && $1 ~ /^[Hh][Tt][Tt][Pp]\// {print $2; exit}' "$TMP_HDRS")
BODY=$(cat "$TMP_BODY")

if [[ -z "$HTTP_STATUS" ]]; then
  echo "FATAL: could not parse HTTP status from response headers" >&2
  echo "  Raw headers (first 5 lines):" >&2
  head -5 "$TMP_HDRS" >&2
  exit 3
fi

if [[ "$HTTP_STATUS" != "202" ]]; then
  echo "FATAL: expected HTTP 202 but got ${HTTP_STATUS}" >&2
  echo "  Endpoint: ${ENDPOINT}" >&2
  echo "  Response body:" >&2
  echo "$BODY" | sed 's/^/    /' >&2
  if [[ "$HTTP_STATUS" == "401" ]]; then
    echo "  Hint: VELOX_ADMIN_TOKEN mismatch with server cfg.Auth.AdminToken." >&2
  fi
  exit 4
fi

# ---- assert job_id populated ------------------------------------------------
# jq -e makes the pipeline exit non-zero when the last value is null/false.
# The `// empty` filter then turns null into empty output, which still trips
# `-e`. We capture the rc explicitly so a JSON parse error surfaces a distinct
# banner from a missing-field error.
JOB_ID=""
JOB_ID_RC=0
JOB_ID=$(printf '%s' "$BODY" | jq -er '.job_id // empty') || JOB_ID_RC=$?

if [[ $JOB_ID_RC -ne 0 ]]; then
  if [[ -z "$JOB_ID" ]]; then
    echo "FATAL: HTTP 202 received but response body is not valid JSON" >&2
    echo "  jq rc=${JOB_ID_RC} (likely a parse error)" >&2
    echo "  Response body:" >&2
    echo "$BODY" | sed 's/^/    /' >&2
  else
    echo "FATAL: HTTP 202 received but job_id is missing or empty in response body" >&2
    echo "  Response body:" >&2
    echo "$BODY" | sed 's/^/    /' >&2
  fi
  exit 5
fi

# ---- success banner ---------------------------------------------------------
STATUS=$(printf '%s' "$BODY" | jq -r '.status // "(unset)"')
ACCEPTED_FROM=$(printf '%s' "$BODY" | jq -r '.accepted_from // "(unset)"')
DISPATCH_STATUS=$(printf '%s' "$BODY" | jq -r '.dispatch_status // "(unset)"')

echo "OK: /api/v1/creator/jobs accepted the Creator push"
echo "  endpoint       : ${ENDPOINT}"
echo "  http_status    : ${HTTP_STATUS}"
echo "  job_id         : ${JOB_ID}"
echo "  status         : ${STATUS}"
echo "  accepted_from  : ${ACCEPTED_FROM}"
echo "  dispatch_status: ${DISPATCH_STATUS}"
exit 0
