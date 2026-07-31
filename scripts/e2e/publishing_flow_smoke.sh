#!/usr/bin/env bash
# =============================================================================
# publishing_flow_smoke.sh — Cross-repo smoke for the Velox→InstaEdit
# publishing flow on main.
#
# Purpose:
#   1. Use $VELOX_ADMIN_TOKEN to mint an ephemeral M2M client via
#      POST /api/v1/admin/m2m/keys (operator-only admin surface).
#   2. POST /api/v1/publishing/targets with the M2M bearer; filter targets
#      by can_post=true AND capabilities.upload_video=true; capture the
#      canonical destination_id (Velox-side opaque id) and the
#      external_destination_id (InstaeditLogin opaque id).
#   3. POST /api/v1/jobs with delivery_plan[0].destination_id set to the
#      Velox-side destination_id captured in step 2 (no channel /
#      platform_account_id in metadata; the opaque id is authoritative).
#   4. Poll GET /api/v1/jobs/{job_id} with exponential backoff
#      (1s → 2s → 4s → 8s → 16s, capped at $PUBLISHING_POLL_TIMEOUT_S,
#      default 300 = 5 minutes) until SUCCEEDED.
#   5. Best-effort discover external_delivery_id from the polled job
#      response (deliveries[].remote_id / external_delivery_id / etc.
#      depending on the API version — operators adapt).
#   6. If $INSTAEDIT_BASE_URL is set: poll
#      GET ${INSTAEDIT_BASE_URL}/api/v1/integrations/velox/deliveries/{external_delivery_id}
#      until status=PRIVATE_UPLOADED (or until $PUBLISHING_PRIVATE_TIMEOUT_S
#      elapses, default 300s).
#   7. Cleanup: DELETE the ephemeral M2M key on every exit path.
#
# Cross-repo contract:
#   POST {VELOX_MASTER_URL}/api/v1/publishing/targets
#     Bearer: M2M (scope: jobs.submit)
#     Body:   { "workspace_id": <int64>, "platform": "youtube",
#               "platform_account_id": <int64> (optional filter) }
#     200     → { "workspace_id": ..., "platform": ..., "targets": [
#                   { "destination_id": "instaedit_...",
#                     "external_destination_id": "extdst_01J...",
#                     "platform_account_id": ..., "platform": "youtube",
#                     "channel_id": "UC...", "channel_name": "...",
#                     "status": "active|reauth_required|...",
#                     "enabled": true, "can_post": true,
#                     "capabilities": { "upload_video": true, ... }, ... }
#                 ] }
#     The script picks the FIRST target with can_post=true AND
#     capabilities.upload_video=true; if none exists, exit 5.
#
#   POST {VELOX_MASTER_URL}/api/v1/jobs
#     Bearer: same M2M
#     Body:   { "idempotency_key": "...", "video_name": "...", ...,
#               "delivery_plan": [ { "destination_id": "<velox-side id>", ... } ] }
#     202     → { "ok": true, "job_id": "job_...", "status_url": "..." }
#
#   GET  {VELOX_MASTER_URL}/api/v1/jobs/{job_id}
#     Bearer: same M2M
#     200     → { "ok": true, "job_id": "...", "status": "SUCCEEDED|FAILED|...",
#                 ... "remote_id" or "external_delivery_id" (best-effort) }
#
#   (Optional) GET {INSTAEDIT_BASE_URL}/api/v1/integrations/velox/deliveries/{external_delivery_id}
#     200     → { "external_delivery_id": "...", "status": "PRIVATE_UPLOADED|..." }
#
# Environment contract:
#   VELOX_MASTER_URL                 (optional) Default: http://127.0.0.1:8080
#   VELOX_ADMIN_TOKEN                (mandatory) Bearer for the admin surface.
#                                     Same dotenv precedence as
#                                     scripts/api/jobs_smoke.sh.
#   PUBLISHING_WORKSPACE_ID          (mandatory) int64 — workspace owning the
#                                     target YouTube channel.
#   PUBLISHING_PLATFORM              (optional) Default: youtube.
#   PUBLISHING_PLATFORM_ACCOUNT_ID   (optional) int64 scalar filter to one
#                                     channel — useful when the workspace
#                                     has hundreds of channels.
#   INSTAEDIT_BASE_URL               (optional) Base URL of the InstaeditLogin
#                                     instance. When unset the script
#                                     SKIPS step 1b (catalog GET via
#                                     /api/v1/integrations/velox/destinations)
#                                     AND step 6 (PRIVATE_UPLOADED
#                                     verification) and logs a notice.
#                                     Set INSTAEDIT_VELOX_USER_TOKEN as
#                                     well to enable step 1b (cross-validation
#                                     invariant S_velox ⊆ S_inst against
#                                     the InstaeditLogin catalog).
#   INSTAEDIT_VELOX_USER_TOKEN       (required when INSTAEDIT_BASE_URL is
#                                     set) Bearer for the InstaeditLogin
#                                     /api/v1/integrations/velox/destinations
#                                     endpoint. The destinations endpoint
#                                     requires a workspace-owner USER JWT;
#                                     the Velox admin token cannot be reused
#                                     here because the handler rejects
#                                     non-user identities via
#                                     adminIdentityUserID(req)==0 -> 401.
#                                     Mirror of the resolve_token() pattern
#                                     above; can also be sourced from a
#                                     TOKEN_FILE dotenv line
#                                     INSTAEDIT_VELOX_USER_TOKEN=...
#                                     When INSTAEDIT_BASE_URL is set but
#                                     INSTAEDIT_VELOX_USER_TOKEN is unset:
#                                     FATAL exit 2 (explicit misconfig).
#   PUBLISHING_POLL_TIMEOUT_S        (optional) Default: 300 (5 minutes —
#                                     the cross-repo flow includes a real
#                                     render + chunked upload).
#   PUBLISHING_PRIVATE_TIMEOUT_S     (optional) Default: 300 — the
#                                     private_upload verification poll.
#   PUBLISHING_IDEM_KEY              (optional) Override the canonical
#                                     smoke-<epoch>-<pid> idempotency_key.
#   PUBLISHING_FLOW_DEBUG            (optional) "1" → curl verbose + body dump.
#   PUBLISHING_FLOW_DRY              (optional) "1" → print the would-be
#                                     request bodies + chosen target, then
#                                     exit 0 without making any real HTTP
#                                     call (other than the admin-side M2M
#                                     provisioning which is currently
#                                     skipped in dry mode too — see note).
#
# Exit codes:
#   0  success — full chain reached SUCCEEDED (and PRIVATE_UPLOADED if
#      INSTAEDIT_BASE_URL was set AND remote_id was discoverable)
#   2  usage / env error (missing admin token, missing workspace id,
#      no curl/jq, INSTAEDIT_BASE_URL set without INSTAEDIT_VELOX_USER_TOKEN,
#      CR/LF in either token)
#   3  network error (curl could not reach Velox or InstaeditLogin)
#   4  HTTP non-200/202 on a Velox endpoint (M2M issuance, targets, jobs)
#      OR HTTP non-200 on the InstaeditLogin
#      /integrations/velox/destinations catalog GET (step 1b)
#   5  no publishable target found in /publishing/targets response
#      (can_post=true && capabilities.upload_video=true)
#   6  job reached SUCCEEDED but the API response was missing
#      `job_id` (202 envelope shape drift)
#   7  job reached FAILED or CANCELLED on /api/v1/jobs polling
#   8  polling timeout — neither reached a terminal state within
#      $PUBLISHING_POLL_TIMEOUT_S
#   9  PRIVATE_UPLOADED was not reached on InstaeditLogin side within
#      $PUBLISHING_PRIVATE_TIMEOUT_S (only checked when
#      $INSTAEDIT_BASE_URL is set)
#  10  catalog cross-validation failed — either the chosen Velox
#      destination_id does not start with "instaedit_" (per
#      publishing_targets.go::veloxDestinationID mapping contract),
#      OR (when step 1b ran) the Velox destination_id's suffix is
#      not present in the InstaeditLogin catalog for the requested
#      workspace. Indicates catalog drift between Velox and
#      InstaEdit.
#
# Usage:
#   PUBLISHING_WORKSPACE_ID=42 INSTAEDIT_BASE_URL=https://instaedit.example.com \
#     ./scripts/e2e/publishing_flow_smoke.sh
#   PUBLISHING_FLOW_DRY=1 PUBLISHING_WORKSPACE_ID=42 \
#     ./scripts/e2e/publishing_flow_smoke.sh
#   ./scripts/e2e/publishing_flow_smoke.sh --help
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib/publishing_flow_m2m.sh"
source "${SCRIPT_DIR}/lib/publishing_flow_catalog.sh"
source "${SCRIPT_DIR}/lib/publishing_flow_payload.sh"
source "${SCRIPT_DIR}/lib/publishing_flow_polling.sh"

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
# Mirrors scripts/creator_push_smoke.sh::resolve_token and
# scripts/api/jobs_smoke.sh::resolve_token so an operator can reuse one
# TOKEN_FILE across the three smokes.
ADMIN_TOKEN=$(resolve_token) || exit 2

# ---- mandatory: workspace_id ------------------------------------------------
if [[ -z "${PUBLISHING_WORKSPACE_ID:-}" ]]; then
  echo "FATAL: PUBLISHING_WORKSPACE_ID unset (mandatory int64)" >&2
  echo "  Remediation: export PUBLISHING_WORKSPACE_ID=12 (the test workspace id)" >&2
  exit 2
fi
if ! [[ "${PUBLISHING_WORKSPACE_ID}" =~ ^[0-9]+$ ]]; then
  echo "FATAL: PUBLISHING_WORKSPACE_ID must be a non-negative integer; got '${PUBLISHING_WORKSPACE_ID}'" >&2
  exit 2
fi

# ---- optional knobs --------------------------------------------------------
MASTER_URL="${VELOX_MASTER_URL:-http://127.0.0.1:8080}"
MASTER_URL="${MASTER_URL%/}"
PLATFORM="${PUBLISHING_PLATFORM:-youtube}"
PLATFORM_ACCOUNT_ID="${PUBLISHING_PLATFORM_ACCOUNT_ID:-}"
# Validate the optional scalar filter up-front so jq --argjson below is
# guaranteed a valid integer (or empty-string → null). The `${VAR:-null}`
# bash expansion already covers empty-string correctly, but non-integer
# values like "abc" / "1.5" / "-1" would otherwise surface as a cryptic
# mid-Phase-1 jq parse error; reject them with an operator-friendly FATAL.
if [[ -n "${PLATFORM_ACCOUNT_ID}" ]] && ! [[ "${PLATFORM_ACCOUNT_ID}" =~ ^[0-9]+$ ]]; then
  echo "FATAL: PUBLISHING_PLATFORM_ACCOUNT_ID (when set) must be a non-negative integer; got '${PLATFORM_ACCOUNT_ID}'" >&2
  exit 2
fi
INSTAEDIT_BASE_URL="${INSTAEDIT_BASE_URL:-}"
INSTAEDIT_BASE_URL="${INSTAEDIT_BASE_URL%/}"
DRY_MODE="${PUBLISHING_FLOW_DRY:-0}"

# ---- INST_VELOX_USER_TOKEN resolution (USER-JWT for InstaeditLogin) -------
# Required ONLY when INSTAEDIT_BASE_URL is set — the
# /integrations/velox/destinations handler requires a workspace-owner
# USER JWT (adminIdentityUserID(req)==0 → 401), so the Velox admin
# token cannot be reused. We mirror the resolve_token() dotenv
# precedence so an operator can reuse one TOKEN_FILE across the
# three smokes (publishing_flow, jobs_smoke, creator_push_smoke).
INST_USER_TOKEN=""
if [[ -n "$INSTAEDIT_BASE_URL" ]]; then
  if [[ -n "${INSTAEDIT_VELOX_USER_TOKEN:-}" ]]; then
    INST_USER_TOKEN="${INSTAEDIT_VELOX_USER_TOKEN}"
  elif [[ -n "${TOKEN_FILE:-}" && -r "${TOKEN_FILE}" ]]; then
    INST_USER_TOKEN=$(grep -E '^INSTAEDIT_VELOX_USER_TOKEN=' "${TOKEN_FILE}" | head -1 \
      | sed 's/^[^=]*=//' \
      | tr -d '"' | tr -d "'" | xargs || true)
  fi
  if [[ -z "$INST_USER_TOKEN" ]]; then
    echo "FATAL: INSTAEDIT_BASE_URL='${INSTAEDIT_BASE_URL}' but INSTAEDIT_VELOX_USER_TOKEN is unset" >&2
    echo "  Remediation: export INSTAEDIT_VELOX_USER_TOKEN=<workspace-owner USER JWT>" >&2
    echo "  Get a JWT via /api/v1/auth/login on the InstaEdit instance." >&2
    exit 2
  fi
  if [[ "$INST_USER_TOKEN" == *$'\r'* || "$INST_USER_TOKEN" == *$'\n'* ]]; then
    echo "FATAL: INSTAEDIT_VELOX_USER_TOKEN contains CR or LF; refusing to use it" >&2
    exit 2
  fi
fi
DEBUG_MODE="${PUBLISHING_FLOW_DEBUG:-0}"
POLL_TIMEOUT="${PUBLISHING_POLL_TIMEOUT_S:-300}"
PRIVATE_TIMEOUT="${PUBLISHING_PRIVATE_TIMEOUT_S:-300}"

# ---- temp files + cleanup trap --------------------------------------------
TMP_BODY=$(mktemp)
TMP_HDRS=$(mktemp)
TMP_TRACE=$(mktemp)

ADMIN_KEYS_URL="${MASTER_URL}/api/v1/admin/m2m/keys"
TARGETS_URL="${MASTER_URL}/api/v1/publishing/targets"
JOBS_URL="${MASTER_URL}/api/v1/jobs"

# Track cleanup state.
M2M_BEARER=""
PROVISIONED_CLIENT_ID=""
trap 'rm -f "$TMP_BODY" "$TMP_HDRS" "$TMP_TRACE"; cleanup' EXIT

# ---- ephemeral M2M client provisioning --------------------------------------
EPOCH=$(date +%s)
CLIENT_ID="pub-smoke-${EPOCH}-$$"
IDEM_KEY="${PUBLISHING_IDEM_KEY:-pub-smoke-${EPOCH}-$$}"

ISSUE_REQ=$(cat <<JSON
{
  "client_id": "${CLIENT_ID}",
  "description": "publishing_flow_smoke ephemeral client (auto-cleaned)",
  "scopes": ["jobs.submit"],
  "rate_limit_rps": 5,
  "rate_limit_burst": 10,
  "quota_max_scenes": 100,
  "quota_max_total_secs": 600
}
JSON
)

# post_admin_issue() — POST the ephemeral M2M client to /api/v1/admin/m2m/keys.
# MUST include "${ADMIN_KEYS_URL}" as the URL argument; if omitted, curl
# exits 2 ("Failed to initialize") with no body and the smoke misreports
# "curl could not reach" — that was the bug in commit 15aa687, fixed here.

# DRY mode short-circuits the M2M provisioning too — operators want a
# pure-print experience without leaving artifacts. The dry banner prints
# the request bodies + the would-be M2M client_id.
if [[ "$DRY_MODE" == "1" ]]; then
  print_dry_run
  exit 0
fi

echo "--- provisioning ephemeral M2M client: ${CLIENT_ID} ---"
if ! post_admin_issue; then
  echo "FATAL: curl could not reach ${ADMIN_KEYS_URL} during M2M provisioning" >&2
  exit 3
fi
issue_status=$(awk 'NR==1 && $1 ~ /^[Hh][Tt][Tt][Pp]// {print $2; exit}' "$TMP_HDRS")
issue_body=$(cat "$TMP_BODY")

# 409 retry-on-collision (concurrent re-runs in the same second):
# canonical idempotency path on the admin surface. The previous sed-based
# rewrite (`s/${EPOCH})/...`) was structurally wrong because the literal
# pattern `EPOCH)` doesn't appear in the canonical JSON (only the outer
# `.client_id` string carries the unique suffix). Use a full-string
# replacement on `${CLIENT_ID}` instead.
if [[ "$issue_status" == "409" || "$issue_status" == "400" ]]; then
  OLD_CLIENT_ID="${CLIENT_ID}"
  CLIENT_ID="pub-smoke-$((${EPOCH}+1))-$$"
  ISSUE_REQ="${ISSUE_REQ//${OLD_CLIENT_ID}/${CLIENT_ID}}"
  if ! post_admin_issue; then
    echo "FATAL: retry of M2M provisioning also failed at network level" >&2
    exit 3
  fi
  issue_status=$(awk 'NR==1 && $1 ~ /^[Hh][Tt][Tt][Pp]// {print $2; exit}' "$TMP_HDRS")
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
  exit 5
fi
PROVISIONED_CLIENT_ID="$CLIENT_ID"
echo "  client_id        : ${PROVISIONED_CLIENT_ID}"
echo "  plaintext_secret : ********** (redacted)"

publishing_fetch_inst_catalog
build_targets_payload
publishing_discover_target

# ---- POST /api/v1/jobs ----------------------------------------------------
# The metadata block is the JOB-side pass-through blob that the Velox
# worker forwards to InstaeditLogin at POST /internal/v1/deliveries.
# Per the user's spec the metadata MUST pin contract_version="velox.instaedit.publish.v1"
# (this is the JOB-side discriminator literal — the InstaeditLogin
# /internal/v1/deliveries CONTRACT discriminator is a separate value,
# `ContractVersionV1 = "velox-instaedit.v1"` (dashes), used at the
# worker hand-off boundary; both literals coexist because they tag
# different wire boundaries on the cross-repo flow). All other fields
# are hardcoded per the user spec: privacy_status=private,
# final_privacy=public, require_thumbnail=true.


build_jobs_payload
echo "--- POST /api/v1/jobs (idempotency_key=${IDEM_KEY}) ---"
CURL_RC=0
curl -sS -m 30 -X POST \
  -H "Authorization: Bearer ${M2M_BEARER}" \
  -H "Content-Type: application/json" \
  -H "X-Request-ID: pub-smoke-${EPOCH}-jobs" \
  --data-raw "$JOBS_PAYLOAD" \
  "${JOBS_URL}" \
  -D "$TMP_HDRS" -o "$TMP_BODY" 2>"$TMP_TRACE" || CURL_RC=$?
if [[ $CURL_RC -ne 0 ]]; then
  echo "FATAL: curl could not reach ${JOBS_URL} (exit=${CURL_RC})" >&2
  exit 3
fi
post_status=$(awk 'NR==1 && $1 ~ /^[Hh][Tt][Tt][Pp]// {print $2; exit}' "$TMP_HDRS")
post_body=$(cat "$TMP_BODY")
if [[ "${DEBUG_MODE}" == "1" ]]; then
  echo "DEBUG: /api/v1/jobs response:" >&2
  echo "$post_body" | jq . >&2
fi
if [[ "$post_status" != "202" ]]; then
  echo "FATAL: expected HTTP 202 but got ${post_status}" >&2
  echo "  Endpoint: POST ${JOBS_URL}" >&2
  echo "  Response body:" >&2
  echo "$post_body" | sed 's/^/    /' >&2
  exit 4
fi

JOB_ID=$(printf '%s' "$post_body" | jq -er '.job_id // empty') || JOB_ID=""
if [[ -z "$JOB_ID" ]]; then
  echo "FATAL: HTTP 202 received but job_id is missing or empty" >&2
  echo "  Response body:" >&2
  echo "$post_body" | sed 's/^/    /' >&2
  exit 6
fi
ACCEPTED_FROM=$(printf '%s' "$post_body" | jq -r '.accepted_from // "(unset)"')
DISPATCH_STATUS=$(printf '%s' "$post_body" | jq -r '.dispatch_status // "(unset)"')
echo "OK: /api/v1/jobs accepted"
echo "  job_id           : ${JOB_ID}"
echo "  accepted_from    : ${ACCEPTED_FROM}"
echo "  dispatch_status  : ${DISPATCH_STATUS}"

poll_job
discover_external_delivery_id
poll_private_delivery

# ---- summary ---------------------------------------------------------------
FINAL_JOB_STATUS=$(printf '%s' "$last_body" | jq -r '.status // "(unset)"')
echo "OK: publishing_flow_smoke completed end-to-end"
echo "  job_id                 : ${JOB_ID}"
echo "  final_job_status       : ${FINAL_JOB_STATUS}"
echo "  destination_id (Velox) : ${DESTINATION_ID}"
if [[ -n "${EXTERNAL_DESTINATION_ID:-}" ]]; then
  echo "  external_destination_id (InstaEdit pre-mapping): ${EXTERNAL_DESTINATION_ID}"
fi
if [[ -n "${EXTERNAL_DELIVERY_ID:-}" ]]; then
  echo "  external_delivery_id (InstaEdit post-render)   : ${EXTERNAL_DELIVERY_ID}"
fi
if [[ -n "$INSTAEDIT_BASE_URL" && -n "${EXTERNAL_DELIVERY_ID:-}" ]]; then
  echo "  private verification   : reached PRIVATE_UPLOADED"
fi
echo "  cleanup                : ephemeral M2M client ${PROVISIONED_CLIENT_ID} will be DELETED on exit"
exit 0
