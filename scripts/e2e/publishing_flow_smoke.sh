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
cleanup() {
  if [[ -n "$PROVISIONED_CLIENT_ID" && -n "$ADMIN_TOKEN" ]]; then
    # Best-effort DELETE; never let cleanup mask the original exit code.
    curl -sS -m 5 -X DELETE \
      -H "Authorization: Bearer ${ADMIN_TOKEN}" \
      "${ADMIN_KEYS_URL}/${PROVISIONED_CLIENT_ID}" >/dev/null 2>&1 || true
  fi
}
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
post_admin_issue() {
  local rc=0
  curl -sS -m 15 -X POST -H "Authorization: Bearer ${ADMIN_TOKEN}" \
    -H "Content-Type: application/json" \
    --data-raw "$ISSUE_REQ" \
    "${ADMIN_KEYS_URL}" \
    -D "$TMP_HDRS" -o "$TMP_BODY" 2>"$TMP_TRACE" || rc=$?
  return $rc
}

# DRY mode short-circuits the M2M provisioning too — operators want a
# pure-print experience without leaving artifacts. The dry banner prints
# the request bodies + the would-be M2M client_id.
if [[ "$DRY_MODE" == "1" ]]; then
  echo "[DRY] /api/v1/admin/m2m/keys POST would carry:" >&2
  echo "$ISSUE_REQ" >&2
  if [[ -n "$INSTAEDIT_BASE_URL" && -n "$INST_USER_TOKEN" ]]; then
    cat <<'DRYINNER' >&2
[DRY] /api/v1/integrations/velox/destinations GET would target:
[DRY]   ${INSTAEDIT_BASE_URL}/api/v1/integrations/velox/destinations?workspace_id=${PUBLISHING_WORKSPACE_ID}
[DRY]   Bearer: <USER-JWT, redacted>  (workspace-owner)
[DRY]   expected shape: { destinations: [ { external_destination_id, platform_account_id, status, ... } , ... ] }
[DRY]   step 1b captures: S_inst := [external_destination_id for each enabled active row]
DRYINNER
  else
    echo "[DRY] (skipped) /api/v1/integrations/velox/destinations GET - INSTAEDIT_BASE_URL or INSTAEDIT_VELOX_USER_TOKEN unset" >&2
    echo "[DRY]   cross-validation degrades to one-sided shape check (destination_id STARTSWITH instaedit_)" >&2
  fi
  echo "[DRY] /api/v1/publishing/targets POST would carry:" >&2
  printf '{"workspace_id":%s,"platform":"%s"' \
    "${PUBLISHING_WORKSPACE_ID}" "${PLATFORM}" >&2
  if [[ -n "$PLATFORM_ACCOUNT_ID" ]]; then
    printf ',"platform_account_id":%s' "${PLATFORM_ACCOUNT_ID}" >&2
  fi
  printf '}\n' >&2
  echo "[DRY] step 2 captures: S_velox := [external_destination_id for each target row in response]" >&2
  echo "[DRY] step 2 will assert (invariant S_velox is-subset S_inst):" >&2
  echo "[DRY]   - chosen target destination_id STARTSWITH instaedit_" >&2
  echo "[DRY]   - exit 10 on STARTSWITH failure OR chosen-target suffix not in S_inst" >&2
  echo "[DRY]   - WARN line surfaces S_velox-S_inst diff across ALL targets (not just chosen)" >&2
  echo "[DRY] jobs POST would carry (delivery_plan[0].metadata per spec, jq-built for safety):" >&2
  jq -nc --arg idem "${IDEM_KEY}" --arg epoch "${EPOCH}" --arg dest "<velox-side-instaedit_...>" --arg contract "velox.instaedit.publish.v1" \
    '{
      idempotency_key: $idem,
      video_name: ("publishing_flow_smoke epoch=" + $epoch),
      script_text: "Smoke script for publishing flow E2E.",
      voiceover_paths: ["velox-asset://voiceovers/pub-smoke.mp3"],
      scenes: [ { text: "Smoke scene", clip_link: "velox-asset://clips/pub-smoke.mp4", duration_seconds: 3 } ],
      delivery_plan: [
        {
          destination_id: $dest,
          priority: 1,
          retry_budget: 1,
          metadata: {
            contract_version: $contract,
            title: ("Velox Publishing Smoke (epoch=" + $epoch + ")"),
            description: "Automated smoke script for cross-repo publishing flow.",
            tags: ["velox-smoke", "e2e", "publishing"],
            privacy_status: "private",
            final_privacy: "public",
            require_thumbnail: true
          }
        }
      ]
    }' >&2
  echo "[DRY] no live HTTP calls done; exit 0" >&2
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

# ---- Step 1b: GET /api/v1/integrations/velox/destinations on InstaeditLogin
# Optional, mirrors the PRIVATE_UPLOADED-check pattern below: when
# INSTAEDIT_BASE_URL / INST_USER_TOKEN is unset, skip with a notice
# AND downgrade the step-2 cross-validation to a one-sided shape check
# (destination_id STARTSWITH "instaedit_") — a fully stricter invariant
# BOTH-URL+TURN-OFF would have us fail; partial-degrade is the better
# caller experience and is documented in the exit-code table above.
S_INST=""
S_INST_COUNT=""
INST_CATALOG_STATUS=""
if [[ -n "$INSTAEDIT_BASE_URL" && -n "$INST_USER_TOKEN" ]]; then
  INST_CATALOG_URL="${INSTAEDIT_BASE_URL}/api/v1/integrations/velox/destinations?workspace_id=${PUBLISHING_WORKSPACE_ID}"
  echo "--- GET ${INST_CATALOG_URL} ---"
  CURL_RC=0
  curl -sS -m 15 \
    -H "Authorization: Bearer ${INST_USER_TOKEN}" \
    -H "X-Request-ID: pub-smoke-${EPOCH}-catalog" \
    "${INST_CATALOG_URL}" \
    -D "$TMP_HDRS" -o "$TMP_BODY" 2>"$TMP_TRACE" || CURL_RC=$?
  if [[ $CURL_RC -ne 0 ]]; then
    echo "FATAL: curl could not reach ${INST_CATALOG_URL} (exit=${CURL_RC})" >&2
    exit 3
  fi
  INST_CATALOG_STATUS=$(awk 'NR==1 && $1 ~ /^[Hh][Tt][Tt][Pp]// {print $2; exit}' "$TMP_HDRS")
  INST_CATALOG_BODY=$(cat "$TMP_BODY")
  if [[ "${DEBUG_MODE}" == "1" ]]; then
    echo "DEBUG: /integrations/velox/destinations response:" >&2
    echo "$INST_CATALOG_BODY" | jq . >&2
  fi
  if [[ "$INST_CATALOG_STATUS" != "200" ]]; then
    echo "FATAL: GET ${INST_CATALOG_URL} returned HTTP ${INST_CATALOG_STATUS}" >&2
    echo "  Hint: 401 → USER-JWT missing/invalid (adminIdentityUserID(req)==0);" >&2
    echo "        403 → workspace owned by another user (ownership check);" >&2
    echo "        400 → workspace_id query param missing or non-positive." >&2
    echo "  Response body:" >&2
    echo "$INST_CATALOG_BODY" | sed 's/^/    /' >&2
    exit 4
  fi
  # Capture active+enabled rows into a newline-separated set S_inst.
  # We DO NOT include include_disabled=true — the cross-validation target
  # is "what can the sender actually pick right now?", and disabled rows
  # are not selectable from the perspective of an active can_post target.
  if ! S_INST=$(printf '%s' "$INST_CATALOG_BODY" | jq -er '
    (.destinations // [])
    | map(select(
        (.status // "active") == "active"
        and ((.external_destination_id // "") | length) > 0
      ))
    | map(.external_destination_id)
    | join("\n")
  ' 2>/dev/null); then
    echo "WARN: jq parse of /integrations/velox/destinations body failed; treating S_inst as empty" >&2
    S_INST=""
  fi
  # `wc -l` on empty string yields 0; strip CR defensively.
  S_INST=$(printf '%s' "$S_INST" | tr -d '\r')
  if [[ -z "$S_INST" ]]; then
    S_INST_COUNT="0"
  else
    S_INST_COUNT=$(printf '%s\n' "$S_INST" | grep -c '^' || true)
  fi
  echo "OK: InstaeditLogin catalog returned (active enabled rows): ${S_INST_COUNT}"
  if [[ "${DEBUG_MODE}" == "1" && -n "$S_INST" ]]; then
    echo "DEBUG: S_inst external_destination_ids:" >&2
    printf '%s\n' "$S_INST" | sed 's/^/    /' >&2
  fi
else
  echo "NOTICE: INSTAEDIT_BASE_URL or INSTAEDIT_VELOX_USER_TOKEN unset —" \
       "skipping step 1b (InstaeditLogin catalog GET). Cross-validation" \
       "in step 2 degrades to one-sided shape check (destination_id STARTSWITH" \
       "'instaedit_' per publishing_targets.go::veloxDestinationID); suffix" \
       "membership in S_inst is left as a WARN." >&2
fi

# ---- POST /api/v1/publishing/targets -------------------------------------
TARGETS_PAYLOAD=$(jq -nc \
  --argjson ws "${PUBLISHING_WORKSPACE_ID}" \
  --arg platform "${PLATFORM}" \
  --argjson pa "${PLATFORM_ACCOUNT_ID:-null}" \
  '{workspace_id: $ws, platform: $platform}
   + (if $pa != null then {platform_account_id: $pa} else {} end)')

echo "--- POST /api/v1/publishing/targets (workspace_id=${PUBLISHING_WORKSPACE_ID}, platform=${PLATFORM}${PLATFORM_ACCOUNT_ID:+, platform_account_id=${PLATFORM_ACCOUNT_ID}}) ---"
CURL_RC=0
curl -sS -m 15 -X POST \
  -H "Authorization: Bearer ${M2M_BEARER}" \
  -H "Content-Type: application/json" \
  -H "X-Request-ID: pub-smoke-${EPOCH}" \
  --data-raw "$TARGETS_PAYLOAD" \
  "${TARGETS_URL}" \
  -D "$TMP_HDRS" -o "$TMP_BODY" 2>"$TMP_TRACE" || CURL_RC=$?
if [[ $CURL_RC -ne 0 ]]; then
  echo "FATAL: curl could not reach ${TARGETS_URL} (exit=${CURL_RC})" >&2
  exit 3
fi
targets_status=$(awk 'NR==1 && $1 ~ /^[Hh][Tt][Tt][Pp]// {print $2; exit}' "$TMP_HDRS")
targets_body=$(cat "$TMP_BODY")
if [[ "${DEBUG_MODE}" == "1" ]]; then
  echo "DEBUG: /publishing/targets response:" >&2
  echo "$targets_body" | jq . >&2
fi
if [[ "$targets_status" != "200" ]]; then
  echo "FATAL: POST /publishing/targets returned HTTP ${targets_status}" >&2
  echo "  Response body:" >&2
  echo "$targets_body" | sed 's/^/    /' >&2
  exit 4
fi

# Pick the first target satisfying can_post=true AND
# capabilities.upload_video=true. jq -e sets rc=1 when no match +
# .selected/0/null is emitted (depends on jq version), so we explicitly
# default to empty and check.
CHOSEN=$(printf '%s' "$targets_body" | jq -er '
  .targets // [] | map(select(
    (.can_post // false) == true
    and (.capabilities.upload_video // false) == true
  )) | (if length > 0 then .[0] else empty end)
' 2>/dev/null) || CHOSEN=""
if [[ -z "$CHOSEN" || "$CHOSEN" == "null" ]]; then
  echo "FATAL: no target with can_post=true AND capabilities.upload_video=true" >&2
  echo "  workspace_id=${PUBLISHING_WORKSPACE_ID} platform=${PLATFORM}" >&2
  echo "  Inspect the targets array above to find a can_post=false / reauth row." >&2
  exit 5
fi
DESTINATION_ID=$(printf '%s' "$CHOSEN" | jq -er '.destination_id // empty') || true
EXTERNAL_DESTINATION_ID=$(printf '%s' "$CHOSEN" | jq -er '.external_destination_id // empty') || true
CHANNEL_ID=$(printf '%s' "$CHOSEN" | jq -r '.channel_id // "(unset)"')
CHANNEL_NAME=$(printf '%s' "$CHOSEN" | jq -r '.channel_name // "(unset)"')
if [[ -z "$DESTINATION_ID" || "$DESTINATION_ID" == "null" ]]; then
  echo "FATAL: chosen target has empty destination_id — Velox-side catalog is malformed" >&2
  exit 5
fi
echo "OK: target selected"
echo "  channel_id             : ${CHANNEL_ID}"
echo "  channel_name           : ${CHANNEL_NAME}"
echo "  destination_id (Velox) : ${DESTINATION_ID}"
echo "  external_destination_id (InstaEdit): ${EXTERNAL_DESTINATION_ID:-"(unset)"}"

# ---- Cross-validation: chosen target shape + full S_velox-is-subset-S_inst ---
# Per the user spec + the production mapping contract
# (publishing_targets.go::veloxDestinationID), a Velox-side destination_id
# MUST be of the canonical form 'instaedit_<external_destination_id>'.
# We assert two layers:
#   (1) chosen target destination_id STARTSWITH "instaedit_" AND when
#       S_inst is known its suffix is in S_inst (the InstaeditLogin catalog);
#   (2) full-iteration WARN: all S_velox destination_id suffixes vs all
#       S_inst rows, surfacing any drift across the entire response.
#       WARN not FATAL so a transient async-disabled target does not fail.
EXPECTED_DIR_PREFIX="instaedit_"
if [[ "${DESTINATION_ID:0:${#EXPECTED_DIR_PREFIX}}" != "$EXPECTED_DIR_PREFIX" ]]; then
  echo "FATAL: chosen target destination_id='${DESTINATION_ID}' does not start with '${EXPECTED_DIR_PREFIX}' -- Velox mapping contract drift" >&2
  echo "  Expected: instaedit_<external_destination_id> per publishing_targets.go::veloxDestinationID" >&2
  exit 10
fi
S_VELOX_SUFFIX="${DESTINATION_ID#${EXPECTED_DIR_PREFIX}}"
if [[ -n "$S_INST" ]]; then
  if ! printf '%s\n' "$S_INST" | grep -Fxq "$S_VELOX_SUFFIX"; then
    echo "FATAL: chosen destination_id='${DESTINATION_ID}' has suffix '${S_VELOX_SUFFIX}' which is NOT in the InstaeditLogin catalog for workspace_id=${PUBLISHING_WORKSPACE_ID}" >&2
    echo "  S_inst (active enabled rows from /integrations/velox/destinations):" >&2
    printf '%s\n' "$S_INST" | sed 's/^/    /' >&2
    echo "  Hint: Velox is reporting a destination_id not known by InstaEdit -- possible catalog drift or stale snapshot." >&2
    exit 10
  fi
  echo "OK: chosen-target cross-validation passed -- Velox destination_id suffix is in the InstaeditLogin catalog"
elif [[ -n "$INST_CATALOG_STATUS" ]]; then
  echo "WARN: step 1b returned 200 but S_inst was empty -- cross-validation downgrades to one-sided shape check (destination_id STARTSWITH instaedit_)" >&2
else
  echo "WARN: step 1b was skipped -- only enforced destination_id STARTSWITH instaedit_ (single-sided shape check)" >&2
fi
# Full-iteration WARN: surface any external_destination_ids present in Velox
# /publishing/targets but absent from the InstaeditLogin catalog across ALL
# targets. Catches drift even when the can_post filter hides the drifted row.
if [[ -n "$S_INST" ]]; then
  S_VELOX_FULL=$(printf '%s' "$targets_body" | jq -er '
    (.targets // [])
    | map(
        if (.external_destination_id // "") != "" then .external_destination_id
        elif (.destination_id // "") | startswith("instaedit_") then
          .destination_id | sub("^instaedit_"; "")
        else "" end
      )
    | map(select(. != ""))
    | unique
    | join("\n")
  ' 2>/dev/null || echo "")
  S_INST_SORTED=$(printf '%s\n' "$S_INST" | sort -u || true)
  S_VELOX_SORTED=$(printf '%s\n' "$S_VELOX_FULL" | sort -u || true)
  if [[ -n "$S_VELOX_SORTED" ]]; then
    S_DIFF=$(comm -23 <(printf '%s\n' "$S_VELOX_SORTED") <(printf '%s\n' "$S_INST_SORTED") || true)
    if [[ -n "$S_DIFF" ]]; then
      echo "WARN: drift detected -- these external_destination_ids appear in Velox /publishing/targets but NOT in InstaeditLogin catalog:" >&2
      printf '%s\n' "$S_DIFF" | sed 's/^/    /' >&2
      echo "  (WARN not FATAL -- operator should investigate catalog drift)" >&2
    else
      echo "OK: full S_velox-is-subset-S_inst invariant holds (all Velox destination_id suffixes are in S_inst)"
    fi
  fi
fi

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
JOBS_PAYLOAD=$(jq -nc \
  --arg idem "${IDEM_KEY}" \
  --arg ts "publishing_flow_smoke epoch=${EPOCH}" \
  --arg dest "${DESTINATION_ID}" \
  --arg contract "velox.instaedit.publish.v1" \
  --arg title "Velox Publishing Smoke (epoch=${EPOCH})" \
  --arg desc "Automated smoke script for cross-repo publishing flow." \
  '{
    idempotency_key: $idem,
    video_name: $ts,
    script_text: "Smoke script for publishing flow E2E.",
    voiceover_paths: ["velox-asset://voiceovers/pub-smoke.mp3"],
    scenes: [
      {
        text: "Smoke scene",
        clip_link: "velox-asset://clips/pub-smoke.mp4",
        duration_seconds: 3
      }
    ],
    delivery_plan: [
      {
        destination_id: $dest,
        priority: 1,
        retry_budget: 1,
        metadata: {
          contract_version: $contract,
          title: $title,
          description: $desc,
          tags: ["velox-smoke", "e2e", "publishing"],
          privacy_status: "private",
          final_privacy: "public",
          require_thumbnail: true
        }
      }
    ]
  }')

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

# ---- polling loop ---------------------------------------------------------
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
  get_status=$(awk 'NR==1 && $1 ~ /^[Hh][Tt][Tt][Pp]// {print $2; exit}' "$TMP_HDRS")
  get_body=$(cat "$TMP_BODY")
  get_status_value=$(printf '%s' "$get_body" | jq -er '.status // empty') || true
  if [[ -n "$get_status_value" ]]; then
    last_status="$get_status_value"
    last_body="$get_body"
  fi
  if [[ "${DEBUG_MODE}" == "1" ]]; then
    echo "DEBUG: poll t=${elapsed}s status=${get_status_value:-(none)} (HTTP ${get_status})" >&2
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
      exit 7
      ;;
  esac
  if [[ "$get_status" != "200" ]]; then
    echo "FATAL: GET /api/v1/jobs/${JOB_ID} returned HTTP ${get_status}" >&2
    echo "  Response body:" >&2
    echo "$get_body" | sed 's/^/    /' >&2
    exit 4
  fi
done

if [[ "$last_status" != "SUCCEEDED" ]]; then
  echo "FATAL: polling exhausted after ${POLL_TIMEOUT}s without reaching terminal state" >&2
  echo "  Last observed status: ${last_status:-none}" >&2
  echo "  Hint: raise PUBLISHING_POLL_TIMEOUT_S or check master log." >&2
  exit 8
fi

# ---- best-effort: discover external_delivery_id (remote_id) --------------
# Operator-facing honesty: this smoke does NOT depend on a specific
# Velox enumeration endpoint. We mine the polled job body for any of the
# well-known field names a future API version might use; if none is
# present we log a notice and continue.
EXTERNAL_DELIVERY_ID=""
for field in external_delivery_id social_delivery_id remote_id delivery_id remoteId socialDeliveryId; do
  EXTERNAL_DELIVERY_ID=$(printf '%s' "$last_body" | jq -er --arg f "$field" '
    # Look in: top-level; .deliveries[].<field>; .job_deliveries[].<field>;
    # we use `paths` to scan the whole tree cheaply.
    [.. | objects | select(has($f)) | getpath([$f])] | (map(select(. != null and . != ""))[0] // empty)
  ' 2>/dev/null) || EXTERNAL_DELIVERY_ID=""
  if [[ -n "$EXTERNAL_DELIVERY_ID" && "$EXTERNAL_DELIVERY_ID" != "null" ]]; then
    echo "OK: external_delivery_id discovered via field '$field' : ${EXTERNAL_DELIVERY_ID}"
    break
  fi
done

# ---- PRIVATE_UPLOADED verification on InstaeditLogin side -----------------
if [[ -n "$INSTAEDIT_BASE_URL" ]]; then
  if [[ -z "$EXTERNAL_DELIVERY_ID" ]]; then
    echo "NOTICE: INSTAEDIT_BASE_URL is set but external_delivery_id cannot be" \
         "discovered from the polled Velox job body — skipping PRIVATE_UPLOADED check." >&2
    echo "  Possible cause: Velox /api/v1/jobs/{id} response shape hides the" \
         "remote_id; an operator may need to add a dedicated enumeration endpoint." >&2
  else
    DELIVERY_URL="${INSTAEDIT_BASE_URL}/api/v1/integrations/velox/deliveries/${EXTERNAL_DELIVERY_ID}"
    echo "--- polling GET ${DELIVERY_URL} (timeout=${PRIVATE_TIMEOUT}s) ---"
    elapsed=0
    sleep_s=1
    last_private_status=""
    last_private_body=""
    while (( elapsed < PRIVATE_TIMEOUT )); do
      sleep "$sleep_s"
      elapsed=$((elapsed + sleep_s))
      sleep_s=$(( sleep_s * 2 ))
      if (( sleep_s > 16 )); then sleep_s=16; fi
      PRIV_RC=0
      curl -sS -m 10 \
        "${DELIVERY_URL}" \
        -D "$TMP_HDRS" -o "$TMP_BODY" 2>"$TMP_TRACE" || PRIV_RC=$?
      if [[ $PRIV_RC -ne 0 ]]; then
        echo "FATAL: curl could not reach ${DELIVERY_URL} (exit=${PRIV_RC})" >&2
        exit 3
      fi
      priv_status=$(awk 'NR==1 && $1 ~ /^[Hh][Tt][Tt][Pp]// {print $2; exit}' "$TMP_HDRS")
      priv_body=$(cat "$TMP_BODY")
      if [[ "${DEBUG_MODE}" == "1" ]]; then
        echo "DEBUG: private poll t=${elapsed}s HTTP=${priv_status}" >&2
      fi
      if [[ "$priv_status" == "200" ]]; then
        priv_status_value=$(printf '%s' "$priv_body" | jq -er '.status // empty') || true
        if [[ -n "$priv_status_value" ]]; then
          last_private_status="$priv_status_value"
          last_private_body="$priv_body"
        fi
        case "$priv_status_value" in
          PRIVATE_UPLOADED)
            echo "OK: PRIVATE_UPLOADED reached on InstaeditLogin side after ${elapsed}s"
            break 2   # break out of nested while + outer scoping
            ;;
          PUBLISHED|THUMBNAIL_PENDING)
            # Success-on-the-way states — keep polling.
            ;;
          FAILED|CANCELLED)
            echo "FATAL: delivery state ${priv_status_value} on InstaeditLogin side" >&2
            echo "  Last response body:" >&2
            echo "$priv_body" | sed 's/^/    /' >&2
            exit 9
            ;;
        esac
      elif [[ "$priv_status" == "404" ]]; then
        # The endpoint hasn't surfaced this delivery yet; keep polling.
        :
      else
        echo "FATAL: GET ${DELIVERY_URL} returned HTTP ${priv_status}" >&2
        echo "  Response body:" >&2
        echo "$priv_body" | sed 's/^/    /' >&2
        exit 4
      fi
    done

    if [[ "$last_private_status" != "PRIVATE_UPLOADED" ]]; then
      echo "FATAL: PRIVATE_UPLOADED not reached on InstaeditLogin side after ${PRIVATE_TIMEOUT}s" >&2
      echo "  Last observed private status: ${last_private_status:-none}" >&2
      exit 9
    fi
  fi
else
  echo "NOTICE: INSTAEDIT_BASE_URL unset — skipping step 6 (PRIVATE_UPLOADED on" \
       "InstaeditLogin side). Velox SUCCEEDED is the canonical quality gate" \
       "for this smoke when running in master-only environments." >&2
fi

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
