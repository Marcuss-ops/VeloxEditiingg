#!/usr/bin/env bash
# ops/jobs/lib/benchmark-common.sh
#
# Shared plumbing for the official video-generator benchmark submit scripts
# (submit_benchmark_minimal.sh / heavy / pathological).
#
# Flow (mirrors scripts/api/jobs_smoke.sh):
#   1. Resolve VELOX_MASTER_URL + VELOX_ADMIN_TOKEN (env > TOKEN_FILE dotenv).
#   2. Mint an ephemeral M2M client via POST /api/v1/admin/m2m/keys.
#   3. POST /api/v1/jobs with the M2M plaintext secret as Bearer.
#   4. Poll GET /api/v1/jobs/{job_id} with exponential backoff until a
#      terminal state (SUCCEEDED / FAILED / CANCELLED).
#   5. Best-effort DELETE of the ephemeral M2M key on every exit path.
#
# Environment contract:
#   VELOX_MASTER_URL       (optional) default http://127.0.0.1:8080
#   VELOX_ADMIN_TOKEN      admin bearer; source precedence: env var then
#                          TOKEN_FILE dotenv (VELOX_ADMIN_TOKEN=... line).
#   VELOX_BENCHMARK_IDEM_KEY   (optional) idempotency-key override, e.g. for
#                          cold/warm cache runs ("benchmark-minimal-cold").
#   VELOX_BENCHMARK_POLL_TIMEOUT_S (optional) poll cap, default 300.
#   VELOX_BENCHMARK_DELIVERY_DESTINATION (optional) delivery_plan destination
#                          override. The frozen payloads carry
#                          destination_id "drive" (the operator's deployment
#                          must have that row in delivery_destinations and
#                          enabled = 1, otherwise enqueue rejects the job with
#                          DESTINATION_NOT_FOUND). Point this env var at a real
#                          destination in your deployment to override it.
#                          NOTE: delivery_plan is REQUIRED at enqueue — an
#                          empty value keeps the payload's frozen destination;
#                          there is deliberately no way to strip it.
#
# Exit-code contract (per benchmark script):
#   submit_and_poll returns 0 on SUCCEEDED, 1 on FAILED/CANCELLED,
#   2 on poll timeout, 3 on non-202 POST rejection, 4 on usage/env error.
set -euo pipefail

MASTER_URL="${VELOX_MASTER_URL:-http://127.0.0.1:8080}"
MASTER_URL="${MASTER_URL%/}"
POLL_TIMEOUT_S="${VELOX_BENCHMARK_POLL_TIMEOUT_S:-300}"
IDEM_KEY="${VELOX_BENCHMARK_IDEM_KEY:-}"
DELIVERY_DEST="${VELOX_BENCHMARK_DELIVERY_DESTINATION:-}"

ADMIN_TOKEN=""
M2M_BEARER=""
PROVISIONED_CLIENT_ID=""
_TMP_BODY=""
_TMP_HDRS=""
_TMP_TRACE=""

benchmark_fail() {
  printf 'FATAL: %s\n' "$*" >&2
  benchmark_cleanup
  exit 4
}

# resolve_admin_token: env var > TOKEN_FILE dotenv (mirrors jobs_smoke.sh).
benchmark_resolve_admin_token() {
  if [[ -n "${VELOX_ADMIN_TOKEN:-}" ]]; then
    ADMIN_TOKEN="$VELOX_ADMIN_TOKEN"
  elif [[ -n "${TOKEN_FILE:-}" && -r "${TOKEN_FILE}" ]]; then
    ADMIN_TOKEN="$(grep -E '^VELOX_ADMIN_TOKEN=' "$TOKEN_FILE" | head -1 \
      | sed 's/^[^=]*=//' | tr -d '"' | tr -d "'" | xargs || true)"
  fi
  if [[ -z "${ADMIN_TOKEN}" ]]; then
    benchmark_fail "VELOX_ADMIN_TOKEN unset and TOKEN_FILE not provided/unreadable"
  fi
  if [[ "${ADMIN_TOKEN}" == *$'\r'* || "${ADMIN_TOKEN}" == *$'\n'* ]]; then
    benchmark_fail "VELOX_ADMIN_TOKEN contains CR or LF; refusing to use it"
  fi
}

benchmark_cleanup() {
  if [[ -n "${PROVISIONED_CLIENT_ID}" && -n "${ADMIN_TOKEN}" ]]; then
    curl -sS -m 5 -X DELETE \
      -H "Authorization: Bearer ${ADMIN_TOKEN}" \
      "${MASTER_URL}/api/v1/admin/m2m/keys/${PROVISIONED_CLIENT_ID}" >/dev/null 2>&1 || true
    PROVISIONED_CLIENT_ID=""
  fi
  rm -f "${_TMP_BODY}" "${_TMP_HDRS}" "${_TMP_TRACE}" 2>/dev/null || true
}
trap benchmark_cleanup EXIT

# mint_m2m: provision an ephemeral M2M key with the jobs.submit scope.
benchmark_mint_m2m() {
  _TMP_BODY="$(mktemp)"
  _TMP_HDRS="$(mktemp)"
  _TMP_TRACE="$(mktemp)"

  local epoch client_id issue_req
  epoch="$(date +%s)"
  client_id="benchmark-cli-${epoch}-$$"
  issue_req=$(cat <<JSON
{
  "client_id": "${client_id}",
  "description": "video-generator benchmark ephemeral client (auto-cleaned)",
  "scopes": ["jobs.submit"],
  "rate_limit_rps": 5,
  "rate_limit_burst": 10,
  "quota_max_scenes": 1000,
  "quota_max_total_secs": 14400
}
JSON
)
  printf '→ minting ephemeral M2M client: %s\n' "${client_id}"
  if ! curl -sS -m 15 -X POST \
    -H "Authorization: Bearer ${ADMIN_TOKEN}" \
    -H "Content-Type: application/json" \
    --data-raw "$issue_req" \
    -D "$_TMP_HDRS" -o "$_TMP_BODY" 2>"$_TMP_TRACE" \
    "${MASTER_URL}/api/v1/admin/m2m/keys"; then
    benchmark_fail "curl could not reach ${MASTER_URL}/api/v1/admin/m2m/keys"
  fi

  local issue_status
  issue_status="$(awk 'NR==1 && $1 ~ /^[Hh][Tt][Tt][Pp]\// {print $2; exit}' "$_TMP_HDRS" || true)"
  if [[ "${issue_status}" != "201" ]]; then
    benchmark_fail "M2M provisioning returned HTTP ${issue_status:-?}: $(cat "$_TMP_BODY" 2>/dev/null | head -c 400 || true)"
  fi

  M2M_BEARER="$(jq -er '.plaintext_secret // empty' "$_TMP_BODY" 2>/dev/null || true)"
  if [[ -z "${M2M_BEARER}" ]]; then
    benchmark_fail "M2M provisioning returned 201 but plaintext_secret is missing"
  fi
  PROVISIONED_CLIENT_ID="${client_id}"
}

# substitute_placeholders: jq filter applied to the payload before POST.
# Override in the per-benchmark script BEFORE calling submit_and_poll.
benchmark_substitute_payload() {
  # Default: no asset substitution (pathological benchmark); override in
  # each script by redefining this function.
  cat "${BENCHMARK_PAYLOAD_FILE}"
}

# post_and_poll: POST the payload, extract job_id, poll to terminal state.
# Echoes the final status; returns the exit-code contract above.
benchmark_submit_and_poll() {
  local filter="." payload staged
  if [[ -n "${IDEM_KEY}" ]]; then
    filter="${filter} | .idempotency_key = \$idem"
  fi
  if [[ -n "${DELIVERY_DEST}" ]]; then
    # delivery_plan is REQUIRED at enqueue; only override the destination,
    # never strip it.
    filter="${filter} | .delivery_plan[0].destination_id = \$dest"
  fi

  payload="$(benchmark_substitute_payload)"
  staged="$(printf '%s' "${payload}" | jq --arg idem "${IDEM_KEY}" --arg dest "${DELIVERY_DEST}" "${filter}")"
  if [[ -z "${staged}" ]]; then
    benchmark_fail "jq substitution produced an empty payload (bad template?)"
  fi

  local post_status post_body job_id
  printf '→ POST %s/api/v1/jobs\n' "${MASTER_URL}"
  if ! curl -sS -m 30 -X POST \
    -H "Authorization: Bearer ${M2M_BEARER}" \
    -H "Content-Type: application/json" \
    --data-raw "${staged}" \
    -D "$_TMP_HDRS" -o "$_TMP_BODY" 2>"$_TMP_TRACE" \
    "${MASTER_URL}/api/v1/jobs"; then
    benchmark_fail "curl could not reach ${MASTER_URL}/api/v1/jobs"
  fi

  post_status="$(awk 'NR==1 && $1 ~ /^[Hh][Tt][Tt][Pp]\// {print $2; exit}' "$_TMP_HDRS" || true)"
  post_body="$(cat "$_TMP_BODY" 2>/dev/null || true)"
  if [[ "${post_status}" != "202" ]]; then
    printf 'REJECTED: POST /api/v1/jobs returned HTTP %s\n' "${post_status:-?}"
    printf '%s\n' "${post_body}" | head -c 2000
    printf '\n'
    return 3
  fi

  job_id="$(printf '%s' "${post_body}" | jq -er '.job_id // empty')" || benchmark_fail "202 response missing job_id"
  printf 'OK: job accepted job_id=%s\n' "${job_id}"

  local elapsed=0 sleep_s=1 get_status status_value last_body
  while (( elapsed < POLL_TIMEOUT_S )); do
    sleep "$sleep_s"
    elapsed=$((elapsed + sleep_s))
    sleep_s=$(( sleep_s * 2 ))
    if (( sleep_s > 16 )); then sleep_s=16; fi

    if ! curl -sS -m 10 \
      -H "Authorization: Bearer ${M2M_BEARER}" \
      "${MASTER_URL}/api/v1/jobs/${job_id}" \
      -D "$_TMP_HDRS" -o "$_TMP_BODY" 2>"$_TMP_TRACE"; then
      benchmark_fail "curl could not reach ${MASTER_URL}/api/v1/jobs/${job_id} during poll"
    fi
    get_status="$(awk 'NR==1 && $1 ~ /^[Hh][Tt][Tt][Pp]\// {print $2; exit}' "$_TMP_HDRS" || true)"
    if [[ "${get_status}" != "200" ]]; then
      printf 'POLL ERROR: HTTP %s\n' "${get_status:-?}"
      cat "$_TMP_BODY" 2>/dev/null | head -c 1000 || true
      printf '\n'
      return 8
    fi
    last_body="$(cat "$_TMP_BODY" 2>/dev/null || true)"
    status_value="$(printf '%s' "${last_body}" | jq -er '.status // empty' 2>/dev/null || true)"
    printf '  t=%ss status=%s\n' "${elapsed}" "${status_value:-(none)}"

    case "${status_value}" in
      SUCCEEDED) return 0 ;;
      FAILED|CANCELLED) return 1 ;;
    esac
  done

  printf 'TIMEOUT: no terminal state within %ss (last=%s)\n' "${POLL_TIMEOUT_S}" "${status_value:-(none)}"
  return 2
}
