#!/usr/bin/env bash
# ops/jobs/submit_benchmark_five_boxers.sh
#
# Submit the official benchmark-five-boxers payload ("Five legendary boxers"):
# intro, five portraits, clips, stock and voiceover — the representative
# normal workload used as the daily real benchmark.
#
# The frozen payload (five_legendary_boxers_it.generate.json) already
# carries real asset references (velox-asset:// voiceovers + Drive stock
# links + background music), so no substitution is needed. It is submitted
# through the legacy generate endpoint, mirroring
# submit_jackie_chan_doc_voiceover_clips.sh.
#
# Env:
#   VELOX_MASTER_URL      (optional) default http://127.0.0.1:8000 (matches the
#                          master's default listen port; the M2M benchmark
#                          scripts default to 8080 only to mirror
#                          scripts/api/jobs_smoke.sh — set VELOX_MASTER_URL
#                          explicitly to the real master in both cases)
#   VELOX_ADMIN_TOKEN     (optional) default velox-dev-token
#
# Exit codes: 0 accepted (HTTP 2xx), 1 curl error, 2 non-2xx response,
#             3 usage/env error.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PAYLOAD_FILE="${SCRIPT_DIR}/five_legendary_boxers_it.generate.json"
MASTER_URL="${VELOX_MASTER_URL:-http://127.0.0.1:8000}"
ADMIN_TOKEN="${VELOX_ADMIN_TOKEN:-velox-dev-token}"

if [[ ! -r "${PAYLOAD_FILE}" ]]; then
  echo "FATAL: payload not found: ${PAYLOAD_FILE}" >&2
  exit 3
fi

echo "→ POST ${MASTER_URL}/api/v1/script/generate (benchmark-five-boxers)"
resp_body="$(mktemp)"
resp_hdrs="$(mktemp)"
trap 'rm -f "${resp_body}" "${resp_hdrs}"' EXIT

if ! curl -sS --fail-with-body -m 60 -X POST \
  -H "Authorization: Bearer ${ADMIN_TOKEN}" \
  -H "Content-Type: application/json" \
  --data-binary @"${PAYLOAD_FILE}" \
  -D "${resp_hdrs}" -o "${resp_body}" 2>&1 \
  "${MASTER_URL}/api/v1/script/generate"; then
  echo "FAIL benchmark-five-boxers: curl/HTTP error" >&2
  cat "${resp_body}" 2>/dev/null || true
  exit 1
fi

http_status="$(awk 'NR==1 && $1 ~ /^[Hh][Tt][Tt][Pp]\// {print $2; exit}' "${resp_hdrs}" || true)"
echo "OK: HTTP ${http_status:-?}"
cat "${resp_body}"
echo
echo "PASS benchmark-five-boxers: accepted (job_id above; poll via master API if needed)"
exit 0
