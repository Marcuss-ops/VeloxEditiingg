#!/usr/bin/env bash
set -euo pipefail

MASTER_URL="${MASTER_URL:-http://51.91.11.36:8000}"
ADMIN_TOKEN="${ADMIN_TOKEN:-}"

if [[ -z "${ADMIN_TOKEN}" ]]; then
  echo "ADMIN_TOKEN is required" >&2
  exit 1
fi

WORKER_IDS=(
  "host_149_56_131_97"
  "host_51_222_204_158"
  "host_host_57_129_132_133"
)

payload=$(cat <<JSON
{
  "worker_ids": [
    "host_149_56_131_97",
    "host_51_222_204_158",
    "host_host_57_129_132_133"
  ],
  "command": "run_smoke_job",
  "exclude_revoked": true
}
JSON
)

echo "Submitting smoke command to: ${WORKER_IDS[*]}"
curl -sS -X POST "${MASTER_URL}/api/v1/workers/send_command_bulk" \
  -H "Authorization: Bearer ${ADMIN_TOKEN}" \
  -H "Content-Type: application/json" \
  -d "${payload}"
echo
echo
echo "Current worker status:"
curl -sS "${MASTER_URL}/api/v1/workers_status" \
  -H "Authorization: Bearer ${ADMIN_TOKEN}"
echo
