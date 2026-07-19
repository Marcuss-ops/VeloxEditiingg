#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PAYLOAD_FILE="${ROOT_DIR}/ops/jobs/jackie_chan_doc_voiceover.generate.json"
MASTER_URL="${VELOX_MASTER_URL}"
ADMIN_TOKEN="${VELOX_ADMIN_TOKEN}"

if [[ -z "${MASTER_URL}" || -z "${ADMIN_TOKEN}" ]]; then
  echo "FATAL: VELOX_MASTER_URL and VELOX_ADMIN_TOKEN must be set" >&2
  exit 1
fi

curl -sS --fail-with-body -X POST \
  "${MASTER_URL}/api/v1/script/generate" \
  -H "Authorization: Bearer ${ADMIN_TOKEN}" \
  -H "Content-Type: application/json" \
  --data-binary @"${PAYLOAD_FILE}"
