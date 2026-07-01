#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PAYLOAD_FILE="${ROOT_DIR}/ops/jobs/jackie_chan_doc_voiceover.generate-from-clips.json"
MASTER_URL="${VELOX_MASTER_URL:-http://127.0.0.1:8000}"
ADMIN_TOKEN="${VELOX_ADMIN_TOKEN:-velox-dev-token}"

curl -sS -X POST "${MASTER_URL}/api/script/generate-from-clips" \
  -H "Authorization: Bearer ${ADMIN_TOKEN}" \
  -H "Content-Type: application/json" \
  --data-binary @"${PAYLOAD_FILE}"
