#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PAYLOAD_FILE="${ROOT_DIR}/ops/jobs/roman_aqueducts_city_engineering.fixed-job.json"
MASTER_URL="${VELOX_MASTER_URL:-http://127.0.0.1:8000}"
ADMIN_TOKEN="${VELOX_ADMIN_TOKEN:-velox-dev-token}"
VOICEOVER_URL="${1:-${VELOX_ROMAN_VOICEOVER_URL:-}}"
DRIVE_OUTPUT_FOLDER="${2:-${VELOX_ROMAN_DRIVE_OUTPUT_FOLDER:-}}"

if [[ -z "${VOICEOVER_URL}" ]]; then
  echo "usage: $0 <full_voiceover_url>" >&2
  echo "optional: $0 <full_voiceover_url> <drive_output_folder>" >&2
  echo "or set VELOX_ROMAN_VOICEOVER_URL" >&2
  echo "and optionally VELOX_ROMAN_DRIVE_OUTPUT_FOLDER" >&2
  exit 1
fi

jq --arg voiceover "${VOICEOVER_URL}" \
   --arg drive_output_folder "${DRIVE_OUTPUT_FOLDER}" \
   '.voiceover_path = $voiceover
    | if $drive_output_folder != "" then .drive_output_folder = $drive_output_folder else . end' "${PAYLOAD_FILE}" \
  | curl -sS -X POST "${MASTER_URL}/api/script/generate-with-images" \
      -H "Authorization: Bearer ${ADMIN_TOKEN}" \
      -H "Content-Type: application/json" \
      --data @-
