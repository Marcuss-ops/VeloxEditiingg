#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REGISTRY="$ROOT_DIR/cases/registry.json"

command -v jq >/dev/null || { echo "jq is required" >&2; exit 2; }
jq empty "$REGISTRY"

while IFS= read -r case_id; do
  payload=$(jq -r --arg id "$case_id" '.cases[] | select(.id == $id) | .payload' "$REGISTRY")
  case_dir="$ROOT_DIR/cases"
  payload_path="$case_dir/$payload"
  [[ -r "$payload_path" ]] || { echo "missing payload for $case_id: $payload_path" >&2; exit 1; }
  jq empty "$payload_path"
  echo "PASS $case_id $payload_path"
done < <(jq -r '.cases[].id' "$REGISTRY")
