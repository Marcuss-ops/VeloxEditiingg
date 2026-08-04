#!/usr/bin/env bash
# Download and verify one parallel-benchmark artifact.
# The benchmark harness supplies response_json and artifact_url; this wrapper
# owns no lifecycle, lease, placement, or cache behavior.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

JOB_ID=""
RESPONSE_JSON=""
ARTIFACT_URL=""
MASTER_URL=""
OUTPUT_DIR="${PARALLEL_BENCH_ARTIFACT_DIR:-${REPO_ROOT}/tests/worker-cert/artifacts/parallel}"

usage() {
  cat >&2 <<'EOF'
usage: verify_parallel_artifact.sh --job-id ID --response-json PATH \
  --artifact-url URL --master-url URL [--output-dir PATH]
EOF
  exit "${1:-2}"
}

while (( $# > 0 )); do
  case "$1" in
    --job-id) [[ $# -ge 2 ]] || usage; JOB_ID="$2"; shift 2 ;;
    --response-json) [[ $# -ge 2 ]] || usage; RESPONSE_JSON="$2"; shift 2 ;;
    --artifact-url) [[ $# -ge 2 ]] || usage; ARTIFACT_URL="$2"; shift 2 ;;
    --master-url) [[ $# -ge 2 ]] || usage; MASTER_URL="$2"; shift 2 ;;
    --output-dir) [[ $# -ge 2 ]] || usage; OUTPUT_DIR="$2"; shift 2 ;;
    --help|-h) usage 0 ;;
    *) printf 'unknown option: %s\n' "$1" >&2; usage ;;
  esac
done

for value in JOB_ID RESPONSE_JSON ARTIFACT_URL MASTER_URL; do
  [[ -n "${!value}" ]] || { printf '%s is required\n' "$value" >&2; usage; }
done
[[ -r "$RESPONSE_JSON" ]] || { printf 'response JSON is not readable: %s\n' "$RESPONSE_JSON" >&2; exit 2; }
for binary in curl jq sha256sum ffprobe; do
  command -v "$binary" >/dev/null 2>&1 || { printf 'required binary missing: %s\n' "$binary" >&2; exit 2; }
done

TOKEN="${VELOX_ADMIN_TOKEN:-${VELOX_MASTER_BEARER:-}}"
if [[ -z "$TOKEN" && -n "${TOKEN_FILE:-}" && -r "$TOKEN_FILE" ]]; then
  TOKEN=$(grep -E '^VELOX_ADMIN_TOKEN=' "$TOKEN_FILE" | head -1 | sed 's/^[^=]*=//' | tr -d "'\"" | xargs || true)
fi
[[ -n "$TOKEN" ]] || { printf 'VELOX_ADMIN_TOKEN or VELOX_MASTER_BEARER is required\n' >&2; exit 2; }

mkdir -p "$OUTPUT_DIR" || exit 2
artifact_name="${JOB_ID}.mp4"
artifact_path="${OUTPUT_DIR}/${artifact_name}"
master_url="${MASTER_URL%/}"
if [[ "$ARTIFACT_URL" == /* ]]; then
  download_url="${master_url}${ARTIFACT_URL}"
elif [[ "$ARTIFACT_URL" =~ ^https?:// ]]; then
  download_url="$ARTIFACT_URL"
else
  download_url="${master_url}/${ARTIFACT_URL#./}"
fi

if ! curl -fsS --max-time 120 -H "Authorization: Bearer ${TOKEN}" "$download_url" -o "$artifact_path"; then
  printf 'artifact download failed: %s\n' "$download_url" >&2
  rm -f "$artifact_path"
  exit 1
fi
[[ -s "$artifact_path" ]] || { printf 'downloaded artifact is empty\n' >&2; rm -f "$artifact_path"; exit 1; }

VELOX_MASTER_BEARER="$TOKEN" bash "${SCRIPT_DIR}/verify_artifact.sh" "$artifact_path" \
  --job-id "$JOB_ID" --master-url "$master_url" --expect-status SUCCEEDED
