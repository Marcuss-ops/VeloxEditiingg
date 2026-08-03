#!/usr/bin/env bash
# ops/jobs/submit_benchmark_minimal.sh
#
# Submit the official benchmark-minimal payload to the master via the
# canonical M2M intake (POST /api/v1/jobs) and poll to terminal state.
#
# Measures the FIXED generator overhead (scheduling, worker start, parsing,
# download, FFmpeg open, upload) with the smallest real workload: one short
# scene, one short clip, one short voiceover, 1080p, no stock.
#
# Required env:
#   VELOX_MASTER_URL            master base URL (default http://127.0.0.1:8080)
#   VELOX_ADMIN_TOKEN           admin bearer (env var or TOKEN_FILE dotenv)
#   VELOX_BENCHMARK_CLIP_URL    real short clip asset URL (https or velox-asset://)
#   VELOX_BENCHMARK_VOICEOVER_URL  real short voiceover asset URL
#
# Optional env:
#   VELOX_BENCHMARK_IDEM_KEY    idempotency override (cold/warm cache runs)
#   VELOX_BENCHMARK_POLL_TIMEOUT_S   poll cap in seconds (default 300)
#   VELOX_BENCHMARK_DELIVERY_DESTINATION  delivery_plan destination override
#                              (payload default "drive" must exist in the
#                              deployment's delivery_destinations with
#                              enabled=1; empty keeps the frozen destination)
#
# Exit codes: 0 SUCCEEDED, 1 FAILED/CANCELLED, 2 poll timeout,
#             3 POST rejected at intake, 4 usage/env error.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/benchmark-common.sh
source "${SCRIPT_DIR}/lib/benchmark-common.sh"

BENCHMARK_PAYLOAD_FILE="${SCRIPT_DIR}/benchmark-minimal.generate.json"

CLIP_URL="${VELOX_BENCHMARK_CLIP_URL:-}"
VOICEOVER_URL="${VELOX_BENCHMARK_VOICEOVER_URL:-}"
if [[ -z "${CLIP_URL}" || -z "${VOICEOVER_URL}" ]]; then
  benchmark_fail "VELOX_BENCHMARK_CLIP_URL and VELOX_BENCHMARK_VOICEOVER_URL must be set (real short clip + voiceover asset URLs)"
fi

benchmark_resolve_admin_token
benchmark_mint_m2m

# Substitute the frozen placeholder URLs with the operator-provided assets.
benchmark_substitute_payload() {
  jq --arg clip "${CLIP_URL}" --arg vo "${VOICEOVER_URL}" \
    '.scenes[0].clip.url = $clip | .scenes[0].voiceover.url = $vo' \
    "${BENCHMARK_PAYLOAD_FILE}"
}

rc=0
benchmark_submit_and_poll || rc=$?
case $rc in
  0) printf 'PASS benchmark-minimal: SUCCEEDED\n' ;;
  1) printf 'FAIL benchmark-minimal: terminal FAILED/CANCELLED (expected? inspect job)\n' >&2 ;;
  2) printf 'FAIL benchmark-minimal: poll timeout\n' >&2 ;;
  3) printf 'FAIL benchmark-minimal: rejected at intake (HTTP non-202)\n' >&2 ;;
esac
exit $rc
