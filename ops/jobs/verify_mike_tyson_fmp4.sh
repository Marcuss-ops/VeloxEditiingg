#!/usr/bin/env bash
# Verify the Mike Tyson final render through the producer-owned fMP4 path.
#
# The input plan must already be a complete CompiledRenderPlanV2 emitted by
# PipelineGen. The legacy Creator push fixture is the source/rendering input;
# it is not itself an fMP4 assembly job.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
VERIFY="$ROOT/scripts/ops/verify-fmp4-producer-job.sh"

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" || $# -lt 1 ]]; then
  cat >&2 <<'USAGE'
usage: ops/jobs/verify_mike_tyson_fmp4.sh <compiled-render-plan-v2.json> [options]

The plan must contain output.profile_id=velox-h264-fmp4-stream-v1 and the
complete final_audio contract. Extra options are passed to the generic
producer-job verifier; for example: --dry-run, --json, --job-id JOB_ID.

Environment:
  VELOX_TYSON_FMP4_DESTINATION  delivery destination (default: drive-production)
USAGE
  [[ $# -gt 0 ]] && exit 0
  exit 2
fi

PLAN="$1"
shift
[[ -r "$PLAN" ]] || {
  printf 'precondition: plan is not readable: %s\n' "$PLAN" >&2
  exit 2
}

DESTINATION="${VELOX_TYSON_FMP4_DESTINATION:-drive-production}"
exec "$VERIFY" \
  --plan "$PLAN" \
  --video-name "From Cinema to the Ring: Mike Tyson — fMP4 final" \
  --idempotency-key "mike-tyson-fmp4-final-v1" \
  --delivery-destination "$DESTINATION" \
  "$@"
