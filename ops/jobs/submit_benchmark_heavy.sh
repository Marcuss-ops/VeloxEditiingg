#!/usr/bin/env bash
# ops/jobs/submit_benchmark_heavy.sh
#
# Submit the official benchmark-heavy payload to the master via the
# canonical M2M intake (POST /api/v1/jobs) and poll to terminal state.
#
# 24 scenes with alternating clip/stock assets, long voiceover, subtitles,
# background music and a title overlay. Targets RAM growth, temp
# amplification, concat cost and worker limits.
#
# Required env:
#   VELOX_MASTER_URL            master base URL (default http://127.0.0.1:8080)
#   VELOX_ADMIN_TOKEN           admin bearer (env var or TOKEN_FILE dotenv)
#   VELOX_BENCHMARK_CLIP_A_URL      clip used on even scenes
#   VELOX_BENCHMARK_CLIP_B_URL      clip used on odd scenes
#   VELOX_BENCHMARK_STOCK_A_URL     stock used on even scenes
#   VELOX_BENCHMARK_STOCK_B_URL     stock used on odd scenes
#   VELOX_BENCHMARK_VOICEOVER_URL   voiceover reused on every scene
#   VELOX_BENCHMARK_SUBTITLES_URL   subtitle source
#   VELOX_BENCHMARK_MUSIC_URL       background music source
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

BENCHMARK_PAYLOAD_FILE="${SCRIPT_DIR}/benchmark-heavy.generate.json"

CLIP_A="${VELOX_BENCHMARK_CLIP_A_URL:-}"
CLIP_B="${VELOX_BENCHMARK_CLIP_B_URL:-}"
STOCK_A="${VELOX_BENCHMARK_STOCK_A_URL:-}"
STOCK_B="${VELOX_BENCHMARK_STOCK_B_URL:-}"
VOICEOVER_URL="${VELOX_BENCHMARK_VOICEOVER_URL:-}"
SUBTITLES_URL="${VELOX_BENCHMARK_SUBTITLES_URL:-}"
MUSIC_URL="${VELOX_BENCHMARK_MUSIC_URL:-}"
if [[ -z "${CLIP_A}" || -z "${CLIP_B}" || -z "${STOCK_A}" || -z "${STOCK_B}" \
      || -z "${VOICEOVER_URL}" || -z "${SUBTITLES_URL}" || -z "${MUSIC_URL}" ]]; then
  benchmark_fail "set VELOX_BENCHMARK_CLIP_A_URL, CLIP_B_URL, STOCK_A_URL, STOCK_B_URL, VOICEOVER_URL, SUBTITLES_URL, MUSIC_URL (real asset URLs)"
fi

benchmark_resolve_admin_token
benchmark_mint_m2m

# Substitute frozen placeholders: even-index scenes use clip A/stock A,
# odd-index scenes use clip B/stock B.
benchmark_substitute_payload() {
  jq --arg clipA "${CLIP_A}" --arg clipB "${CLIP_B}" \
     --arg stockA "${STOCK_A}" --arg stockB "${STOCK_B}" \
     --arg vo "${VOICEOVER_URL}" --arg subs "${SUBTITLES_URL}" --arg music "${MUSIC_URL}" '
    .scenes |= map(
      if (.index % 2) == 0 then
        (.clip.url = $clipA) | (.stock.url = $stockA)
      else
        (.clip.url = $clipB) | (.stock.url = $stockB)
      end
      | (.voiceover.url = $vo)
    )
    | (.scenes[0].subtitles.url = $subs)
    | .audio_tracks[0].source_url = $music
  ' "${BENCHMARK_PAYLOAD_FILE}"
}

rc=0
benchmark_submit_and_poll || rc=$?
case $rc in
  0) printf 'PASS benchmark-heavy: SUCCEEDED\n' ;;
  1) printf 'FAIL benchmark-heavy: terminal FAILED/CANCELLED (expected? inspect job)\n' >&2 ;;
  2) printf 'FAIL benchmark-heavy: poll timeout\n' >&2 ;;
  3) printf 'FAIL benchmark-heavy: rejected at intake (HTTP non-202)\n' >&2 ;;
esac
exit $rc
