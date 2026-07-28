#!/usr/bin/env bash
# =============================================================================
# tests/smoke/full_payload/audio_preservation/check_audio_streams.sh
# =============================================================================
# Pure ffprobe verifier for the dual-audio-preservation contract. Given a
# rendered MP4, asserts:
#   1. audio_streams count = 2 (one voiceover, one scene_clip_audio)
#   2. both audio streams start within ±100ms of each other (sync drift ≤ 0.10s)
#   3. format.duration > 0 (the MP4 is non-empty)
#
# Source-of-truth for the dual-track contract:
#   - DataServer/internal/jobs/enqueue/narrated_clip_timeline.go
#     (builds role:voiceover + role:scene_clip_audio lanes)
#   - RemoteCodex/native/video-engine-cpp/src/services/media_utils.cpp::muxAudio
#     (muxes both feeds into the final MP4)
#   - tests/worker-cert/verify_artifact.sh (canonical ffprobe call pattern)
#
# Exit codes:
#   0  PASS — 2 audio streams present + sync + non-empty
#   1  FAIL — at least one assertion failed (count / sync / duration)
#   2  usage / env (missing argument, missing ffprobe, missing input)
#
# Usage:
#   bash tests/smoke/full_payload/audio_preservation/check_audio_streams.sh <path/to/rendered.mp4>
#   bash tests/smoke/full_payload/audio_preservation/check_audio_streams.sh --help
# =============================================================================

set -uo pipefail

REAL_SCRIPT="$(readlink -f "${BASH_SOURCE[0]}")"
SCRIPT_DIR="$(cd "$(dirname "$REAL_SCRIPT")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../../.." && pwd)"

# shellcheck source=tests/_lib/sh/_lib.sh
source "${REPO_ROOT}/tests/_lib/sh/_lib.sh"

usage() {
  sed -n '2,/^# ====/p' "$0" | sed 's/^# \{0,1\}//'
  exit 0
}
[[ "${1:-}" == "--help" || "${1:-}" == "-h" ]] && usage
[[ -n "${1:-}" ]] || { log_error "usage: $0 <path-to-rendered.mp4>"; exit 2; }

MP4_PATH="$1"
[[ -r "$MP4_PATH" ]] || { log_error "mp4 not readable: $MP4_PATH"; exit 2; }
ensure_command_available ffprobe || { log_error "ffprobe missing on PATH"; exit 2; }

# Stream inventory via ffprobe JSON. -print_format json + -show_format + -show_streams
# gives us stream array + container format fields; -v quiet suppresses
# ffprobe's banner noise (matches scripts/ci/golden-e2e-verify-media.py).
PROBE_JSON="$(ffprobe -v quiet -print_format json -show_format -show_streams "$MP4_PATH")"
[[ -n "$PROBE_JSON" ]] || { log_error "ffprobe returned empty for $MP4_PATH"; exit 2; }

AUDIO_COUNT=$(printf '%s' "$PROBE_JSON" | jq '[.streams[] | select(.codec_type == "audio")] | length' 2>/dev/null)
VIDEO_COUNT=$(printf '%s' "$PROBE_JSON" | jq '[.streams[] | select(.codec_type == "video")] | length' 2>/dev/null)

log_info "mp4=$MP4_PATH audio_streams=$AUDIO_COUNT video_streams=$VIDEO_COUNT"

# Assertion 1: exactly 2 audio streams (voiceover + scene_clip_audio).
if (( AUDIO_COUNT != 2 )); then
  log_error "FAIL: expected exactly 2 audio streams (voiceover + scene_clip_audio); got AUDIO_COUNT=$AUDIO_COUNT"
  exit 1
fi

# Per-stream fields: codec_name (audit-friendly) + start_time (PTS-based, in
# seconds). ffprobe populates .start_time lazily; fall back to "0" when
# the field is absent (raw PCM dual = both starting at t=0 canonical).
A1_CODEC=$(printf '%s' "$PROBE_JSON" \
  | jq -r '[.streams[] | select(.codec_type=="audio")][0] | .codec_name // "(unset)"' 2>/dev/null)
A2_CODEC=$(printf '%s' "$PROBE_JSON" \
  | jq -r '[.streams[] | select(.codec_type=="audio")][1] | .codec_name // "(unset)"' 2>/dev/null)

A1_START=$(printf '%s' "$PROBE_JSON" \
  | jq '[.streams[] | select(.codec_type=="audio")][0] | .start_time // "0" | tonumber' 2>/dev/null)
A2_START=$(printf '%s' "$PROBE_JSON" \
  | jq '[.streams[] | select(.codec_type=="audio")][1] | .start_time // "0" | tonumber' 2>/dev/null)

TOTAL_DUR=$(printf '%s' "$PROBE_JSON" | jq '.format.duration // "0" | tonumber' 2>/dev/null)
# Each stream also reports its own duration (defensive in case format
# level dur is missing due to a multi-mux concat edge case).
A1_DUR=$(printf '%s' "$PROBE_JSON" \
  | jq '[.streams[] | select(.codec_type=="audio")][0] | .duration // "0" | tonumber' 2>/dev/null)
A2_DUR=$(printf '%s' "$PROBE_JSON" \
  | jq '[.streams[] | select(.codec_type=="audio")][1] | .duration // "0" | tonumber' 2>/dev/null)

# Assertion 2: sync drift between the two audio streams ≤ 0.10s (100ms).
# For a clean dual-track the C++ engine aligns both feeds at the same PTS
# base; any drift inside the 100ms band is acceptable flake territory.
SYNC_DRIFT=$(awk -v a="$A1_START" -v b="$A2_START" \
  'BEGIN{ d=a-b; if (d<0) d=-d; printf "%.3f", d }')

# Assertion 2b: per-stream duration drift ≤ 0.05s (50ms). Catches a malformed
# mux where the C++ engine wrote e.g. a 30s voiceover next to a 5s clip
# audio (count check passes but tip-off is missing).
DUR_DRIFT=$(awk -v d1="$A1_DUR" -v d2="$A2_DUR" \
  'BEGIN{ d=d1-d2; if (d<0) d=-d; printf "%.3f", d }')

log_info "audio_stream_1: codec=$A1_CODEC start_time=${A1_START}s duration=${A1_DUR}s"
log_info "audio_stream_2: codec=$A2_CODEC start_time=${A2_START}s duration=${A2_DUR}s"
log_info "sync_drift=${SYNC_DRIFT}s  dur_drift=${DUR_DRIFT}s  total_duration=${TOTAL_DUR}s"

if ! awk -v d="$SYNC_DRIFT" 'BEGIN{ exit !(d <= 0.10) }'; then
  log_error "FAIL: sync drift ${SYNC_DRIFT}s exceeds the 0.10s tolerance band"
  exit 1
fi
if ! awk -v d="$DUR_DRIFT" 'BEGIN{ exit !(d <= 0.05) }'; then
  log_error "FAIL: per-stream duration drift ${DUR_DRIFT}s exceeds the 0.05s tolerance band"
  exit 1
fi

# Assertion 3: container format.duration > 0 ensures the MP4 actually has
# muxed content (an empty/zero-duration file would silently pass the
# stream-count check otherwise).
if ! awk -v d="$TOTAL_DUR" 'BEGIN{ exit !(d > 0) }'; then
  log_error "FAIL: format.duration non-positive (${TOTAL_DUR}s); mp4 is empty"
  exit 1
fi

echo "OK: dual-audio preserved -> audio_streams=${AUDIO_COUNT} codec_1=${A1_CODEC} codec_2=${A2_CODEC} codec_s_1_dur=${A1_DUR}s codec_s_2_dur=${A2_DUR}s sync_drift_s=${SYNC_DRIFT} total_duration_s=${TOTAL_DUR}"
exit 0
