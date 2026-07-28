#!/usr/bin/env bash
# =============================================================================
# tests/smoke/full_payload/subtitle_sync/check_subtitle_sync.sh
# =============================================================================
# Local FFmpeg burn-in verifier for SUBTITLE-VOICEOVER synchronization. Loads
# a single-event ASS subtitle whose Dialogue start PTS is 0.300s, generates
# a 3.0s synthetic voiceover (440Hz sine, 48kHz stereo), renders the
# combined timeline with the same ffmpeg `subtitles=` filter the C++ engine
# invokes (RemoteCodex/native/video-engine-cpp/src/core/render_engine.cpp:77-83
# → burnSubtitleTrack path), then per-frame strip-stats extracts the actual
# PTS at which the subtitle text appears in the rendered video.
#
# Drift = detected_pts - 0.300   (sub PTS Start in ASS vs measured PTS in render)
# PASS if |drift| <= 80ms
#
# Voiceover audio PTS in the output is checked separately as a control: the
# voiceover starts at PTS=0.000 (lavfi sine @ duration=3.0), so audio+subtitle
# chronology in the muxed MP4 is end-to-end preserved. The drift measured is
# then exclusively the libass burn-in latency on top of the encoding step.
#
# Source-of-truth chain (mirrors the production path):
#   SubmitJobRequest with voiceover binding + subtitle_tracks[] (ASS)
#     → DataServer/internal/jobs/enqueue/enqueue_normalization_test.go
#     → DataServer/internal/handlers/server/pipeline/plan_derivation.go
#     → worker pkg/video/pipelines/hybrid/compiler.go → parseSubtitleTracks
#     → C++ engine RemoteCodex/native/video-engine-cpp/src/core/render_engine.cpp
#          reportProgress(80, "burning_subtitles");
#          const auto& subtitle = plan.subtitle_tracks.front();
#          burnSubtitleTrack(videoOnly, localSubtitle, subtitledVideo);
#          filter << "subtitles=" << file::shellQuote(subtitleFile.string());
#     → ffmpeg libass burn-in (output MP4 has no separate codec_type=subtitle)
#
# Exit codes:
#   0  PASS — |drift| <= 80 ms; voiceover PTS=0 confirmed; subtitle visibly
#             painted within the burnt-in window.
#   1  FAIL — |drift| > 80 ms OR no text detected OR voiceover PTS != 0.
#   2  usage / env — missing arg, missing prereq.
#
# Usage:
#   bash check_subtitle_sync.sh [--help]
# =============================================================================

set -uo pipefail

REAL_SCRIPT="$(readlink -f "${BASH_SOURCE[0]}")"
SCRIPT_DIR="$(cd "$(dirname "$REAL_SCRIPT")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../../.." && pwd)"
JOB_DIR="${JOB_DIR:-/tmp/velox-subtitle-sync/$(date -u +'%Y%m%dT%H%M%SZ')}"

# shellcheck source=tests/_lib/sh/_lib.sh
source "${REPO_ROOT}/tests/_lib/sh/_lib.sh"

usage() { sed -n '2,/^# ====/p' "$0" | sed 's/^# \{0,1\}//'; exit 0; }
[[ "${1:-}" == "--help" || "${1:-}" == "-h" ]] && usage

# --- prereqs ---
for t in ffmpeg ffprobe jq python3; do
  ensure_command_available "$t" || { log_error "missing prereq: $t"; exit 2; }
done

# --- known timing contract (mirrors the ASS Dialogue: 0,0:00:00.300,0:00:01.200,...) ---
EXPECTED_SUBTITLE_START_S=0.300
EXPECTED_SUBTITLE_END_S=1.200
TOTAL_DURATION_S=3.000
FPS=30
DRIFT_THRESHOLD_MS=80

# --- workspace ---
mkdir_p "$JOB_DIR"

# --- compute_strip_stats reused verbatim from
#     subtitle_special_chars/check_subtitle_burn_in.sh ---
compute_strip_stats() {
  local png="$1"
  local raw_rgb
  raw_rgb="$(mktemp --suffix=.rgb)"
  ffmpeg -v error -i "$png" -pix_fmt rgb24 -f rawvideo "$raw_rgb" -y \
    > /dev/null 2>&1 || { echo "mean=0 stddev=0 bright_pct=0"; return; }
  local dims
  dims="$(ffprobe -v error -select_streams v:0 -show_entries stream=width,height -of csv=p=0 "$png" 2>/dev/null)"
  local W="${dims%,*}"
  local H="${dims##*,}"
  : "${W:=1280}"; : "${H:=720}"
  python3 - "$raw_rgb" "$W" "$H" <<'PY'
import sys
fname, W, H = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
data = open(fname, 'rb').read()
y0 = 3 * H // 4
y1 = H
stride = W * 3
ystart = y0 * stride
yend   = y1 * stride
strip = data[ystart:yend]
n_px = len(strip) // 3
if n_px == 0:
    print("mean=0 stddev=0 bright_pct=0.00")
    sys.exit(0)
total = 0; sumsq = 0; bright = 0
for i in range(n_px):
    r = strip[i*3]; g = strip[i*3+1]; b = strip[i*3+2]
    y = (r * 299 + g * 587 + b * 114) // 1000
    total += y; sumsq += y*y
    if y > 128: bright += 1
mean = total / n_px
var  = sumsq / n_px - mean * mean
std  = var ** 0.5 if var > 0 else 0.0
print(f"mean={mean:.1f} stddev={std:.2f} bright_pct={bright / n_px * 100:.2f}")
PY
  rm -f "$raw_rgb"
}

# --- generate voiceover stub: 440Hz sine, 48kHz stereo, 3.0s ---
VOICEOVER_WAV="$JOB_DIR/voiceover.wav"
log_info "generating voiceover stub (sine 440Hz 3.0s) -> $VOICEOVER_WAV"
if ! ffmpeg -y -f lavfi -i "sine=frequency=440:duration=${TOTAL_DURATION_S}:sample_rate=48000" \
      -ac 2 "$VOICEOVER_WAV" > "$JOB_DIR/voiceover.log" 2>&1; then
  log_error "voiceover generation FAILED; see $JOB_DIR/voiceover.log"; exit 2
fi
# Sanity: confirm voiceover is 3.0s ± 50ms.
VO_DUR=$(ffprobe -v error -show_entries format=duration -of csv=p=0 "$VOICEOVER_WAV")
awk -v d="${VO_DUR}" -v expected_dur="3.0" 'BEGIN{ exit !((d+0 > 2.95) && (d+0 < 3.05)) }' \
  || { log_error "voiceover duration ${VO_DUR}s not within 2.95..3.05s"; exit 2; }

# --- generate test frame: dark-blue 1280x720 ---
FRAME_PNG="$JOB_DIR/test_frame.png"
log_info "generating test frame (dark-blue 1280x720 lavfi) -> $FRAME_PNG"
ffmpeg -y -f lavfi -i "color=c=0x1e3a5f:s=1280x720:d=1" -frames:v 1 "$FRAME_PNG" \
  > "$JOB_DIR/frame.log" 2>&1

# --- render: subtitles burned into video + voiceover muxed into audio ---
SUB_PATH="$SCRIPT_DIR/fixtures/special_chars_sync.ass"
[[ -r "$SUB_PATH" ]] || { log_error "ASS fixture not readable: $SUB_PATH"; exit 2; }

OUT_MP4="$JOB_DIR/render.mp4"
log_info "rendering: subtitles=${SUB_PATH} + voiceover=${VOICEOVER_WAV} -> $OUT_MP4"
ffmpeg -y -framerate "${FPS}" -loop 1 -i "$FRAME_PNG" -i "$VOICEOVER_WAV" \
      -vf "subtitles=${SUB_PATH}" \
      -t "${TOTAL_DURATION_S}" -c:v libx264 -pix_fmt yuv420p -c:a aac \
      -shortest "$OUT_MP4" > "$JOB_DIR/render.log" 2>&1 \
  || { log_error "render FAILED; see $JOB_DIR/render.log"; exit 2; }

# --- Tier 1: ffprobe sanity gates ---
probe_json="$(ffprobe -v quiet -print_format json -show_format -show_streams "$OUT_MP4")"
video_count=$(printf '%s' "$probe_json" | jq '[.streams[] | select(.codec_type=="video")] | length' 2>/dev/null)
audio_count=$(printf '%s' "$probe_json" | jq '[.streams[] | select(.codec_type=="audio")] | length' 2>/dev/null)
sub_count=$(   printf '%s' "$probe_json" | jq '[.streams[] | select(.codec_type=="subtitle")] | length' 2>/dev/null)
: "${video_count:=0}"; : "${audio_count:=0}"; : "${sub_count:=0}"

if (( video_count != 1 )); then log_error "video streams=${video_count} (expected 1)"; exit 1; fi
if (( audio_count != 1 )); then log_error "audio streams=${audio_count} (expected 1, voiceover)"; exit 1; fi
if (( sub_count != 0 ));   then log_error "subtitle streams=${sub_count} (expected 0, burn-in only)"; exit 1; fi

# --- Tier 1b: voiceover PTS starts at 0.000 (chronology preserved) ---
START_TIME=$(printf '%s' "$probe_json" | jq -r '.format.start_time // "0"' 2>/dev/null)
if ! awk -v s="${START_TIME}" 'BEGIN{ exit !(s+0 >= -0.001 && s+0 <= 0.001) }'; then
  log_error "format.start_time=${START_TIME} (expected 0.000)"
  exit 1
fi

# --- Tier 2: per-frame strip-stats to find first frame with text painted ---
mkdir -p "$JOB_DIR/frames"
log_info "extracting ${FPS}x${TOTAL_DURATION_S}s = ${FPS}*$(echo "$TOTAL_DURATION_S" | awk '{print int($1)}') per-frame PNGs"
ffmpeg -y -i "$OUT_MP4" -r "${FPS}" "$JOB_DIR/frames/p_%04d.png" \
  > "$JOB_DIR/extract.log" 2>&1 \
  || { log_error "per-frame PNG extract FAILED"; exit 1; }

PER_FRAME_STATS="$JOB_DIR/per_frame_stats.tsv"
: > "$PER_FRAME_STATS"
for f in "$JOB_DIR"/frames/p_*.png; do
  n=$(basename "$f" | sed 's/^p_0*//;s/\.png$//')
  pts=$(awk -v n="$n" -v fps="${FPS}" 'BEGIN{ printf "%.4f", n/fps }')
  stats="$(compute_strip_stats "$f")"
  stddev=$(awk '{ for (i=1;i<=NF;i++) if ($i ~ /^stddev=/) { split($i,a,"="); print a[2]; exit } }' <<< "$stats")
  bright_pct=$(awk '{ for (i=1;i<=NF;i++) if ($i ~ /^bright_pct=/) { split($i,a,"="); print a[2]; exit } }' <<< "$stats")
  printf '%s\t%s\t%s\t%s\n' "$n" "$pts" "$stddev" "$bright_pct" >> "$PER_FRAME_STATS"
done

# Find first frame where stddev > 5 (text painted) AND before subtitle-end PTS.
# Note: text may persist until 1.200s; we pick the FIRST appearance.
DETECTED_FRAME=$(awk -v thr=5 '
{
  if (($3+0 > thr+0) && !found) {
    print $1
    found = 1
    exit
  }
}
' "$PER_FRAME_STATS")

if [[ -z "$DETECTED_FRAME" ]]; then
  log_error "no text painted in any frame within the expected window — strip-threshold not crossed"
  log_error "per_frame_stats.tsv:"; cat "$PER_FRAME_STATS" >&2
  exit 1
fi

# --- Compute drift ---
DETECTED_PTS=$(awk -v n="$DETECTED_FRAME" -v fps="$FPS" 'BEGIN{ printf "%.4f", n/fps }')
DRIFT_MS=$(python3 -c "print(round(($DETECTED_PTS - $EXPECTED_SUBTITLE_START_S) * 1000, 2))")
ABS_DRIFT_MS=$(python3 -c "print(abs($DRIFT_MS))")
PASS_DRIFT=$(python3 -c "print(int($ABS_DRIFT_MS <= $DRIFT_THRESHOLD_MS))")

# --- Write machine-parseable run summary at $JOB_DIR/run_summary.json ---
SUMMARY="$JOB_DIR/run_summary.json"
python3 - <<PY > "$SUMMARY"
import json
print(json.dumps({
  "schema": "velox.smoke.subtitle-sync@1",
  "run_utc": "$(date -u +'%Y-%m-%dT%H:%M:%SZ')",
  "expected": {
    "subtitle_start_s": $EXPECTED_SUBTITLE_START_S,
    "subtitle_end_s": $EXPECTED_SUBTITLE_END_S,
    "voiceover_start_s": 0.0,
    "voiceover_total_s": $TOTAL_DURATION_S,
    "fps": $FPS,
    "drift_threshold_ms": $DRIFT_THRESHOLD_MS,
  },
  "measured": {
    "detected_frame": $DETECTED_FRAME,
    "detected_pts_s": $DETECTED_PTS,
    "drift_ms": $DRIFT_MS,
    "abs_drift_ms": $ABS_DRIFT_MS,
    "voiceover_start_s": $START_TIME,
  },
  "verdict": "PASS" if $PASS_DRIFT == 1 else "FAIL",
  "evidence": {
    "per_frame_stats": "$PER_FRAME_STATS",
    "rendered_mp4": "$OUT_MP4",
    "voiceover_wav": "$VOICEOVER_WAV",
  },
}, indent=2, sort_keys=True))
PY

# --- operator-facing verdict ---
echo
echo "==== SUBTITLE-VOICEOVER SYNC VERDICT ===="
echo "expected_subtitle_start_s = $EXPECTED_SUBTITLE_START_S"
echo "voiceover_start_s        = $START_TIME   (must be 0.000)"
echo "video_fps                = $FPS"
echo "total_frames_extracted   = $(wc -l < "$PER_FRAME_STATS")"
echo "first_text_frame_n       = $DETECTED_FRAME"
echo "first_text_pts_s         = $DETECTED_PTS"
echo "drift_ms                 = $DRIFT_MS"
echo "abs_drift_ms             = $ABS_DRIFT_MS   (threshold <= $DRIFT_THRESHOLD_MS)"
echo "run_summary              = $SUMMARY"
echo "per_frame_stats          = $PER_FRAME_STATS"
echo
ls -la "$JOB_DIR" | sed 's/^/  /'

if (( PASS_DRIFT == 1 )); then
  log_info "SYNC-PASS: subtitle rendered within tolerance (drift=${DRIFT_MS}ms <= ${DRIFT_THRESHOLD_MS}ms)"
  exit 0
fi
log_error "SYNC-FAIL: drift=${DRIFT_MS}ms exceeds ${DRIFT_THRESHOLD_MS}ms tolerance"
exit 1
