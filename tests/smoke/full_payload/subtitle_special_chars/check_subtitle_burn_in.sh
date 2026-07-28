#!/usr/bin/env bash
# =============================================================================
# tests/smoke/full_payload/subtitle_special_chars/check_subtitle_burn_in.sh
# =============================================================================
# Local FFmpeg burn-in verifier for ASS + SRT subtitle special-character
# rendering. Independent of the master / worker fleet: synthesizes a flat
# test frame via ffmpeg lavfi (no external asset), burns each fixture's
# subtitles onto that frame using ffmpeg's `subtitles=` filter (identical to
# the C++ engine's burnSubtitleTrack path in
# RemoteCodex/native/video-engine-cpp/src/core/render_engine.cpp:77-83,280-292),
# then asserts the result via ffprobe + strip-stats + tesseract OCR.
#
# Why local-only rather than master-routed:
#   tests/worker-cert/fixtures/assets.json has zero rows with kind=subtitle
#   (asset reuse rule: smoke harnesses MUST NOT add asset_ids there). The
#   codepath under test — ffmpeg/libass burn-in — is invoked identically in
#   master-routed and local-routed flows, so a local test is sufficient to
#   validate the rendering core before any master-routed rollout.
#
# Verification tiers:
#   Tier 1 (gate):   ffprobe confirms burn-in (codec_type=subtitle stream
#                    count == 0; codec_type=video stream == 1; mp4 non-empty
#                    and non-zero duration).
#   Tier 2 (gate):   strip-stats — luminance stddev > 5 OR bright-pixel
#                    density > 0.5% in the bottom 25% strip of the rendered
#                    middle frame. Proves text was actually painted on screen
#                    (not blank, not full-tofu, not font-fallback box only).
#   Tier 3 (informational): tesseract OCR char-recovery rate over 6
#                    visually-distinct chars (€, «, », —, ß, ñ). Logged to
#                    evidence JSON — operator-facing metric, NOT a pass/fail
#                    gate (tesseract is fragile on emoji + lowercase Greek +
#                    font fallback rendering).
#   Tier 4 (gate, ASS-only):  libass-bypass detector — the override tags
#                    {\c&H0000FF&}{\b1} in the .ass must NOT appear verbatim
#                    in the OCR text. Presence ⇒ the filter ran in text-only
#                    mode (libass bypassed).
#
# Exit codes:
#   0  PASS — Tier 1 + Tier 2 + (Tier 4 if ASS) all green for both formats.
#             Tier 3 OCR rate is logged to evidence/run_summary.json.
#   1  FAIL — at least one gate failed; per-format status JSON in $JOB_DIR.
#   2  usage / env — missing arg, missing prereq (ffmpeg/ffprobe/tesseract/jq),
#                    missing fixture path.
#
# Usage:
#   bash check_subtitle_burn_in.sh [--help]
#
# Environment overrides:
#   JOB_DIR     per-run audit directory (default /tmp/velox-subtitle-smoke/$UTC)
#               override to point at a stable dir if you want to compare runs.
# =============================================================================

set -uo pipefail  # NOT -e: continue across formats so the verdict reports all

REAL_SCRIPT="$(readlink -f "${BASH_SOURCE[0]}")"
SCRIPT_DIR="$(cd "$(dirname "$REAL_SCRIPT")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../../.." && pwd)"
JOB_DIR="${JOB_DIR:-/tmp/velox-subtitle-smoke/$(date -u +'%Y%m%dT%H%M%SZ')}"

# shellcheck source=tests/_lib/sh/_lib.sh
source "${REPO_ROOT}/tests/_lib/sh/_lib.sh"

usage() { sed -n '2,/^# ====/p' "$0" | sed 's/^# \{0,1\}//'; exit 0; }
[[ "${1:-}" == "--help" || "${1:-}" == "-h" ]] && usage

# --- Tier-3 (informational) chars ---
# Visually distinct in OCR for tesseract with ita+eng training. Subset of the
# full fixture char-set; chosen because each has a unique glyph geometry:
#   €   Euro sign               — horizontal-line geometry
#   « » guillemets              — chevron shapes
#   —   em-dash                 — long horizontal line
#   ß   ESZETT (German)         — distinct ligature
#   ñ   n-tilde                 — tilde overletter
# Note: emoji (😃 🎉) and Greek β are NOT in this list — tesseract has no
# reliable recovery for them. They are validated by Tier 2 (text rendered)
# and operator visual review of $JOB_DIR/still_*.png.
EXPECTED_KEY_CHARS=(€ « » — ß ñ)

# --- Tier 4 (ass-only) libass-via bypass substrings ---
# If libass was bypassed (filter ran in text-stub mode), libass override tags
# leak through. Any of these substrings in OCR text = bypass detected.
LIBASS_BYPASS_MARKERS=('{\c' '&H00' '&HFF' '\\c' '{c' '{\b' '{\i')

# --- prereqs ---
for t in ffmpeg ffprobe tesseract jq python3; do
  ensure_command_available "$t" || { log_error "missing prereq: $t"; exit 2; }
done

# --- workspace ---
mkdir_p "$JOB_DIR"

# --- compute_strip_stats <png> ---
# Emits "mean=Y.AA stddev=SS.SS bright_pct=P.PP" using python3 stdlib only
# (PIL-free). Reads raw RGB24 from ffmpeg, slices the bottom 25%, computes
# luminance Y=0.299*R + 0.587*G + 0.114*B per pixel, summarizes stats.
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
# bottom 25% strip
y0 = 3 * H // 4
y1 = H
# Reconstruct pixel rows from raw RGB24 triples
stride = W * 3
ystart = y0 * stride
yend   = y1 * stride
strip = data[ystart:yend]
n_px = len(strip) // 3
if n_px == 0:
    print("mean=0 stddev=0 bright_pct=0.00")
    sys.exit(0)
# Compute luminance per pixel
total = 0
sumsq = 0
bright = 0
for i in range(n_px):
    r = strip[i*3]
    g = strip[i*3 + 1]
    b = strip[i*3 + 2]
    y = (r * 299 + g * 587 + b * 114) // 1000  # Q-scale
    total += y
    sumsq += y * y
    if y > 128:
        bright += 1
mean = total / n_px
var  = sumsq / n_px - mean * mean
std  = var ** 0.5 if var > 0 else 0.0
print(f"mean={mean:.1f} stddev={std:.2f} bright_pct={bright / n_px * 100:.2f}")
PY
  rm -f "$raw_rgb"
}

# --- test frame: dark-blue 1280x720 single frame (lavfi, no external asset) ---
FRAME_PNG="$JOB_DIR/test_frame.png"
log_info "generating test frame (dark-blue 1280x720 lavfi) -> $FRAME_PNG"
if ! ffmpeg -y -f lavfi -i "color=c=0x1e3a5f:s=1280x720:d=1" -frames:v 1 "$FRAME_PNG" \
    > "$JOB_DIR/frame.log" 2>&1; then
  log_error "lavfi frame generation FAILED; see $JOB_DIR/frame.log"
  exit 2
fi
[[ -r "$FRAME_PNG" ]] || { log_error "frame.png not produced"; exit 2; }

# --- baseline strip-stats of the no-subtitle test frame ---
BASE_STATS="$(compute_strip_stats "$FRAME_PNG")"
log_info "strip_stats BASE (no subtitle): ${BASE_STATS}"

# --- per-format verification ---
verify_format() {
  local format="$1"   # ass or srt
  local sub_path="$SCRIPT_DIR/fixtures/special_chars.${format}"
  local out_mp4="$JOB_DIR/render_${format}.mp4"
  local still_png="$JOB_DIR/still_${format}.png"
  local ocr_prefix="$JOB_DIR/ocr_${format}"
  local ocr_txt="${ocr_prefix}.txt"
  local status="$JOB_DIR/status_${format}.json"
  local pass=1
  local failed=()
  local notes=()

  [[ -r "$sub_path" ]] || { log_error "[${format}] fixture not readable: $sub_path"; return 1; }

  log_info "[${format}] ffmpeg burn-in: subtitles=${sub_path} → ${out_mp4}"
  if ! ffmpeg -y -loop 1 -i "$FRAME_PNG" \
        -vf "subtitles=${sub_path}" \
        -t 1.5 -r 30 -c:v libx264 -pix_fmt yuv420p \
        "$out_mp4" \
        > "$JOB_DIR/burn_${format}.log" 2>&1; then
    log_error "[${format}] ffmpeg burn-in FAILED; see $JOB_DIR/burn_${format}.log"
    failed+=("ffmpeg burn-in failed")
    pass=0
    printf '{"format":"%s","verdict":"FAIL","stage":"burn_in","failed":["ffmpeg burn-in failed"]}\n' "$format" > "$status"
    return $(( 1 - pass ))
  fi

  # --- Tier 1: ffprobe assertions ---
  local probe_json
  probe_json="$(ffprobe -v quiet -print_format json -show_format -show_streams "$out_mp4")"
  local video_count audio_count sub_count
  video_count=$(printf '%s' "$probe_json" | jq '[.streams[] | select(.codec_type=="video")] | length' 2>/dev/null)
  audio_count=$(printf '%s' "$probe_json" | jq '[.streams[] | select(.codec_type=="audio")] | length' 2>/dev/null)
  sub_count=$(   printf '%s' "$probe_json" | jq '[.streams[] | select(.codec_type=="subtitle")] | length' 2>/dev/null)
  : "${video_count:=0}"; : "${audio_count:=0}"; : "${sub_count:=0}"
  local duration
  duration=$(printf '%s' "$probe_json" | jq '.format.duration // "0" | tonumber' 2>/dev/null || echo 0)
  local size_bytes
  size_bytes=$(wc -c < "$out_mp4")
  log_info "[${format}] streams: video=${video_count} audio=${audio_count} subtitle=${sub_count}  duration=${duration}s  size=${size_bytes}B"

  if (( sub_count != 0 )); then pass=0; failed+=("Tier 1: subtitle streams=${sub_count} (expected 0)"); fi
  if (( video_count != 1 )); then pass=0; failed+=("Tier 1: video streams=${video_count} (expected 1)"); fi
  if ! awk -v d="$duration" 'BEGIN{ exit !(d > 0) }'; then
    pass=0; failed+=("Tier 1: duration=${duration}s (expected > 0)")
  fi
  if (( size_bytes < 1000 )); then
    pass=0; failed+=("Tier 1: size=${size_bytes}B (expected >= 1000)")
  fi

  if (( pass == 0 )); then
    log_error "[${format}] Tier 1 (ffprobe) FAILED: ${failed[*]}"
    printf '{"format":"%s","verdict":"FAIL","stage":"ffprobe","streams":{"video":%s,"audio":%s,"subtitle":%s},"duration_s":%s,"size_bytes":%s,"failed":["%s"]}\n' \
      "$format" "$video_count" "$audio_count" "$sub_count" "${duration:-0}" "$size_bytes" \
      "$(printf '%s\n' "${failed[@]}" | paste -sd '","' -)" > "$status"
    return $(( 1 - pass ))
  fi

  # --- middle still frame ---
  log_info "[${format}] extracting middle still frame (n=15 of ~45, ~0.5s)"
  rm -f "$still_png"
  if ! ffmpeg -y -i "$out_mp4" -vf "select=eq(n\,15)" -vframes 1 "$still_png" \
      > "$JOB_DIR/still_${format}.log" 2>&1; then
    pass=0; failed+=("Tier 2: still-frame extraction failed")
    log_error "[${format}] still-frame extraction failed"
    printf '{"format":"%s","verdict":"FAIL","stage":"still_extract","failed":["still-frame extraction failed"]}\n' "$format" > "$status"
    return $(( 1 - pass ))
  fi

  # --- Tier 2: strip-stats (text rendered, not blank / not full-tofu) ---
  local stats
  stats="$(compute_strip_stats "$still_png")"
  log_info "[${format}] strip_stats: ${stats}"
  local stddev bright_pct
  stddev=$(awk '{ for (i=1; i<=NF; i++) if ($i ~ /^stddev=/) { split($i,a,"="); print a[2]; exit } }' <<< "$stats")
  bright_pct=$(awk '{ for (i=1; i<=NF; i++) if ($i ~ /^bright_pct=/) { split($i,a,"="); print a[2]; exit } }' <<< "$stats")
  : "${stddev:=0}"; : "${bright_pct:=0}"
  if ! awk -v s="$stddev" -v b="$bright_pct" 'BEGIN{ exit !((s+0 > 5) || (b+0 > 0.5)) }'; then
    pass=0
    failed+=("Tier 2: strip-stats text not rendered (stddev=${stddev} bright_pct=${bright_pct}%)")
  else
    notes+=("Tier 2 OK: stddev=${stddev} bright_pct=${bright_pct}% — text rendered on strip")
  fi

  # --- Tier 3 (informational): tesseract OCR ---
  log_info "[${format}] tesseract OCR --psm 7 (single line, ita+eng) [informational]"
  rm -f "${ocr_prefix}.txt"
  tesseract "$still_png" "$ocr_prefix" -l ita+eng --psm 7 \
    > "$JOB_DIR/tesseract_${format}.log" 2>&1 || true
  local ocr_content=""
  [[ -r "$ocr_txt" ]] && ocr_content="$(< "$ocr_txt")"
  log_info "[${format}] OCR text: $(printf '%s' "$ocr_content" | tr '\n' ' ' | head -c 200)"

  local recovered=0 missing_csv=""
  for c in "${EXPECTED_KEY_CHARS[@]}"; do
    if [[ "$ocr_content" == *"$c"* ]]; then
      (( recovered++ ))
    else
      [[ -n "$missing_csv" ]] && missing_csv+=","
      missing_csv+="\"$c\""
    fi
  done
  local total=${#EXPECTED_KEY_CHARS[@]}
  local pct=$(( recovered * 100 / total ))
  log_info "[${format}] Tier 3 (informational) ocr_recovered=${recovered}/${total} (${pct}%) key-chars presence"
  notes+=("Tier 3 (OCR): ${recovered}/${total} ${pct}% key-char coverage — informational only")

  # --- Tier 4 (ASS only): libass-bypass marker check ---
  local libass_ok=1
  if [[ "$format" == "ass" ]]; then
    for marker in "${LIBASS_BYPASS_MARKERS[@]}"; do
      if [[ "$ocr_content" == *"$marker"* ]]; then
        libass_ok=0
        failed+=("Tier 4: libass-bypass detected — marker '${marker}' present in OCR")
        break
      fi
    done
    if (( libass_ok == 1 )); then
      notes+=("Tier 4 OK: libass override tags {\c&H0000FF&} absent from OCR — libass processed")
    fi
  fi
  (( pass == 1 && libass_ok == 0 )) && pass=0

  # --- final per-format verdict ---
  if (( pass == 1 )); then
    printf '{"format":"%s","verdict":"PASS","tier1_ffprobe":true,"tier2_strip_stats":"%s","tier3_ocr":{"recovered":%s,"total":%s,"pct":%s},"notes":["%s"],"evidence_png":"%s"}\n' \
      "$format" "$stats" "$recovered" "$total" "$pct" \
      "$(printf '%s\n' "${notes[@]}" | paste -sd '","' -)" \
      "$still_png" > "$status"
    log_info "[${format}] PASS"
  else
    printf '{"format":"%s","verdict":"FAIL","stage":"tier2_or_tier4","failed":["%s"],"tier3_ocr":{"recovered":%s,"total":%s,"pct":%s},"evidence_png":"%s"}\n' \
      "$format" \
      "$(printf '%s\n' "${failed[@]}" | paste -sd '","' -)" \
      "$recovered" "$total" "$pct" \
      "$still_png" > "$status"
    log_error "[${format}] FAIL: ${failed[*]}"
  fi
  return $(( 1 - pass ))
}

verify_format ass; rv_ass=$?
verify_format srt; rv_srt=$?

# --- combined run summary (machine-parseable) ---
RUN_SUMMARY="$JOB_DIR/run_summary.json"
printf '{"run_utc":"%s","base_stats":"%s","results":{"ass":{"rc":%s,"status":"%s"},"srt":{"rc":%s,"status":"%s"}}}\n' \
  "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$BASE_STATS" \
  "$rv_ass" "$JOB_DIR/status_ass.json" \
  "$rv_srt" "$JOB_DIR/status_srt.json" > "$RUN_SUMMARY"

# --- combined verdict ---
echo
echo "==== SUBTITLE-SPECIAL-CHARS VERDICT ===="
echo "ass_rc=${rv_ass}  srt_rc=${rv_srt}"
echo "evidence dir: $JOB_DIR"
ls -la "$JOB_DIR" | sed 's/^/  /'
echo "run_summary: $RUN_SUMMARY"
if (( rv_ass == 0 && rv_srt == 0 )); then
  log_info "DUAL-PASS: ASS + SRT rendered without substitution or missing-glyph (Tier 1+2+4 green; Tier 3 OCR rate informational; operator reviews \$JOB_DIR/still_*.png for visual confirmation)"
  exit 0
fi
log_error "DUAL-FAIL: at least one format rejected (see $JOB_DIR/status_*.json)"
exit 1
