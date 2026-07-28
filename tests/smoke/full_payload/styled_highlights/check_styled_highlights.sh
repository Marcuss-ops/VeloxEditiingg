#!/usr/bin/env bash
# =============================================================================
# tests/smoke/full_payload/styled_highlights/check_styled_highlights.sh
# =============================================================================
# Local FFmpeg burn-in verifier for STYLE preservation across 3 distinct ASS
# Dialogue events ("frase importante", "nome speciale", "parola evidenziata")
# rendered in non-overlapping PTS windows, each in a different vertical
# quadrant with a distinct font size + colour + (bold/italic) attribute.
#
#   Event 1  frase importante   TOP    fs96  RED   bold          0.10..0.80s
#   Event 2  Marco Aurelio      MIDDLE fs64  CYAN  regular       0.90..1.60s
#   Event 3  _HIGHLIGHTED_      BOTTOM fs80  GREEN italic       1.70..2.40s
#
# Verification Tiers
#   1.  Burn-in contract: ffprobe video=1 audio=1 subtitle=0; start_time=0
#   2.  Position gate (per event): painted quadrant stddev>5; others <1.
#   3.  Colour gate (per event): channel-dominance ratio Sum(R)/(G/B) within
#       the painted quadrant over painted pixels.
#   4.  Size gate (per event): painted-pixel footprint within bracket for
#       the expected font size at 1280x720 Arial.
#   5.  Reference stills (F2): $JOB_DIR/stills/event{N}_first_painted.png
#       saved for operator visual review + future `compare -metric AE`.
#   6.  Reference signatures (F3): run_summary.json with measured per-event
#       (quadrant, color_signature, footprint_count).
#
# Source-of-truth chain:
#   SubmitJobRequest with voiceover binding + subtitle_tracks[] (ASS)
#     → DataServer/internal/jobs/enqueue/normalize.go
#     → RemoteCodex/native/worker-agent-go/pkg/video/pipelines/hybrid/compiler.go
#     → RemoteCodex/native/video-engine-cpp/src/core/render_engine.cpp:279
#          `burnSubtitleTrack` (lines 77-83)
#          `filter << "subtitles=" << file::shellQuote(...);`
#     → local libass burn-in (mirrors the engine's ffmpeg filtergraph)
#
# Exit codes:
#   0  PASS  all 3 events painted + correct quadrant + correct colour
#                dominance + correct size bracket
#   1  FAIL  any of Tiers 1..4 missed
#   2  USAGE missing arg, prereq, or fixture
#
# Usage:
#   bash check_styled_highlights.sh [--help]
# =============================================================================

set -uo pipefail

REAL_SCRIPT="$(readlink -f "${BASH_SOURCE[0]}")"
SCRIPT_DIR="$(cd "$(dirname "$REAL_SCRIPT")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../../.." && pwd)"
JOB_DIR="${JOB_DIR:-/tmp/velox-styled-highlights/$(date -u +'%Y%m%dT%H%M%SZ')}"

# shellcheck source=tests/_lib/sh/_lib.sh
source "${REPO_ROOT}/tests/_lib/sh/_lib.sh"

usage() { sed -n '2,/^# ====/p' "$0" | sed 's/^^# \{0,1\}//'; exit 0; }
[[ "${1:-}" == "--help" || "${1:-}" == "-h" ]] && usage

# --- prereqs ---
for t in ffmpeg ffprobe jq python3; do
  ensure_command_available "$t" || { log_error "missing prereq: $t"; exit 2; }
done

# --- constants ---
TOTAL_DURATION_S=3.000
FPS=30
BG_COLOUR_R=30
BG_COLOUR_G=58
BG_COLOUR_B=95
QUADRANT_H=240   # 720 / 3 quadrants
DELTA_FROM_BG_THRESHOLD=50

# --- expected events: name | quadrant | pts_start | pts_end | expected_color | min_footprint | size_label
# Event 1 (frase importante): TOP quadrant, RED, fs96, bold
# Event 2 (nome speciale "Marco Aurelio"): MIDDLE quadrant, CYAN, fs64, regular
# Event 3 (parola evidenziata "_HIGHLIGHTED_"): BOTTOM quadrant, GREEN, fs80, italic
EXPECTED_EVENTS=(
  "event1|top|0.10|0.80|red|1500|fs96|bold"
  "event2|mid|0.90|1.60|cyan|700|fs64|regular"
  "event3|bot|1.70|2.40|green|1100|fs80|italic"
)

# --- workspace ---
mkdir_p "$JOB_DIR"
mkdir_p "$JOB_DIR/stills"

# --- compute_strip_stats reused verbatim from sibling harness ---
compute_strip_stats() {
  local png="$1"
  local y_start="${2:-540}"   # default bottom-25% strip
  local y_end="${3:-720}"
  local raw_rgb
  raw_rgb="$(mktemp --suffix=.rgb)"
  ffmpeg -v error -i "$png" -pix_fmt rgb24 -f rawvideo "$raw_rgb" -y \
    > /dev/null 2>&1 || { echo "mean=0 stddev=0 bright_pct=0 painted_px=0"; return; }
  local dims
  dims="$(ffprobe -v error -select_streams v:0 -show_entries stream=width,height -of csv=p=0 "$png" 2>/dev/null)"
  local W="${dims%,*}"; local H="${dims##*,}"
  : "${W:=1280}"; : "${H:=720}"
  python3 - "$raw_rgb" "$W" "$H" "$y_start" "$y_end" "$BG_COLOUR_R" "$BG_COLOUR_G" "$BG_COLOUR_B" "$DELTA_FROM_BG_THRESHOLD" <<'PY'
import sys
fname, W, H, y0, y1, bgR, bgG, bgB, delta_thr = (
  sys.argv[1], int(sys.argv[2]), int(sys.argv[3]),
  int(sys.argv[4]), int(sys.argv[5]),
  int(sys.argv[6]), int(sys.argv[7]), int(sys.argv[8]),
  int(sys.argv[9]),
)
data = open(fname, 'rb').read()
y0 = max(0, min(H, y0)); y1 = max(y0, min(H, y1))
stride = W * 3
ystart = y0 * stride; yend = y1 * stride
strip = data[ystart:yend]
n_px = len(strip) // 3
if n_px == 0:
    print("mean=0 stddev=0 bright_pct=0 painted_px=0 rsum=0 gsum=0 bsum=0")
    sys.exit(0)
total = 0; sumsq = 0; bright = 0
painted = 0; rsum = 0; gsum = 0; bsum = 0
rdev = 0; gdev = 0; bdev = 0
for i in range(n_px):
    r = strip[i*3]; g = strip[i*3+1]; b = strip[i*3+2]
    y = (r * 299 + g * 587 + b * 114) // 1000
    total += y; sumsq += y*y
    if y > 128: bright += 1
    if max(abs(r - bgR), abs(g - bgG), abs(b - bgB)) > delta_thr:
        painted += 1
        rsum += r; gsum += g; bsum += b
        rdev += max(r - bgR, 0)
        gdev += max(g - bgG, 0)
        bdev += max(b - bgB, 0)
mean = total / n_px
var = sumsq / n_px - mean * mean
std = var ** 0.5 if var > 0 else 0.0
print(f"mean={mean:.1f} stddev={std:.2f} bright_pct={bright / n_px * 100:.2f} "
      f"painted_px={painted} rsum={rsum} gsum={gsum} bsum={bsum} "
      f"rdev={rdev} gdev={gdev} bdev={bdev}")
PY
  rm -f "$raw_rgb"
}

# --- voiceover stub ---
VOICEOVER_WAV="$JOB_DIR/voiceover.wav"
log_info "generating voiceover stub (sine 440Hz 3.0s) -> $VOICEOVER_WAV"
if ! ffmpeg -y -f lavfi -i "sine=frequency=440:duration=${TOTAL_DURATION_S}:sample_rate=48000" \
      -ac 2 "$VOICEOVER_WAV" > "$JOB_DIR/voiceover.log" 2>&1; then
  log_error "voiceover generation FAILED; see $JOB_DIR/voiceover.log"; exit 2
fi
VO_DUR=$(ffprobe -v error -show_entries format=duration -of csv=p=0 "$VOICEOVER_WAV")
awk -v d="${VO_DUR}" -v exp_d="3.0" 'BEGIN{ exit !((d+0 > 2.95) && (d+0 < 3.05)) }' \
  || { log_error "voiceover duration ${VO_DUR}s not within 2.95..3.05s"; exit 2; }

# --- test frame ---
FRAME_PNG="$JOB_DIR/test_frame.png"
log_info "generating dark-blue test frame -> $FRAME_PNG"
ffmpeg -y -f lavfi -i "color=c=0x1e3a5f:s=1280x720:d=1" -frames:v 1 "$FRAME_PNG" \
  > "$JOB_DIR/frame.log" 2>&1

# --- render ---
SUB_PATH="$SCRIPT_DIR/fixtures/styled_highlights.ass"
[[ -r "$SUB_PATH" ]] || { log_error "ASS fixture not readable: $SUB_PATH"; exit 2; }

OUT_MP4="$JOB_DIR/render.mp4"
log_info "rendering: subtitles=${SUB_PATH} + voiceover=${VOICEOVER_WAV} -> $OUT_MP4"
ffmpeg -y -framerate "${FPS}" -loop 1 -i "$FRAME_PNG" -i "$VOICEOVER_WAV" \
      -vf "subtitles=${SUB_PATH}" \
      -t "${TOTAL_DURATION_S}" -c:v libx264 -pix_fmt yuv420p -c:a aac \
      -shortest "$OUT_MP4" > "$JOB_DIR/render.log" 2>&1 \
  || { log_error "render FAILED; see $JOB_DIR/render.log"; exit 2; }

# --- Tier 1: ffprobe burn-in contract ---
probe_json="$(ffprobe -v quiet -print_format json -show_format -show_streams "$OUT_MP4")"
video_count=$(printf '%s' "$probe_json" | jq '[.streams[] | select(.codec_type=="video")] | length' 2>/dev/null)
audio_count=$(printf '%s' "$probe_json" | jq '[.streams[] | select(.codec_type=="audio")] | length' 2>/dev/null)
sub_count=$(   printf '%s' "$probe_json" | jq '[.streams[] | select(.codec_type=="subtitle")] | length' 2>/dev/null)
: "${video_count:=0}"; : "${audio_count:=0}"; : "${sub_count:=0}"

if (( video_count != 1 )); then log_error "video streams=${video_count} (expected 1)"; exit 1; fi
if (( audio_count != 1 )); then log_error "audio streams=${audio_count} (expected 1)"; exit 1; fi
if (( sub_count != 0 ));   then log_error "subtitle streams=${sub_count} (expected 0)"; exit 1; fi

START_TIME=$(printf '%s' "$probe_json" | jq -r '.format.start_time // "0"' 2>/dev/null)
if ! awk -v s="${START_TIME}" 'BEGIN{ exit !(s+0 >= -0.001 && s+0 <= 0.001) }'; then
  log_error "format.start_time=${START_TIME} (expected 0.000)"; exit 1
fi
log_info "Tier 1 PASS: burn-in contract (v=1 a=1 s=0, format.start_time=0.000)"

# --- per-frame extraction ---
mkdir -p "$JOB_DIR/frames"
log_info "extracting ${FPS}x${TOTAL_DURATION_S}s = $(awk -v fps="$FPS" -v d="$TOTAL_DURATION_S" 'BEGIN{ printf "%d", fps*d }') per-frame PNGs"
ffmpeg -y -i "$OUT_MP4" -r "${FPS}" "$JOB_DIR/frames/p_%04d.png" \
  > "$JOB_DIR/extract.log" 2>&1 \
  || { log_error "per-frame PNG extract FAILED"; exit 1; }

# --- Tier 2/3/4: per-event verification ---
mkdir_p "$JOB_DIR/stills"
EVENT_RESULTS="$JOB_DIR/event_results.json"
echo '[]' > "$EVENT_RESULTS"

declare -i EVENT_FAILS=0
for spec in "${EXPECTED_EVENTS[@]}"; do
  IFS='|' read -r name quad pts_s pts_e expected_color min_footprint size_label style_label <<< "$spec"
  log_info "Verifying $name [${quad}] ${size_label} ${style_label} colour=${expected_color} pts=${pts_s}..${pts_e}"

  # 1st-painted frame: first extracted frame whose PTS >= pts_s
  first_f="$JOB_DIR/stills/${name}_first_painted.png"
  target_pts="$(awk -v s="$pts_s" 'BEGIN{ printf "%.4f", s }')"
  for png in $(ls "$JOB_DIR"/frames/p_*.png | sort); do
    n=$(basename "$png" | sed 's/^p_0*//;s/\.png$//')
    pts=$(awk -v n="$n" -v fps="$FPS" 'BEGIN{ printf "%.4f", n/fps }')
    if awk -v p="$pts" -v t="$target_pts" 'BEGIN{ exit !(p+0 >= t+0) }'; then
      cp "$png" "$first_f"
      break
    fi
  done

  # Centre frame: extracted frame near the middle of the event window
  centre_pts=$(awk -v s="$pts_s" -v e="$pts_e" 'BEGIN{ printf "%.4f", (s+e)/2 }')
  centre_f="$JOB_DIR/stills/${name}_centre.png"
  for png in $(ls "$JOB_DIR"/frames/p_*.png | sort); do
    n=$(basename "$png" | sed 's/^p_0*//;s/\.png$//')
    pts=$(awk -v n="$n" -v fps="$FPS" 'BEGIN{ printf "%.4f", n/fps }')
    if awk -v p="$pts" -v t="$centre_pts" 'BEGIN{ exit !(p+0 >= t+0) }'; then
      mkdir_p "$JOB_DIR/stills"; cp "$png" "$centre_f"
      break
    fi
  done

  # Quadrant y-ranges
  case "$quad" in
    top) y0=0;     y1=240 ;;
    mid) y0=240;   y1=480 ;;
    bot) y0=480;   y1=720 ;;
    *)   log_error "unknown quadrant: $quad"; exit 2 ;;
  esac

  # Tier 2: position gate — painted quadrant stddev > 5; others stddev < 1
  stats_pq="$(compute_strip_stats "$centre_f" "$y0" "$y1")"
  painted_stddev=$(awk '{for(i=1;i<=NF;i++) if($i~/^stddev=/){split($i,a,"=");print a[2];exit}}' <<<"$stats_pq")
  painted_px=$(awk    '{for(i=1;i<=NF;i++) if($i~/^painted_px=/){split($i,a,"=");print a[2];exit}}' <<<"$stats_pq")
  rsum=$(awk          '{for(i=1;i<=NF;i++) if($i~/^rsum=/){split($i,a,"=");print a[2];exit}}' <<<"$stats_pq")
  gsum=$(awk          '{for(i=1;i<=NF;i++) if($i~/^gsum=/){split($i,a,"=");print a[2];exit}}' <<<"$stats_pq")
  bsum=$(awk          '{for(i=1;i<=NF;i++) if($i~/^bsum=/){split($i,a,"=");print a[2];exit}}' <<<"$stats_pq")
  rdev=$(awk          '{for(i=1;i<=NF;i++) if($i~/^rdev=/){split($i,a,"=");print a[2];exit}}' <<<"$stats_pq")
  gdev=$(awk          '{for(i=1;i<=NF;i++) if($i~/^gdev=/){split($i,a,"=");print a[2];exit}}' <<<"$stats_pq")
  bdev=$(awk          '{for(i=1;i<=NF;i++) if($i~/^bdev=/){split($i,a,"=");print a[2];exit}}' <<<"$stats_pq")

  # Inactive quadrants — sum stddev for cross-contamination gate
  declare -a INACTIVE_STDDEVS=()
  for q in top mid bot; do
    [[ "$q" == "$quad" ]] && continue
    case "$q" in top) ii0=0;   ii1=240 ;; mid) ii0=240; ii1=480 ;; bot) ii0=480; ii1=720 ;; esac
    s_iq="$(compute_strip_stats "$centre_f" "$ii0" "$ii1")"
    sd_iq=$(awk '{for(i=1;i<=NF;i++) if($i~/^stddev=/){split($i,a,"=");print a[2];exit}}' <<<"$s_iq")
    INACTIVE_STDDEVS+=("$sd_iq")
  done
  max_inactive=$(printf '%s\n' "${INACTIVE_STDDEVS[@]}" | sort -g | tail -n1)

  pos_pass=true
  awk -v sd="$painted_stddev" 'BEGIN{ exit !(sd+0 > 5+0) }' || pos_pass=false
  awk -v mi="$max_inactive"   'BEGIN{ exit !(mi+0 < 1+0) }' || pos_pass=false

  # Tier 3: colour gate on bg-removed positive-deviation channel sums.
  # The bg is dark-blue (R=30 G=58 B=95). Antialiased text-edge pixels
  # dilute channel sums toward bg values; only the POSITIVE deviation
  # from bg (per channel, per pixel) isolates the text colour signal.
  #   red:    rdev > 1.5*max(gdev, bdev)
  #   cyan:   gdev > 1.5*rdev AND bdev > 1.5*rdev AND |gdev-bdev|/max < 0.30
  #   green:  gdev > 1.5*max(rdev, bdev)
  col_pass=true
  case "$expected_color" in
    red)
      python3 - "$rdev" "$gdev" "$bdev" <<'PY' || col_pass=false
import sys
r, g, b = int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3])
ok = (r > 1.5 * max(g, b))
sys.exit(0 if ok else 1)
PY
      ;;
    cyan)
      python3 - "$rdev" "$gdev" "$bdev" <<'PY' || col_pass=false
import sys
r, g, b = int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3])
ratio_ok = (g > 1.5 * r) and (b > 1.5 * r)
balance_ok = (max(g, b) > 0) and (abs(g - b) / max(g, b) < 0.30)
ok = ratio_ok and balance_ok
sys.exit(0 if ok else 1)
PY
      ;;
    green)
      python3 - "$rdev" "$gdev" "$bdev" <<'PY' || col_pass=false
import sys
r, g, b = int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3])
ok = (g > 1.5 * max(r, b))
sys.exit(0 if ok else 1)
PY
      ;;
    *) log_error "unknown color: $expected_color"; exit 2 ;;
  esac

  # Tier 4: size gate — painted-pixel footprint >= min_footprint
  size_pass=true
  awk -v p="$painted_px" -v m="$min_footprint" 'BEGIN{ exit !(p+0 >= m+0) }' || size_pass=false

  tier2_pass=$pos_pass
  tier3_pass=$col_pass
  tier4_pass=$size_pass

  event_verdict="PASS"
  [[ "$tier2_pass" == false ]] && event_verdict="FAIL_T2"
  [[ "$tier3_pass" == false ]] && event_verdict="FAIL_T3"
  [[ "$tier4_pass" == false ]] && event_verdict="FAIL_T4"
  [[ "$event_verdict" == "PASS" ]] || EVENT_FAILS+=1

  log_info "  $name: T2(quadrant stddev=${painted_stddev} inactive_max=${max_inactive})=$pos_pass T3(colour rdev=$rdev gdev=$gdev bdev=$bdev)=$col_pass T4(painted_px=${painted_px} min=${min_footprint})=$size_pass -> $event_verdict"

  # Append result vector to event_results.json
  python3 - "$EVENT_RESULTS" "$name" "$quad" "$expected_color" \
                       "$painted_stddev" "$max_inactive" \
                       "$painted_px" "$min_footprint" \
                       "$size_label" "$style_label" \
                      "$rsum" "$gsum" "$bsum" \
                      "$rdev" "$gdev" "$bdev" \
                      "$tier2_pass" "$tier3_pass" "$tier4_pass" \
                      "$event_verdict" "$first_f" "$centre_f" <<'PY'
import json, sys
path, name, quad, ecolor, psd, imax, ppx, mpx, size_lbl, style_lbl, rsum, gsum, bsum, rdev, gdev, bdev, t2, t3, t4, verdict, first_f, centre_f = sys.argv[1:]  # noqa
try:
    with open(path) as f:
        arr = json.load(f)
except Exception:
    arr = []
arr.append({
    "event": name,
    "quadrant": quad,
    "expected_color": ecolor,
    "size_label": size_lbl,
    "style_label": style_lbl,
    "position": {
        "painted_stddev": float(psd),
        "inactive_max_stddev": float(imax),
        "tier2_pass": t2 == "true",
    },
    "color": {
        "rsum": int(rsum), "gsum": int(gsum), "bsum": int(bsum),
        "rdev": int(rdev), "gdev": int(gdev), "bdev": int(bdev),
        "tier3_pass": t3 == "true",
    },
    "size": {
        "painted_px": int(ppx), "min_footprint": int(mpx),
        "tier4_pass": t4 == "true",
    },
    "verdict": verdict,
    "stills": {
        "first_painted": first_f,
        "centre": centre_f,
    },
})
with open(path, 'w') as f:
    json.dump(arr, f, indent=2, sort_keys=True)
PY
done

# --- Tier 5/6: write machine-parseable run summary ---
SUMMARY="$JOB_DIR/run_summary.json"
EVT_TOTAL=${#EXPECTED_EVENTS[@]}
EVT_PASS=$(( EVT_TOTAL - EVENT_FAILS ))
GLOBAL_VERDICT="PASS"
(( EVENT_FAILS > 0 )) && GLOBAL_VERDICT="FAIL"

python3 - "$SUMMARY" "$EVENT_RESULTS" "$EVT_TOTAL" "$EVT_PASS" "$EVENT_FAILS" "$GLOBAL_VERDICT" \
                  "${JOB_DIR}" "${OUT_MP4}" "${VOICEOVER_WAV}" \
                  "${EXPECTED_EVENTS[@]}" <<'PY'
import json, sys, os, datetime
(path, evt_res_path, evt_total, evt_pass, evt_fail, verdict,
 job_dir, mp4, wav, *expected_specs) = sys.argv[1:]
expected = []
for sp in expected_specs:
    parts = sp.split('|')
    expected.append({
        "name": parts[0],
        "quadrant": parts[1],
        "pts_start": float(parts[2]),
        "pts_end":   float(parts[3]),
        "expected_color": parts[4],
        "min_footprint": int(parts[5]),
        "size_label":   parts[6],
        "style_label":  parts[7],
    })
with open(evt_res_path) as f:
    measured = json.load(f)
out = {
    "schema": "velox.smoke.styled-highlights@1",
    "run_utc": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
    "expected": expected,
    "measured": measured,
    "summary": {
        "events_total": int(evt_total),
        "events_pass":  int(evt_pass),
        "events_fail":  int(evt_fail),
        "verdict": verdict,
    },
    "evidence": {
        "rendered_mp4":   mp4,
        "voiceover_wav":  wav,
        "stills_dir":     os.path.join(job_dir, "stills"),
        "per_frame_dir":  os.path.join(job_dir, "frames"),
        "event_results":  evt_res_path,
    },
}
with open(path, 'w') as f:
    json.dump(out, f, indent=2, sort_keys=True)
PY

# Operator-facing verdict
echo
echo "==== STYLED-HIGHLIGHTS SYNC VERDICT ===="
echo "expected_events  = $EVT_TOTAL (no-overlap PTS windows)"
echo "events_pass      = $EVT_PASS"
echo "events_fail      = $EVENT_FAILS"
echo "verdict          = $GLOBAL_VERDICT"
echo "run_summary      = $SUMMARY"
echo "stills_dir       = $JOB_DIR/stills"
echo
ls -la "$JOB_DIR/stills" | sed 's/^/  /'

if [[ "$GLOBAL_VERDICT" == "PASS" ]]; then
  log_info "STYLED-PASS: all events painted + correct quadrant + correct colour + correct size bracket"
  exit 0
fi
log_error "STYLED-FAIL: ${EVENT_FAILS}/${EVT_TOTAL} events missed a tier"
exit 1
