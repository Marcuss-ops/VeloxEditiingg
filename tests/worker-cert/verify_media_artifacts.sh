#!/usr/bin/env bash
# =============================================================================
# verify_media_artifacts.sh — final media evidence validator.
# =============================================================================
# Usage:
#   verify_media_artifacts.sh --report-json report.json --evidence-dir evidence \
#     [--expected-duration-s 12] [--duration-tolerance-s 0.5] \
#     [--ass-source subtitle.ass] artifact.mp4 [...]
#
# Exit codes:
#   0 PASS       all machine checks pass and no manual review remains
#   1 FAIL       at least one machine-verifiable check failed
#   2 usage / missing tool
#   3 REVIEW_REQUIRED / machine checks passed but perceptual review remains
#
# The final mux cannot prove semantic voiceover/music presence or perceptual
# ASS style correctness without source tracks/reference frames. Those checks
# are explicitly REVIEW_REQUIRED; they are never silently reported as PASS.
# =============================================================================

set -uo pipefail

SCRIPT_VERSION="verify_media_artifacts@2"
EXPECTED_DURATION_S="${VERIFY_EXPECTED_DURATION_S:-12}"
DURATION_TOLERANCE_S="${VERIFY_DURATION_TOLERANCE_S:-0.5}"
EXPECTED_VIDEO_CODEC="${VERIFY_VIDEO_CODEC:-h264}"
EXPECTED_AUDIO_CODEC="${VERIFY_AUDIO_CODEC:-aac}"
REPORT_JSON=""
EVIDENCE_DIR=""
ASS_SOURCE=""
ARTIFACTS=()
TMP_DIR=""

usage() {
  sed -n '2,/^# ====/p' "$0" | sed 's/^# \{0,1\}//'
  exit "${1:-0}"
}
fail_usage() { printf 'error: %s\n' "$1" >&2; usage 2; }

while (( $# > 0 )); do
  case "$1" in
    --report-json) [[ $# -ge 2 ]] || fail_usage "--report-json requires a path"; REPORT_JSON="$2"; shift 2 ;;
    --evidence-dir) [[ $# -ge 2 ]] || fail_usage "--evidence-dir requires a directory"; EVIDENCE_DIR="$2"; shift 2 ;;
    --expected-duration-s) [[ $# -ge 2 ]] || fail_usage "--expected-duration-s requires a number"; EXPECTED_DURATION_S="$2"; shift 2 ;;
    --duration-tolerance-s) [[ $# -ge 2 ]] || fail_usage "--duration-tolerance-s requires a number"; DURATION_TOLERANCE_S="$2"; shift 2 ;;
    --ass-source) [[ $# -ge 2 ]] || fail_usage "--ass-source requires a path"; ASS_SOURCE="$2"; shift 2 ;;
    --help|-h) usage 0 ;;
    --*) fail_usage "unknown option: $1" ;;
    *) ARTIFACTS+=("$1"); shift ;;
  esac
done

[[ -n "$REPORT_JSON" ]] || fail_usage "--report-json is required"
[[ -n "$EVIDENCE_DIR" ]] || fail_usage "--evidence-dir is required"
(( ${#ARTIFACTS[@]} > 0 )) || fail_usage "at least one final artifact is required"
for value_name in EXPECTED_DURATION_S DURATION_TOLERANCE_S; do
  value="${!value_name}"
  [[ "$value" =~ ^[0-9]+([.][0-9]+)?$ ]] || fail_usage "$value_name must be numeric"
done
[[ -z "$ASS_SOURCE" || -r "$ASS_SOURCE" ]] || { printf 'error: ASS source is not readable: %s\n' "$ASS_SOURCE" >&2; exit 2; }

for binary in ffprobe ffmpeg jq sha256sum awk sed grep stat mktemp mkdir mv; do
  command -v "$binary" >/dev/null 2>&1 || { printf 'error: required binary missing: %s\n' "$binary" >&2; exit 2; }
done

mkdir -p "$EVIDENCE_DIR" "$(dirname "$REPORT_JSON")" || { printf 'error: cannot create evidence/report directories\n' >&2; exit 2; }
TMP_DIR="$(mktemp -d)" || exit 2
trap 'rm -rf "$TMP_DIR"' EXIT INT TERM

numeric_close() {
  awk -v actual="$1" -v expected="$2" -v tolerance="$3" \
    'BEGIN { d=actual-expected; if (d<0) d=-d; exit !(d<=tolerance) }'
}

add_check() {
  local array="$1" name="$2" status="$3" detail="$4"
  jq --arg n "$name" --arg s "$status" --arg d "$detail" \
    '. + [{name:$n,status:$s,detail:$d}]' <<<"$array"
}

ass_checks() {
  local source="$1" checks='[]' required_ok=1 timing_ok=1 override_ok=1
  grep -Eq '^\[V4\+ Styles\]' "$source" && checks="$(add_check "$checks" ass_styles PASS '[V4+ Styles] present')" || { checks="$(add_check "$checks" ass_styles FAIL '[V4+ Styles] section missing')"; required_ok=0; }
  grep -Eq '^Format:.*Outline.*Shadow.*MarginL.*MarginR.*MarginV' "$source" || required_ok=0
  grep -Eq '^Style:.*,[^,]*,[^,]*,[^,]*,[^,]*,[^,]*,[^,]*,[^,]*,[^,]*,[^,]*,[^,]*,[^,]*,[^,]*,[^,]*,[^,]*,[^,]*,[0-9]+,[0-9]+,' "$source" || required_ok=0
  [[ "$required_ok" == 1 ]] && checks="$(add_check "$checks" ass_layout_style PASS 'outline/shadow/margins present')" || checks="$(add_check "$checks" ass_layout_style FAIL 'outline/shadow/margins incomplete')"
  grep -Eq '^Dialogue:[[:space:]]*[0-9]+,0:00:00(\.[0-9]+)?,0:00:04(\.[0-9]+)?,' "$source" || timing_ok=0
  grep -Eq '^Dialogue:[[:space:]]*[0-9]+,0:00:04(\.[0-9]+)?,0:00:08(\.[0-9]+)?,' "$source" || timing_ok=0
  grep -Eq '^Dialogue:[[:space:]]*[0-9]+,0:00:08(\.[0-9]+)?,0:00:12(\.[0-9]+)?,' "$source" || timing_ok=0
  [[ "$timing_ok" == 1 ]] && checks="$(add_check "$checks" ass_timing PASS '0-4/4-8/8-12 events present')" || checks="$(add_check "$checks" ass_timing FAIL 'required 0-4/4-8/8-12 events missing')"
  grep -Eq '\\(c|\\b|\\fs|\\pos)' "$source" && override_ok=0
  if [[ "$override_ok" == 0 ]]; then
    checks="$(add_check "$checks" ass_overrides PASS 'simple ASS override token present (color/bold/size/position)')"
  else
    checks="$(add_check "$checks" ass_overrides FAIL 'no \\c, \\b, \\fs or \\pos override found')"
  fi
  printf '%s' "$checks"
}

RESULT_FILES=()
OVERALL_STATUS="PASS"

for index in "${!ARTIFACTS[@]}"; do
  artifact="${ARTIFACTS[$index]}"
  base="$(basename "$artifact")"
  dir="$EVIDENCE_DIR/${base%.*}-$((index+1))"
  mkdir -p "$dir" || { printf 'error: cannot create %s\n' "$dir" >&2; exit 2; }
  checks='[]'
  status="PASS"
  bytes=0
  sha=""

  if [[ ! -r "$artifact" ]]; then
    checks="$(add_check "$checks" file_readable FAIL 'artifact is not readable')"
    status="FAIL"
  else
    bytes="$(stat -c '%s' "$artifact" 2>/dev/null || echo 0)"
    sha="$(sha256sum "$artifact" | awk '{print $1}')"
    if (( bytes <= 0 )); then
      checks="$(add_check "$checks" file_size FAIL 'artifact is empty')"
      status="FAIL"
    else
      checks="$(add_check "$checks" file_size PASS "${bytes} bytes")"        probe="$dir/ffprobe.json"
        probe_tmp="$(mktemp -p "$dir" '.ffprobe.XXXXXX')"
        stderr_tmp="$(mktemp -p "$dir" '.ffprobe-stderr.XXXXXX')"
      if ffprobe -v error -show_streams -show_format -of json "$artifact" >"$probe_tmp" 2>"$stderr_tmp" && mv -f "$probe_tmp" "$probe" && mv -f "$stderr_tmp" "$dir/ffprobe.stderr"; then
        checks="$(add_check "$checks" ffprobe PASS 'ffprobe completed')"
      else
        mv -f "$probe_tmp" "$probe" 2>/dev/null || true
        mv -f "$stderr_tmp" "$dir/ffprobe.stderr" 2>/dev/null || true
        checks="$(add_check "$checks" ffprobe FAIL 'ffprobe failed; see ffprobe.stderr')"
        status="FAIL"
      fi

      if [[ -s "$probe" ]]; then
        video_count="$(jq '[.streams[] | select(.codec_type=="video")] | length' "$probe")"
        audio_count="$(jq '[.streams[] | select(.codec_type=="audio")] | length' "$probe")"
        subtitle_count="$(jq '[.streams[] | select(.codec_type=="subtitle")] | length' "$probe")"
        video_codec="$(jq -r '[.streams[] | select(.codec_type=="video")][0].codec_name // ""' "$probe")"
        audio_codec="$(jq -r '[.streams[] | select(.codec_type=="audio")][0].codec_name // ""' "$probe")"
        duration="$(jq -r '.format.duration // "0"' "$probe")"
        if [[ "$video_count" == 1 && "$audio_count" == 1 && "$subtitle_count" == 0 ]]; then
          checks="$(add_check "$checks" stream_layout PASS 'video=1 audio=1 subtitle=0; subtitles are expected burned-in')"
        else
          checks="$(add_check "$checks" stream_layout FAIL "video=$video_count audio=$audio_count subtitle=$subtitle_count")"
          status="FAIL"
        fi
        if [[ "$video_codec" == "$EXPECTED_VIDEO_CODEC" && "$audio_codec" == "$EXPECTED_AUDIO_CODEC" ]]; then
          checks="$(add_check "$checks" codecs PASS "video=$video_codec audio=$audio_codec")"
        else
          checks="$(add_check "$checks" codecs FAIL "video=$video_codec audio=$audio_codec expected=$EXPECTED_VIDEO_CODEC/$EXPECTED_AUDIO_CODEC")"
          status="FAIL"
        fi
        if numeric_close "$duration" "$EXPECTED_DURATION_S" "$DURATION_TOLERANCE_S"; then
          checks="$(add_check "$checks" duration PASS "${duration}s expected=${EXPECTED_DURATION_S}s tolerance=${DURATION_TOLERANCE_S}s")"
        else
          checks="$(add_check "$checks" duration FAIL "${duration}s expected=${EXPECTED_DURATION_S}s tolerance=${DURATION_TOLERANCE_S}s")"
          status="FAIL"
        fi

        frames_ok=1
        for timestamp in 2 6 10; do
          frame="$dir/frame_${timestamp}s.png"
          frame_tmp="$(mktemp -p "$dir" ".frame_${timestamp}s.XXXXXX")"
          frame_err_tmp="$(mktemp -p "$dir" ".frame_${timestamp}s-stderr.XXXXXX")"
          if ! ffmpeg -hide_banner -loglevel error -ss "$timestamp" -i "$artifact" -frames:v 1 -f image2 -y "$frame_tmp" > /dev/null 2>"$frame_err_tmp" || [[ ! -s "$frame_tmp" ]]; then
            mv -f "$frame_err_tmp" "$dir/frame_${timestamp}s.stderr" 2>/dev/null || true
            rm -f "$frame_tmp"
            frames_ok=0
          else
            mv -f "$frame_tmp" "$frame"
            mv -f "$frame_err_tmp" "$dir/frame_${timestamp}s.stderr"
          fi
        done
        if [[ "$frames_ok" == 1 ]]; then
          checks="$(add_check "$checks" frame_extraction PASS 'frames at 2s, 6s and 10s extracted')"
        else
          checks="$(add_check "$checks" frame_extraction FAIL 'one or more requested frames failed')"
          status="FAIL"
        fi

        ebur_stdout_tmp="$(mktemp -p "$dir" '.ebur128-stdout.XXXXXX')"
        ebur_log_tmp="$(mktemp -p "$dir" '.ebur128-log.XXXXXX')"
        if ffmpeg -hide_banner -nostats -i "$artifact" -filter_complex '[0:a]ebur128=peak=true:framelog=verbose' -f null - >"$ebur_stdout_tmp" 2>"$ebur_log_tmp" && mv -f "$ebur_stdout_tmp" "$dir/ebur128.stdout" && mv -f "$ebur_log_tmp" "$dir/ebur128.log"; then
          peak="$(awk '/True peak:/{getline; print}' "$dir/ebur128.log" "$dir/ebur128.stdout" 2>/dev/null | grep -Eo -- '[-+]?[0-9]+([.][0-9]+)?' | head -1)"
          integrated="$(grep -hE '^[[:space:]]*I:' "$dir/ebur128.log" "$dir/ebur128.stdout" 2>/dev/null | tail -1 | grep -Eo -- '[-+]?[0-9]+([.][0-9]+)?' | head -1)"
          if [[ "$peak" =~ ^[-+]?[0-9]+([.][0-9]+)?$ ]] && awk -v p="$peak" 'BEGIN { exit !(p < -1) }'; then
            checks="$(add_check "$checks" ebur128_clipping PASS "integrated_lufs=${integrated:-unknown} true_peak_dbfs=$peak (< -1 dBFS)")"
          else
            checks="$(add_check "$checks" ebur128_clipping FAIL "integrated_lufs=${integrated:-unknown} true_peak_dbfs=${peak:-unknown}; required < -1 dBFS")"
            status="FAIL"
          fi
        else
          mv -f "$ebur_stdout_tmp" "$dir/ebur128.stdout" 2>/dev/null || true
          mv -f "$ebur_log_tmp" "$dir/ebur128.log" 2>/dev/null || true
          checks="$(add_check "$checks" ebur128 FAIL 'ebur128 failed; see ebur128.log')"
          status="FAIL"
        fi

        checks="$(add_check "$checks" loudness_volume REVIEW_REQUIRED 'integrated LUFS is recorded; validate target loudness against the production audio spec')"
        checks="$(add_check "$checks" voiceover_presence REVIEW_REQUIRED 'mixed final audio cannot prove source voiceover presence; listen against source')"
        checks="$(add_check "$checks" background_music_presence REVIEW_REQUIRED 'mixed final audio cannot prove music presence; listen for music under voiceover')"
        checks="$(add_check "$checks" voiceover_sync REVIEW_REQUIRED 'compare voiceover waveform/listening against scene timeline')"
        checks="$(add_check "$checks" ass_burn_in_styles REVIEW_REQUIRED 'inspect extracted frames for color, size, position and burn-in')"
        if [[ "$status" == PASS ]]; then
          status="REVIEW_REQUIRED"
          [[ "$OVERALL_STATUS" == PASS ]] && OVERALL_STATUS="REVIEW_REQUIRED"
        fi
      else
        status="FAIL"
      fi
    fi
  fi

  if [[ -n "$ASS_SOURCE" ]]; then
    ass_file="$dir/ass_checks.json"
    ass_tmp="$(mktemp -p "$dir" '.ass-checks.XXXXXX')"
    if ! ass_checks "$ASS_SOURCE" >"$ass_tmp" || ! mv -f "$ass_tmp" "$ass_file"; then
      checks="$(add_check "$checks" ass_source FAIL 'ASS source validation failed')"
      status="FAIL"
    else
      checks="$(jq --slurpfile ass "$ass_file" '. + $ass[0]' <<<"$checks")"
      if jq -e '.[] | select(.status=="FAIL")' "$ass_file" >/dev/null; then
        status="FAIL"
      fi
    fi
  else
    checks="$(add_check "$checks" ass_source REVIEW_REQUIRED 'no ASS source supplied for contract comparison')"
    if [[ "$status" == PASS ]]; then
      status="REVIEW_REQUIRED"
      [[ "$OVERALL_STATUS" == PASS ]] && OVERALL_STATUS="REVIEW_REQUIRED"
    fi
  fi

  result="$TMP_DIR/result-$index.json"
  jq -n --arg path "$artifact" --arg sha "$sha" --arg status "$status" --arg evidence_dir "$dir" --argjson bytes "$bytes" --argjson checks "$checks" \
    '{artifact_path:$path,sha256:$sha,size_bytes:$bytes,status:$status,evidence_dir:$evidence_dir,checks:$checks}' >"$result" || exit 2
  RESULT_FILES+=("$result")
  case "$status" in
    FAIL) OVERALL_STATUS="FAIL" ;;
    REVIEW_REQUIRED) [[ "$OVERALL_STATUS" != FAIL ]] && OVERALL_STATUS="REVIEW_REQUIRED" ;;
  esac
done

all_results="$(jq -s '.' "${RESULT_FILES[@]}")" || exit 2
report_tmp="$(mktemp -p "$(dirname "$REPORT_JSON")" ".$(basename "$REPORT_JSON").partial.XXXXXX")"
jq -n --arg schema "$SCRIPT_VERSION" --arg status "$OVERALL_STATUS" --arg expected "$EXPECTED_DURATION_S" --arg tolerance "$DURATION_TOLERANCE_S" --argjson artifacts "$all_results" \
  '{schema:$schema,generated_at:(now|todateiso8601),overall_status:$status,expected_duration_seconds:($expected|tonumber),duration_tolerance_seconds:($tolerance|tonumber),artifacts:$artifacts}' >"$report_tmp" || exit 2
mv -f "$report_tmp" "$REPORT_JSON" || { printf 'error: cannot atomically publish report: %s\n' "$REPORT_JSON" >&2; exit 2; }
printf 'media validation: %s artifacts=%s report=%s\n' "$OVERALL_STATUS" "${#ARTIFACTS[@]}" "$REPORT_JSON"
case "$OVERALL_STATUS" in
  PASS) exit 0 ;;
  FAIL) exit 1 ;;
  REVIEW_REQUIRED) exit 3 ;;
  *) exit 2 ;;
esac
