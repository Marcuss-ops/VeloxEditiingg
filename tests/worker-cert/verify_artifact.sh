#!/usr/bin/env bash
# =============================================================================
# tests/worker-cert/verify_artifact.sh — Artifact verification harness.
# =============================================================================
# Usage:
#   ./tests/worker-cert/verify_artifact.sh <output.mp4> [OPTIONS]
#
# What the script does (offline checks, always run):
#   1. Asserts <output.mp4> exists and size > 0.
#   2. Runs `ffprobe -v error -show_format -show_streams` and parses the
#      resulting key=value lines into a flat report.
#   3. Validates:
#        - ≥1 video stream with codec_name=h264
#        - ≥1 audio stream with codec_name=aac (unless --allow-no-audio)
#        - duration_seconds between --min-duration-s and --max-duration-s
#        - width ≥ --min-width
#        - height ≥ --min-height
#        - fps ≥ --min-fps (computed from avg_frame_rate "num/den")
#   4. Computes SHA-256 of the artifact via `sha256sum`.
#
# Master-side checks (opt-in; require --job-id + --master-url + bearer):
#   5. GET /api/v1/jobs/<job_id> with bearer; asserts HTTP 200 +
#      .status == --expect-status (default SUCCEEDED). Per the openapi
#      contract (SubmitJobStatusResponse), status=SUCCEEDED is the canonical
#      monotonic guarantee that (a) the artifact has been committed (its
#      CRC bytes are stored in artifacts.bytes_size on the master) and
#      (b) all delivery_plan entries reached job_deliveries.SUBMITTED.
#   6. If --master-blob-path <path> is set, computes the SHA-256 of the
#      master-replica blob and compares with the local file's SHA-256
#      (proves the master-replica round-trip transports the file
#      bit-identical).
#
# Output:
#   - One-line PASS/FAIL summary on stdout.
#   - Atomic per-check JSON report via --report-json (tmp+mv same fs).
#   - Quoted CHECK entries via log_info / log_warn / log_error so a
#     transcript-driven CI can pull individual verdicts.
#
# Exit codes:
#   0  PASS — all assertions ok
#   2  usage / bad args / missing required binaries
#   3  file not readable / size 0
#   4  ffprobe exec/parse failure or threshold/codec failure
#   5  master-side status mismatch (job did not reach expected status)
#   6  master-side network failure (curl could not reach master)
#   7  master-side SHA mismatch (--master-blob-path path differs from local)
#
# Exit-code semantics: the file-readable / file-size / size-zero checks
# (exit 3) are TERMINAL — they short-circuit the rest of the script via
# explicit `exit 3` so they cannot be overwritten by a downstream 4/5/6/7.
# All other checks contribute to OVERALL_FATAL_RC on a LAST-UPDATE-WINS
# basis (4 = ffprobe/threshold, 5 = master-side HTTP semantic 4xx, 6 =
# master-side transport, 7 = master-side blob SHA mismatch). Operators
# reading the per-check JSON report should rely on the per-check status
# field, not on OVERALL_FATAL_RC alone, for full diagnostics.
# =============================================================================

set -uo pipefail  # NOT -e: keep going through each check so the report captures all verdicts

# ─── EXIT trap: tmp cleanup ────────────────────────────────────────────────
# Defensive cleanup on every exit path (success/failure/signals) so the
# verify TMPDIR scratch is never left behind after a CI run.
TMP_FFPROBE=""
TMP_STREAMS=""
TMP_FFPROBE_ERR=""
TMP_JOB_BODY=""
TMP_JOB_STATUS=""
TMP_REPORT=""
on_exit_cleanup() {
  local rc=$?
  [[ -n "$TMP_FFPROBE"  && -e "$TMP_FFPROBE"  ]] && rm -f "$TMP_FFPROBE"  || true
  [[ -n "$TMP_STREAMS"  && -e "$TMP_STREAMS"  ]] && rm -f "$TMP_STREAMS"  || true
  [[ -n "$TMP_FFPROBE_ERR" && -e "$TMP_FFPROBE_ERR" ]] && rm -f "$TMP_FFPROBE_ERR" || true
  [[ -n "$TMP_JOB_BODY" && -e "$TMP_JOB_BODY" ]] && rm -f "$TMP_JOB_BODY" || true
  [[ -n "$TMP_JOB_STATUS" && -e "$TMP_JOB_STATUS" ]] && rm -f "$TMP_JOB_STATUS" || true
  [[ -n "$TMP_REPORT"   && -e "$TMP_REPORT"   ]] && rm -f "$TMP_REPORT"   || true
  exit "$rc"
}
trap on_exit_cleanup EXIT INT TERM

REAL_SCRIPT="$(readlink -f "${BASH_SOURCE[0]}")"
SCRIPT_DIR="$(cd "$(dirname "$REAL_SCRIPT")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# ─── Cross-test helpers ────────────────────────────────────────────────────
# shellcheck source=tests/_lib/sh/_lib.sh
source "${REPO_ROOT}/tests/_lib/sh/_lib.sh"

# ─── Args / defaults ──────────────────────────────────────────────────────
usage() {
  sed -n '2,/^# ====/p' "$0" | sed 's/^# \{0,1\}//'
  exit 0
}
if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
  usage
fi
OUTPUT_PATH="${1:-}"
shift || true
if [[ -z "$OUTPUT_PATH" ]]; then
  log_error "usage: $0 <output.mp4> [OPTIONS]"
  log_error "  see --help"
  exit 2
fi

# Defaults — operator-overridable.
MIN_DURATION_S="${VERIFY_MIN_DURATION_S:-1.0}"
MAX_DURATION_S="${VERIFY_MAX_DURATION_S:-86400.0}"
MIN_WIDTH="${VERIFY_MIN_WIDTH:-480}"
MIN_HEIGHT="${VERIFY_MIN_HEIGHT:-320}"
MIN_FPS="${VERIFY_MIN_FPS:-23.976}"
REQUIRED_VIDEO_CODEC="${VERIFY_REQUIRED_VIDEO_CODEC:-h264}"
REQUIRED_AUDIO_CODEC="${VERIFY_REQUIRED_AUDIO_CODEC:-aac}"
ALLOW_NO_AUDIO="${VERIFY_ALLOW_NO_AUDIO:-0}"
EXPECT_STATUS="${VERIFY_EXPECT_STATUS:-SUCCEEDED}"
JOB_ID=""
MASTER_URL=""
BEARER_ENV="${VERIFY_BEARER_ENV:-VELOX_MASTER_BEARER}"
MASTER_BLOB_PATH=""
REPORT_JSON=""
DESTINATION_ID=""
FFPROBE_TIMEOUT_S="${VERIFY_FFPROBE_TIMEOUT_S:-15}"
CURL_TIMEOUT_S="${VERIFY_CURL_TIMEOUT_S:-10}"

while (( $# > 0 )); do
  case "$1" in
    --job-id)            JOB_ID="$2"; shift 2 ;;
    --master-url)        MASTER_URL="$2"; shift 2 ;;
    --bearer-env)        BEARER_ENV="$2"; shift 2 ;;
    --master-blob-path)  MASTER_BLOB_PATH="$2"; shift 2 ;;
    --min-duration-s)    MIN_DURATION_S="$2"; shift 2 ;;
    --max-duration-s)    MAX_DURATION_S="$2"; shift 2 ;;
    --min-width)         MIN_WIDTH="$2"; shift 2 ;;
    --min-height)        MIN_HEIGHT="$2"; shift 2 ;;
    --min-fps)           MIN_FPS="$2"; shift 2 ;;
    --required-video-codec) REQUIRED_VIDEO_CODEC="$2"; shift 2 ;;
    --required-audio-codec) REQUIRED_AUDIO_CODEC="$2"; shift 2 ;;
    --allow-no-audio)    ALLOW_NO_AUDIO=1; shift ;;
    --expect-status)     EXPECT_STATUS="$2"; shift 2 ;;
    --destination-id)    DESTINATION_ID="$2"; shift 2 ;;
    --report-json)       REPORT_JSON="$2"; shift 2 ;;
    -h|--help)           usage ;;
    *)                   log_error "unknown flag: $1"; exit 2 ;;
  esac
done

# ─── Required binaries ─────────────────────────────────────────────────────
for bin in ffprobe sha256sum awk sed grep; do
  if ! command -v "$bin" >/dev/null 2>&1; then
    log_error "FATAL: required binary not in PATH: $bin"
    exit 2
  fi
done
if [[ -n "$JOB_ID" || -n "$MASTER_URL" ]]; then
  for bin in curl jq; do
    if ! command -v "$bin" >/dev/null 2>&1; then
      log_error "FATAL: master-side check requires $bin in PATH"
      exit 2
    fi
  done
fi

log_info "verify_artifact: file=$OUTPUT_PATH expect_status=$EXPECT_STATUS"
log_info "thresholds: duration=[${MIN_DURATION_S},${MAX_DURATION_S}]s width>=${MIN_WIDTH} height>=${MIN_HEIGHT} fps>=${MIN_FPS}"
log_info "codecs: video=$REQUIRED_VIDEO_CODEC audio=$REQUIRED_AUDIO_CODEC allow_no_audio=$ALLOW_NO_AUDIO"
[[ -n "$JOB_ID"        ]] && log_info "master-side: job_id=$JOB_ID master_url=$MASTER_URL expect_status=$EXPECT_STATUS"
if [[ -n "$DESTINATION_ID" ]]; then
  # Informational only: the openapi GET /api/v1/jobs/{job_id} response
  # contract (SubmitJobStatusResponse) exposes only {job_id, status,
  # created, status_url}; per-destination delivery status is not in the
  # polling envelope. status=SUCCEEDED is therefore the strongest available
  # server-side signal that ALL delivery_plan entries are committed. The
  # destination_id is recorded for traceability/audit but is NOT asserted
  # against the master via the canonical polling endpoint.
  log_warn "--destination-id is informational only; master /api/v1/jobs/<job_id> does not expose per-destination status (status=SUCCEEDED implies all delivery_plan entries committed)"
fi

# ─── Verdict state ─────────────────────────────────────────────────────────
# checks_total / checks_failed / checks_passed — final counts.
# CHECKS_JSON holds a list of { name, status, detail } entries.
CHECKS_JSON=""
append_check() {
  local name="$1" status="$2" detail="${3:-}"
  # Escape detail for JSON (curl-style escape of " and \).
  local esc="${detail//\\/\\\\}"
  esc="${esc//\"/\\\"}"
  local entry
  entry=$(printf '{"name":%s,"status":%s,"detail":%s}' \
    "$(jq -n --arg n "$name" '$n')" \
    "$(jq -n --arg s "$status" '$s')" \
    "$(jq -n --arg d "$esc" '$d')")
  if [[ -z "$CHECKS_JSON" ]]; then
    CHECKS_JSON="$entry"
  else
    CHECKS_JSON="${CHECKS_JSON},${entry}"
  fi
}

record_pass() { append_check "$1" "PASS" "${2:-}"; }
record_fail() { append_check "$1" "FAIL" "${2:-}"; }

# TERMINAL_REASON — when an early-exit path fires (file_readable /
# file_size_zero), the caller sets this global to a non-empty string
# before `emit_report_json` runs. The emitted JSON envelope then
# includes `truncated: true` and `terminal_reason: <string>`, so a
# downstream consumer reading length(checks[]) does not mistake a
# 1-entry early-exit report for a "ran all checks + only 1 fail"
# report. End-of-script emit keeps TERMINAL_REASON unset, so
# truncated=false and terminal_reason=null.
TERMINAL_REASON=""

# emit_report_json — idempotent helper that writes the per-check JSON
# report at any exit path (early-exit file/no-file or natural
# end-of-script). Reads from globals: REPORT_JSON / TMP_REPORT /
# REPORT_DIR / CHECKS_JSON / TERMINAL_REASON. Calling twice is safe:
# the second call sees REPORT_JSON empty (we clear it AFTER a
# successful mv, so a failed mv preserves REPORT_JSON for retry and
# the natural emission at end-of-script fires once correctly).
emit_report_json() {
  [[ -z "$REPORT_JSON" ]] && return 0
  REPORT_DIR=$(dirname -- "$REPORT_JSON")
  ensure_dir "$REPORT_DIR" || { log_error "ensure_dir failed: $REPORT_DIR"; return 1; }
  # Atomic on same filesystem: mktemp -p creates inside the target dir,
  # so the subsequent mv is always EXDEV-safe. .partial suffix keeps the
  # intent human-readable if a previous run is observed mid-write.
  TMP_REPORT=$(mktemp -p "$REPORT_DIR" "${REPORT_JSON##*/}.partial.XXXXXXXX")
  # truncated is true iff an early-exit path fired before the natural
  # end-of-script emit (TERMINAL_REASON is non-empty in that case).
  local truncated="false"
  [[ -n "$TERMINAL_REASON" ]] && truncated="true"
  jq -n \
    --arg file "$OUTPUT_PATH" \
    --argjson bytes "$FILE_BYTES" \
    --arg sha "$LOCAL_SHA" \
    --arg duration "$DURATION_S" \
    --arg width "${PRIMARY_W:-0}" \
    --arg height "${PRIMARY_H:-0}" \
    --arg vcodec "${PRIMARY_VCODEC:-}" \
    --arg fps "$PRIMARY_FPS" \
    --argjson n_video "$V_COUNT" \
    --argjson n_audio "$A_COUNT" \
    --arg expect_status "$EXPECT_STATUS" \
    --arg job_id "$JOB_ID" \
    --arg destination_id "$DESTINATION_ID" \
    --arg truncated "$truncated" \
    --arg terminal_reason "${TERMINAL_REASON:-}" \
    --argjson checks "$(printf '[%s]' "$CHECKS_JSON")" \
    '{
       file: $file,
       bytes: $bytes,
       sha256: $sha,
       duration_seconds: ($duration|tonumber? // $duration),
       width: ($width|tonumber? // $width),
       height: ($height|tonumber? // $height),
       video_codec: $vcodec,
       fps: ($fps|tonumber? // $fps),
       n_video_streams: $n_video,
       n_audio_streams: $n_audio,
       job_id: (if $job_id == "" then null else $job_id end),
       expect_status: $expect_status,
       destination_id: (if $destination_id == "" then null else $destination_id end),
       truncated: ($truncated == "true"),
       terminal_reason: (if $terminal_reason == "" then null else $terminal_reason end),
       checks: $checks
     }' > "$TMP_REPORT" || {
    log_error "FATAL: failed to render report JSON"
    rm -f "$TMP_REPORT"
    return 1
  }
  # Reset REPORT_JSON only after successful mv so a failed EXDEV / EPERM
  # leaves the early-exit's caller able to retry via natural emission.
  mv -f "$TMP_REPORT" "$REPORT_JSON" || {
    log_error "FAILED: mv $TMP_REPORT -> $REPORT_JSON (filesystem state preserved)"
    rm -f "$TMP_REPORT"
    return 1
  }
  log_info "report: $REPORT_JSON (truncated=$truncated terminal_reason=${TERMINAL_REASON:-<none>})"
  REPORT_JSON=""  # idempotency guard: naturally only fires after success
  return 0
}

OVERALL_FATAL_RC=0  # Final rc; 0 = PASS.

# ─── Check 1: file readable + size > 0 ─────────────────────────────────────
# Exit-code priority: missing / empty file short-circuits to exit 3 BEFORE
# any subsequent check runs (file would just generate rc=4 from the
# downstream ffprobe failure, which is misleading). All subsequent checks
# assume a readable, non-empty input file. Trailing checks therefore only
# set OVERALL_FATAL_RC=4+|5+|6+|7+ — exit 3 is reserved for "no artifact to
# verify" and is the only "early-exit" branch in this script.
FILE_BYTES="0"
if ! check_file_readable "$OUTPUT_PATH"; then
  log_error "FAIL: file_readable: $OUTPUT_PATH"
  record_fail "file_readable" "path=$OUTPUT_PATH not readable"
  TERMINAL_REASON="file_readable"
  emit_report_json
  exit 3
fi
# stat fallback is intentionally `|| echo 0` so non-regular paths (FIFO,
# block device, datagram socket) cannot hang the `wc -c <\"$PATH\"` redirect.
# The downstream `(( FILE_BYTES <= 0 ))` triggers rc=3 with a clean log_error
# line instead of a 15s ffprobe timeout on a weird filesystem object.
FILE_BYTES=$(stat -c %s "$OUTPUT_PATH" 2>/dev/null || echo 0)
if (( FILE_BYTES <= 0 )); then
  log_error "FAIL: file_size: ${FILE_BYTES} bytes"
  record_fail "file_size_zero" "got ${FILE_BYTES} bytes"
  TERMINAL_REASON="file_size_zero"
  emit_report_json
  exit 3
fi
log_info "OK: file_size: $FILE_BYTES bytes"
record_pass "file_size" "${FILE_BYTES} bytes"

# ─── Check 2: ffprobe parse ────────────────────────────────────────────────
# -v error suppresses most chatter; -show_format emits [FORMAT] block at end,
# -show_streams emits one [STREAM] block per stream. Each block contains
# key=value lines that we extract with awk.
TMP_FFPROBE=$(mktemp)
TMP_FFPROBE_ERR=$(mktemp)
FFPROBE_RC=0
# NOTE: stderr goes to a separate file (NOT merged into stdout) so parse
# failures surface distinct from data errors. NEITHER is dumped to logs
# unless they contain actionable diagnostics (header dump on HTTP error
# is the bearer-leak risk surface and is disabled by default — see HTTP
# check below for the bearer-leak policy).
timeout "${FFPROBE_TIMEOUT_S}" ffprobe -v error -show_format -show_streams \
  "$OUTPUT_PATH" >"$TMP_FFPROBE" 2>"$TMP_FFPROBE_ERR" || FFPROBE_RC=$?
FFPROBE_OUT=$(cat "$TMP_FFPROBE")
rm -f "$TMP_FFPROBE_ERR"
if (( FFPROBE_RC != 0 )); then
  log_error "FAIL: ffprobe_exec: rc=$FFPROBE_RC"
  record_fail "ffprobe_exec" "ffprobe exit $FFPROBE_RC"
  OVERALL_FATAL_RC=4
else
  log_info "OK: ffprobe_exec"
  record_pass "ffprobe_exec" "ffprobe exit 0"
fi

# Parse [FORMAT] block (single stream-level wrapper for the container).
# ffprobe -show_format emits key=value lines terminated by a section like
# [STREAM] or [/FORMAT]. We want lines from [FORMAT] to first [STREAM|[/FORMAT]].

parse_format_kv() {
  local key="$1"
  awk -v k="$key" '
    BEGIN { inf=0 }
    /^\[FORMAT\]/        { inf=1; next }
    /^\[STREAM\]/        { inf=0 }
    /^\[\/FORMAT\]/      { inf=0 }
    inf && index($0, k "=") == 1 {
      sub("^" k "=", "", $0)
      print
      exit
    }
  ' "$TMP_FFPROBE"
}

DURATION_S=""
DURATION_S=$(parse_format_kv "duration")
[[ -z "$DURATION_S" ]] && DURATION_S="0"
log_info "ffprobe: format.duration=${DURATION_S}s"

STREAMS_TMP="$TMP_STREAMS"  # alias for legacy variable name in check section
STREAMS_TMP="$TMP_STREAMS"  # alias for legacy variable name in check section
# Walk each [STREAM] block independently; emit group lines so caller can
# pick by index=0,1,... Skip non-stream sections (SIDE_DATA, etc.).
TMP_STREAMS=$(mktemp)
awk '
  /^\[STREAM\]/      { idx++; print "==STREAM "idx" =="; next }
  /^\[FORMAT\]|^\[SIDE_DATA|^\[PROGRAM|^\[\/STREAM\]|^\[\/FORMAT\]|^\[\/SIDE_DATA|^\[\/PROGRAM/ { next }
  { print }
' "$TMP_FFPROBE" > "$TMP_STREAMS"

# Stream-type detection: codec_type=video / codec_type=audio.
# We aggregate: count of video streams, audio streams, primary video
# width/height/codec_name + avg_frame_rate, primary audio codec_name.
declare -a V_W=() V_H=() V_CODEC=() V_FPS=()
declare -a A_CODEC=()
STREAM_IDX=0
while (( STREAM_IDX < 32 )); do
  STREAM_IDX=$((STREAM_IDX + 1))
  block=$(awk -v n="$STREAM_IDX" 'BEGIN{p=0} /^==STREAM /{p=($3==n)} p{print}' "$STREAMS_TMP")
  [[ -z "$block" ]] && break
  ctype=$(printf '%s' "$block" | awk -F= '/^codec_type=/{print $2; exit}')
  case "$ctype" in
    video)
      w=$(printf '%s' "$block" | awk -F= '/^width=/{print $2; exit}')
      h=$(printf '%s' "$block" | awk -F= '/^height=/{print $2; exit}')
      vcodec=$(printf '%s' "$block" | awk -F= '/^codec_name=/{print $2; exit}')
      vfps=$(printf '%s' "$block" | awk -F= '/^avg_frame_rate=/{print $2; exit}')
      V_W+=("$w"); V_H+=("$h"); V_CODEC+=("$vcodec"); V_FPS+=("$vfps")
      ;;
    audio)
      acodec=$(printf '%s' "$block" | awk -F= '/^codec_name=/{print $2; exit}')
      A_CODEC+=("$acodec")
      ;;
  esac
done
# Trim 32-stream cap safeguard: drop empty trailing tuples inserted if
# some heuristics left bare entries.
# (No-op here — arrays auto-tighten because we only push typed entries.)

V_COUNT=${#V_W[@]}
A_COUNT=${#A_CODEC[@]}
log_info "ffprobe: ${V_COUNT} video stream(s), ${A_COUNT} audio stream(s)"

# Pick the primary video (first one).
PRIMARY_W="${V_W[0]:-}"
PRIMARY_H="${V_H[0]:-}"
PRIMARY_VCODEC="${V_CODEC[0]:-}"
PRIMARY_VFPS="${V_FPS[0]:-}"

# avg_frame_rate is "num/den"; convert to float fps (e.g. "24000/1001" → 23.976).
fps_ratio_to_float() {
  local r="$1"
  if [[ -z "$r" || "$r" == "0/0" ]]; then echo "0.0"; return; fi
  local n d
  n=${r%%/*}; d=${r##*/}
  [[ -z "$d" || "$d" == "0" ]] && { echo "0.0"; return; }
  awk -v n="$n" -v d="$d" 'BEGIN { printf "%.6f", n/d }'
}
PRIMARY_FPS=$(fps_ratio_to_float "$PRIMARY_VFPS")
log_info "ffprobe: primary video codec=$PRIMARY_VCODEC size=${PRIMARY_W}x${PRIMARY_H} fps=$PRIMARY_FPS (raw=$PRIMARY_VFPS)"

# ─── Check 3: video codec ──────────────────────────────────────────────────
if (( V_COUNT >= 1 )) && [[ "$PRIMARY_VCODEC" == "$REQUIRED_VIDEO_CODEC" ]]; then
  log_info "OK: video_codec: codec_name=$PRIMARY_VCODEC (required=$REQUIRED_VIDEO_CODEC, n_streams=$V_COUNT)"
  record_pass "video_codec" "$PRIMARY_VCODEC (n=$V_COUNT)"
else
  log_error "FAIL: video_codec: codec_name=${PRIMARY_VCODEC:-<none>} required=$REQUIRED_VIDEO_CODEC n=${V_COUNT}"
  record_fail "video_codec" "got=${PRIMARY_VCODEC:-<none>} required=$REQUIRED_VIDEO_CODEC n=${V_COUNT}"
  OVERALL_FATAL_RC=4
fi

# ─── Check 4: audio codec ──────────────────────────────────────────────────
if (( ALLOW_NO_AUDIO == 1 )); then
  log_info "OK: audio_codec: skipped (--allow-no-audio)"
  record_pass "audio_codec" "skipped (allow_no_audio=1, n=$A_COUNT codec=${A_CODEC[0]:-<none>})"
elif (( A_COUNT >= 1 )) && [[ "${A_CODEC[0]:-}" == "$REQUIRED_AUDIO_CODEC" ]]; then
  log_info "OK: audio_codec: codec_name=${A_CODEC[0]} (required=$REQUIRED_AUDIO_CODEC, n_streams=$A_COUNT)"
  record_pass "audio_codec" "${A_CODEC[0]} (n=$A_COUNT)"
else
  log_error "FAIL: audio_codec: codec=${A_CODEC[0]:-<none>} required=$REQUIRED_AUDIO_CODEC n=${A_COUNT}"
  record_fail "audio_codec" "got=${A_CODEC[0]:-<none>} required=$REQUIRED_AUDIO_CODEC n=${A_COUNT}"
  OVERALL_FATAL_RC=4
fi

# ─── Check 5: threshold (duration) ─────────────────────────────────────────
is_within_threshold() {
  local v="$1" lo="$2" hi="$3"
  awk -v v="$v" -v lo="$lo" -v hi="$hi" 'BEGIN { exit !(v+0 >= lo+0 && v+0 <= hi+0) }'
}
if is_within_threshold "$DURATION_S" "$MIN_DURATION_S" "$MAX_DURATION_S"; then
  log_info "OK: duration: ${DURATION_S}s in [${MIN_DURATION_S},${MAX_DURATION_S}]"
  record_pass "duration" "${DURATION_S}s in [${MIN_DURATION_S},${MAX_DURATION_S}]"
else
  log_error "FAIL: duration: ${DURATION_S}s outside [${MIN_DURATION_S},${MAX_DURATION_S}]"
  record_fail "duration" "${DURATION_S}s outside [${MIN_DURATION_S},${MAX_DURATION_S}]"
  OVERALL_FATAL_RC=4
fi

# ─── Check 6: threshold (width) ────────────────────────────────────────────
if (( PRIMARY_W + 0 >= MIN_WIDTH + 0 )); then
  log_info "OK: width: ${PRIMARY_W} >= ${MIN_WIDTH}"
  record_pass "width" "${PRIMARY_W} >= ${MIN_WIDTH}"
else
  log_error "FAIL: width: ${PRIMARY_W:-0} < ${MIN_WIDTH}"
  record_fail "width" "${PRIMARY_W:-0} < ${MIN_WIDTH}"
  OVERALL_FATAL_RC=4
fi

# ─── Check 7: threshold (height) ───────────────────────────────────────────
if (( PRIMARY_H + 0 >= MIN_HEIGHT + 0 )); then
  log_info "OK: height: ${PRIMARY_H} >= ${MIN_HEIGHT}"
  record_pass "height" "${PRIMARY_H} >= ${MIN_HEIGHT}"
else
  log_error "FAIL: height: ${PRIMARY_H:-0} < ${MIN_HEIGHT}"
  record_fail "height" "${PRIMARY_H:-0} < ${MIN_HEIGHT}"
  OVERALL_FATAL_RC=4
fi

# ─── Check 8: threshold (fps) ──────────────────────────────────────────────
if awk -v v="$PRIMARY_FPS" -v lo="$MIN_FPS" 'BEGIN { exit !(v+0 >= lo+0) }'; then
  log_info "OK: fps: ${PRIMARY_FPS} >= ${MIN_FPS} (raw=$PRIMARY_VFPS)"
  record_pass "fps" "${PRIMARY_FPS} >= ${MIN_FPS}"
else
  log_error "FAIL: fps: ${PRIMARY_FPS:-0.0} < ${MIN_FPS} (raw=$PRIMARY_VFPS)"
  record_fail "fps" "${PRIMARY_FPS:-0.0} < ${MIN_FPS}"
  OVERALL_FATAL_RC=4
fi

# ─── Check 9: SHA-256 ──────────────────────────────────────────────────────
LOCAL_SHA=""
LOCAL_SHA=$(sha256sum "$OUTPUT_PATH" | awk '{print $1}')
log_info "sha256: $LOCAL_SHA"
record_pass "sha256_local" "$LOCAL_SHA (${FILE_BYTES} bytes)"

# ─── Check 10-12: master-side (only with --job-id + --master-url + bearer) ─
if [[ -n "$JOB_ID" && -n "$MASTER_URL" ]]; then
  # Bearer resolution: REJECT dynamic env names that don't match the strict
  # POSIX-shell-var convention. This avoids bash-version-dependent
  # `${!NAME}` indirection under `set -u`, and keeps the door closed on
  # typos (a smiley-formatted name like "BEARER-TOK" would silently
  # expand to empty under bash 4.x without the regex guard).
  BEARER=""
  if ! [[ "$BEARER_ENV" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]]; then
    log_error "FAIL: master_side_auth: --bearer-env '$BEARER_ENV' is not a valid env-var name"
    record_fail "master_side_auth" "--bearer-env '$BEARER_ENV' rejected (must match ^[A-Za-z_][A-Za-z0-9_]*$)"
    OVERALL_FATAL_RC=6
  elif [[ -z "${!BEARER_ENV:-}" ]]; then
    log_error "FAIL: master_side_auth: env $BEARER_ENV is unset"
    record_fail "master_side_auth" "env $BEARER_ENV unset"
    OVERALL_FATAL_RC=6
  else
    BEARER="${!BEARER_ENV}"
    # Trim URL trailing slashes (single or multiple). Joining with explicit
    # /api/v1/jobs/<id> yields a clean URL regardless of input style.
    MASTER_URL_TRIMMED=$(printf '%s' "$MASTER_URL" | sed 's|/*$||')
    JOB_URL="${MASTER_URL_TRIMMED}/api/v1/jobs/${JOB_ID}"
    log_info "master-side: GET $JOB_URL (expect $EXPECT_STATUS)"

    # Use -w instead of -D to keep the bearer out of any persisted header
    # file. curl -w prints status code without exposing the Authorization
    # header anywhere on the success or failure path.
    TMP_JOB_BODY=$(mktemp)
    TMP_JOB_STATUS=$(mktemp)
    GET_STATUS=""
    curl -sS -m "${CURL_TIMEOUT_S}" -X GET \
      -H "Authorization: Bearer $BEARER" \
      -o "$TMP_JOB_BODY" \
      -w '%{http_code}' "$JOB_URL" >"$TMP_JOB_STATUS" 2>/dev/null || GET_STATUS=""
    if [[ -z "$GET_STATUS" ]]; then
      # Couldn't even get a status code (DNS/connect error/timeout).
      log_error "FAIL: master_side_http: curl could not reach $JOB_URL"
      record_fail "master_side_http" "curl unreachable (DNS/connect/timeout)"
      OVERALL_FATAL_RC=6
    elif [[ "$GET_STATUS" == "200" ]]; then
      GET_BODY=$(cat "$TMP_JOB_BODY")
      # Use jq -r (string-render) NOT jq -e (truthiness); we want a clean
      # string compare, not a boolean/error path that would race with the
      # exit-code 5/6 mapping.
      JOB_STATUS=$(printf '%s' "$GET_BODY" | jq -r '.status // ""' 2>/dev/null || echo "")
      if [[ "$JOB_STATUS" == "$EXPECT_STATUS" ]]; then
        log_info "OK: destination_completed: job.status=$JOB_STATUS matches expected=$EXPECT_STATUS (artifact+deliveries committed)"
        record_pass "destination_completed" \
          "job.status=$JOB_STATUS (artifact READY + delivery_plan entries committed) dest=${DESTINATION_ID:-<all>}"
      else
        log_error "FAIL: destination_completed: job.status=${JOB_STATUS:-<unset>} expected=$EXPECT_STATUS"
        record_fail "destination_completed" \
          "job.status=${JOB_STATUS:-<unset>} expected=$EXPECT_STATUS"
        OVERALL_FATAL_RC=5
      fi
      record_pass "master_side_http" "HTTP 200 job_id=$JOB_ID"
    elif [[ "$GET_STATUS" =~ ^4 ]]; then
      # 4xx — semantic mismatch (404 job_not_found, 401 invalid_bearer,
      # 422 invalid_payload). All collapse to "the job did not reach
      # the expected status" — exit 5 (NOT exit 6 which is reserved for
      # transport problems with no HTTP roundtrip).
      log_error "FAIL: master_side_http: HTTP $GET_STATUS (semantic 4xx)"
      record_fail "master_side_http" "HTTP $GET_STATUS (semantic 4xx)"
      OVERALL_FATAL_RC=5
    else
      # 5xx or anything else — transport / server fault.
      log_error "FAIL: master_side_http: HTTP $GET_STATUS (server/transport fault)"
      record_fail "master_side_http" "HTTP $GET_STATUS (server/transport fault)"
      OVERALL_FATAL_RC=6
    fi
  fi

  # ─── Check 11: master-replica blob SHA (only --master-blob-path) ─────────
  # Stage 3 is FAIL-SOFT: a missing/unreadable master blob is recorded as
  # skipped in checks[] WITHOUT promoting to overall FAIL. exit 7 is
  # reserved for "blob readable AND local/master sha differ".
  if [[ -n "$MASTER_BLOB_PATH" ]]; then
    if ! [[ -r "$MASTER_BLOB_PATH" ]]; then
      log_warn "master_blob_sha256: skipped (path not readable: $MASTER_BLOB_PATH)"
      record_fail "master_blob_sha256" "skipped: path=$MASTER_BLOB_PATH not readable"
    else
      REMOTE_SHA=$(sha256sum "$MASTER_BLOB_PATH" | awk '{print $1}')
      if [[ "$REMOTE_SHA" == "$LOCAL_SHA" ]]; then
        log_info "OK: master_blob_sha256: $REMOTE_SHA == $LOCAL_SHA"
        record_pass "master_blob_sha256" "$REMOTE_SHA (path=$MASTER_BLOB_PATH)"
      else
        log_error "FAIL: master_blob_sha256: local=$LOCAL_SHA remote=$REMOTE_SHA"
        record_fail "master_blob_sha256" "local=$LOCAL_SHA remote=$REMOTE_SHA"
        OVERALL_FATAL_RC=7
      fi
    fi
  fi
fi

# ─── Cleanup tmp files ──────────────────────────────────────────────────────
# Now handled by EXIT trap on_exit_cleanup; explicit rm kept as a no-op
# safety belt for in-flight failures before the trap re-runs.
rm -f "$TMP_FFPROBE" "$TMP_STREAMS" 2>/dev/null || true

# ─── Emit report JSON atomically (if requested) ────────────────────────────
# Idempotent helper: if early-exit paths already emitted (set REPORT_JSON=""),
# this call is a no-op so we never produce a truncated follow-up write.
emit_report_json

# ─── Final summary ─────────────────────────────────────────────────────────
PASS_COUNT=$(printf '[%s]' "$CHECKS_JSON" | jq '[.[] | select(.status=="PASS")] | length' 2>/dev/null || echo 0)
FAIL_COUNT=$(printf '[%s]' "$CHECKS_JSON" | jq '[.[] | select(.status=="FAIL")] | length' 2>/dev/null || echo 0)
log_info "summary: PASS=$PASS_COUNT FAIL=$FAIL_COUNT rc=$OVERALL_FATAL_RC"

if (( OVERALL_FATAL_RC == 0 )); then
  log_info "OK: verify_artifact PASS"
  exit 0
fi
exit "$OVERALL_FATAL_RC"
