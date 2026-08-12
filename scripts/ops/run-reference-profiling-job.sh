#!/usr/bin/env bash
#
# run-reference-profiling-job.sh — run the Phase-0 reference copy-only job
# (~4m44s / ~284s, ~30 clips, single non-loop audio track) THROUGH the
# profiling wrappers inside the diagnostic worker container, collecting
# strace -f -c / perf stat / perf record evidence.
#
# No Velox logic is touched: the production engine binary and the production
# worker container are used exactly as-is — the same binary the worker agent
# spawns (via VELOX_VIDEO_ENGINE_CPP_BIN), the same ffmpeg/ffprobe children,
# the same read-only rootfs + cap_drop ALL + seccomp:unconfined posture that
# install-worker-perf-wrappers.sh established.
#
# The workload replicates the reference production job measured at
# 15–25s wall / ~4m44s content: copy_only RenderPlan v1, ~30 segments each
# ~9.47s (284s total), one audio track (non-loop → the AAC re-encode path),
# warm cache (a single local fixture reused → the cache→tmp copy path).
#
# Usage:
#   ./scripts/ops/run-reference-profiling-job.sh [--mode all|strace|perfstat|perf] [ssh-host]
#
#   --mode all       run all three wrappers (default)
#   --mode X         run a single wrapper mode
#
# Env overrides:
#   REF_SEGMENTS        segments in the timeline (default: 30)
#   REF_TOTAL_SECONDS   total copy-only duration  (default: 284)
#
# Outputs on the worker host:
#   /var/lib/velox/perf/refjob/                  fixtures + plan + logs + output
#   /var/lib/velox/perf/strace-<stamp>.txt       strace -f -c summary
#   /var/lib/velox/perf/velox-perfstat-<stamp>.txt  perf stat -d -d -d summary
#   /var/lib/velox/perf/velox-<stamp>.data       perf record callgraph (binary)
#   /var/lib/velox/perf/refjob/summary.tsv       wall_ms + rc + trace per mode
#
# Exit codes:
#   0   all requested modes completed
#   1   fixture generation, plan write, or a run failed
#   2   usage error / host unreachable / prerequisites missing
set -Eeuo pipefail

HOST="velox-deb-57.131"
MODE="all"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode)
      [[ $# -ge 2 ]] || { echo "usage: --mode all|strace|perfstat|perf" >&2; exit 2; }
      MODE="$2"; shift 2 ;;
    -h|--help)
      sed -n '2,42p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
      exit 0 ;;
    *)
      HOST="$1"; shift ;;
  esac
done
case "$MODE" in all|strace|perfstat|perf) ;; *) echo "invalid --mode: $MODE" >&2; exit 2 ;; esac

fail() { printf '[run-reference-profiling-job][ERROR] %s\n' "$*" >&2; exit 2; }
ok()   { printf '[run-reference-profiling-job][OK]   %s\n' "$*"; }
log()  { printf '[run-reference-profiling-job][..]   %s\n' "$*"; }

command -v ssh >/dev/null 2>&1 || fail "ssh not found on PATH"
if ! ssh -o BatchMode=yes -o ConnectTimeout=5 "$HOST" true 2>/dev/null; then
  fail "host $HOST unreachable via ssh — check the alias in ~/.ssh/config"
fi
log "host $HOST reachable, mode=$MODE"

# MODE crosses the wire as a positional argument (SSH does not forward env).
if ! ssh -o BatchMode=yes "$HOST" "sudo bash -s '$MODE' '${REF_SEGMENTS:-30}' '${REF_TOTAL_SECONDS:-284}'" <<'REMOTE'
set -Eeuo pipefail

fail() { printf '[worker][ERROR] %s\n' "$*" >&2; exit 1; }
ok()   { printf '[worker][OK]   %s\n' "$*"; }
log()  { printf '[worker][..]   %s\n' "$*"; }

MODE="${1:?}"
SEGMENTS="${2:-30}"
TOTAL_SECONDS="${3:-284}"
PERF_DIR="/var/lib/velox/perf"
REF="${PERF_DIR}/refjob"
C="velox-worker"

[[ "$(id -u)" -eq 0 ]] || fail "remote body must run as root (sudo bash -s)"
[[ "$MODE" =~ ^(all|strace|perfstat|perf)$ ]] || fail "invalid mode: $MODE"
[[ "$SEGMENTS" =~ ^[1-9][0-9]*$ ]] || fail "REF_SEGMENTS must be a positive integer"
[[ "$TOTAL_SECONDS" =~ ^[1-9][0-9]*$ ]] || fail "REF_TOTAL_SECONDS must be a positive integer"

# ── fixtures + plan ─────────────────────────────────────────────────────────
mkdir -p "$REF/fixtures" "$REF/out"
chmod -R 777 "$REF"

VIDEO_FIXTURE="${REF}/fixtures/video.mp4"
AUDIO_FIXTURE="${REF}/fixtures/audio.m4a"
PLAN="${REF}/plan.json"
OUTPUT="${REF}/out/output.mp4"

if [[ ! -s "$VIDEO_FIXTURE" ]]; then
  log "generating video fixture (10s, 640x360@30, h264) in-container..."
  docker exec "$C" ffmpeg -y -hide_banner -loglevel error \
    -f lavfi -i "color=c=0x123456:s=640x360:r=30" \
    -t 10 -an -c:v libx264 -preset ultrafast -pix_fmt yuv420p -r 30 \
    -movflags +faststart "$VIDEO_FIXTURE" || fail "video fixture generation failed"
fi
if [[ ! -s "$AUDIO_FIXTURE" ]]; then
  log "generating audio fixture (285s AAC 48k stereo) in-container..."
  docker exec "$C" ffmpeg -y -hide_banner -loglevel error \
    -f lavfi -i "sine=frequency=440:sample_rate=48000" \
    -t 285 -c:a aac -ar 48000 -ac 2 "$AUDIO_FIXTURE" || fail "audio fixture generation failed"
fi

DUR="$(awk -v t="$TOTAL_SECONDS" -v n="$SEGMENTS" 'BEGIN { printf "%.9f", t / n }')"
SEG_JSON=""
for i in $(seq 0 $((SEGMENTS - 1))); do
  [[ "$i" -ne 0 ]] && SEG_JSON="${SEG_JSON},"
  SEG_JSON="${SEG_JSON}{ \"source\": { \"type\": \"video\", \"url\": \"${VIDEO_FIXTURE}\" }, \"duration_seconds\": ${DUR}, \"include_audio\": false, \"transform\": { \"scale_mode\": \"stretch\", \"slow_zoom\": false } }"
done
cat >"$PLAN" <<JSON
{
  "version": 1,
  "job_id": "phase0-reference-4m44s",
  "copy_only": true,
  "canvas": { "width": 640, "height": 360, "fps": 30 },
  "timeline": [ ${SEG_JSON} ],
  "audio_tracks": [
    { "source_url": "${AUDIO_FIXTURE}", "volume": 1.0, "start_time_offset": 0.0, "loop": false }
  ],
  "output_path": "${OUTPUT}"
}
JSON
log "plan written: $PLAN ($SEGMENTS segments x ${DUR}s = ~${TOTAL_SECONDS}s)"

# ── runs ─────────────────────────────────────────────────────────────────────
run_mode() {
  local m="$1" wrapper stamp trace start_ns end_ns wall_ms rc
  case "$m" in
    strace)   wrapper="velox_video_engine_strace" ;;
    perfstat) wrapper="velox_video_engine_perfstat" ;;
    perf)     wrapper="velox_video_engine_perf" ;;
    *) fail "unknown mode $m" ;;
  esac
  log "run mode=$m wrapper=$wrapper start=$(date -Is)"
  start_ns="$(date +%s%N)"
  docker exec -w "$REF/out" --user velox \
    -e VELOX_FFMPEG_DECODE_TELEMETRY=1 \
    -e VELOX_BENCH_DISK_COPY_METRICS=1 \
    "$C" "/usr/local/bin/${wrapper}" --render --plan "$PLAN" \
    >"$REF/run-${m}.stdout.log" 2>"$REF/run-${m}.stderr.log"
  rc=$?
  end_ns="$(date +%s%N)"
  wall_ms=$(( (end_ns - start_ns) / 1000000 ))

  case "$m" in
    strace)   trace="$(ls -t "${PERF_DIR}"/strace-*.txt 2>/dev/null | head -1)" ;;
    perfstat) trace="$(ls -t "${PERF_DIR}"/velox-perfstat-*.txt 2>/dev/null | head -1)" ;;
    perf)     trace="$(ls -t "${PERF_DIR}"/velox-*.data 2>/dev/null | head -1)" ;;
  esac
  trace="${trace:-<none>}"

  out_ok=0; [[ -s "$OUTPUT" ]] && out_ok=1
  printf '%s\t%s\t%s\t%s\t%s\n' "$m" "$wall_ms" "$rc" "$trace" "$out_ok" >>"$REF/summary.tsv"
  ok "mode=$m wall_ms=$wall_ms rc=$rc output_ok=$out_ok trace=$trace"
  [[ "$rc" -eq 0 ]] || { log "mode=$m engine rc=$rc — tail of stderr:"; tail -20 "$REF/run-${m}.stderr.log" || true; return 1; }
}

printf 'mode\twall_ms\trc\ttrace\toutput_ok\n' >"$REF/summary.tsv"
rm -f "$OUTPUT"
failures=0
case "$MODE" in
  all)     for m in strace perfstat perf; do run_mode "$m" || failures=$((failures + 1)); done ;;
  strace|perfstat|perf) run_mode "$MODE" || failures=$((failures + 1)) ;;
esac

echo "===== summary.tsv ====="
cat "$REF/summary.tsv"
echo "===== traces on host ====="
ls -la "$PERF_DIR" | grep -E 'strace-|velox-|velox-perfstat' || true
if [[ "$failures" -ne 0 ]]; then
  fail "$failures mode(s) failed — see $REF/run-*.stderr.log"
fi
ok "done (mode=$MODE); evidence under $PERF_DIR"
REMOTE
then
  fail "reference profiling run failed on $HOST"
fi

ok "reference profiling completed on $HOST (mode=$MODE)"
printf '[run-reference-profiling-job][..]   collect evidence from /var/lib/velox/perf on the host\n'
