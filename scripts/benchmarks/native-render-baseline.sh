#!/usr/bin/env bash
#
# native-render-baseline.sh — measure the current RenderPlan V1 native path.
#
# The benchmark is deliberately local and synthetic. It does not contact the
# Master, download remote assets, or change the working tree. It builds the
# existing C++ engine in /tmp by default, renders a copy-only plan, traces
# process creation, counts ffprobe/ffmpeg children, and records the disk-copy
# and sidecar counters needed before a LibAV migration.
#
# Usage:
#   scripts/benchmarks/native-render-baseline.sh
#   VELOX_BENCH_RUNS=5 VELOX_BENCH_SEGMENTS=30 \
#     VELOX_BENCH_OUTPUT_DIR=/tmp/velox-baseline \
#     scripts/benchmarks/native-render-baseline.sh
#
# Environment:
#   VELOX_BENCH_RUNS          repetitions (default: 3)
#   VELOX_BENCH_SEGMENTS      copy-only video segments per render (default: 4)
#   VELOX_BENCH_BUILD         set to 0 to use an existing engine binary
#   VELOX_BENCH_BUILD_DIR     CMake build directory (default: /tmp/velox-native-baseline-build)
#   VELOX_BENCH_ENGINE        explicit velox_video_engine path
#   VELOX_BENCH_OUTPUT_DIR    evidence directory (default: temporary directory)
#   VELOX_BENCH_KEEP          set to 1 to keep an auto-created evidence directory
#
# Output:
#   baseline.tsv               one row per render
#   summary.json               aggregate means/medians and workload metadata
#   run-N/                     plan, logs, strace, sidecar and final ffprobe
#
# Exit codes:
#   0 benchmark completed and all renders passed
#   2 missing dependency or invalid configuration
#   1 one or more renders failed

set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
ENGINE_SOURCE="${REPO_ROOT}/RemoteCodex/native/video-engine-cpp"
RUNS="${VELOX_BENCH_RUNS:-3}"
SEGMENTS="${VELOX_BENCH_SEGMENTS:-4}"
BUILD_DIR="${VELOX_BENCH_BUILD_DIR:-${TMPDIR:-/tmp}/velox-native-baseline-build}"
ENGINE_BIN="${VELOX_BENCH_ENGINE:-${BUILD_DIR}/velox_video_engine}"
OUTPUT_DIR="${VELOX_BENCH_OUTPUT_DIR:-}"
BUILD="${VELOX_BENCH_BUILD:-1}"

fail_usage() {
  printf '[native-render-baseline][ERROR] %s\n' "$*" >&2
  exit 2
}

fail_run() {
  printf '[native-render-baseline][FAIL] %s\n' "$*" >&2
  exit 1
}

for command_name in cmake ffmpeg ffprobe python3 stat awk grep sed; do
  command -v "$command_name" >/dev/null 2>&1 || fail_usage "missing dependency: ${command_name}"
done
command -v strace >/dev/null 2>&1 || fail_usage "strace is required to count child processes"
[[ "$(uname -s)" == Linux ]] || fail_usage "this benchmark requires Linux/GNU tooling"

[[ "$RUNS" =~ ^[1-9][0-9]*$ ]] || fail_usage "VELOX_BENCH_RUNS must be a positive integer"
[[ "$SEGMENTS" =~ ^[1-9][0-9]*$ ]] || fail_usage "VELOX_BENCH_SEGMENTS must be a positive integer"
[[ "$BUILD" == 0 || "$BUILD" == 1 ]] || fail_usage "VELOX_BENCH_BUILD must be 0 or 1"

AUTO_OUTPUT=0
if [[ -z "$OUTPUT_DIR" ]]; then
  OUTPUT_DIR="$(mktemp -d "${TMPDIR:-/tmp}/velox-native-baseline.XXXXXX")"
  AUTO_OUTPUT=1
else
  mkdir -p "$OUTPUT_DIR"
fi

cleanup() {
  if [[ "$AUTO_OUTPUT" == 1 && "${VELOX_BENCH_KEEP:-0}" != 1 ]]; then
    rm -rf -- "$OUTPUT_DIR"
  fi
}
trap cleanup EXIT

if [[ "$BUILD" == 1 ]]; then
  mkdir -p "$BUILD_DIR"
  cmake -S "$ENGINE_SOURCE" -B "$BUILD_DIR" -DCMAKE_BUILD_TYPE=Release \
    >"$OUTPUT_DIR/cmake-configure.log" 2>&1 || {
      cat "$OUTPUT_DIR/cmake-configure.log" >&2
      fail_usage "CMake configure failed"
    }
  cmake --build "$BUILD_DIR" --parallel \
    >"$OUTPUT_DIR/cmake-build.log" 2>&1 || {
      tail -200 "$OUTPUT_DIR/cmake-build.log" >&2
      fail_usage "CMake build failed"
    }
fi

[[ -x "$ENGINE_BIN" ]] || fail_usage "engine binary is not executable: $ENGINE_BIN"

FIXTURE_DIR="$OUTPUT_DIR/fixture"
mkdir -p "$FIXTURE_DIR"
SOURCE_VIDEO="$FIXTURE_DIR/copy-only-source.mp4"
SOURCE_AUDIO="$FIXTURE_DIR/final-audio.m4a"

# The generated clip satisfies the current conservative copy-only contract:
# H.264, yuv420p, 640x360, 30 fps, and longer than every requested segment.
ffmpeg -y -hide_banner -loglevel error \
  -f lavfi -i "color=c=0x123456:s=640x360:r=30" \
  -t 2.0 -an -c:v libx264 -preset ultrafast -pix_fmt yuv420p -r 30 \
  -movflags +faststart "$SOURCE_VIDEO" || fail_usage "could not generate video fixture"
ffmpeg -y -hide_banner -loglevel error \
  -f lavfi -i "sine=frequency=440:sample_rate=48000" \
  -t "$((SEGMENTS))" -c:a aac -ar 48000 -ac 2 "$SOURCE_AUDIO" || \
  fail_usage "could not generate audio fixture"

SOURCE_VIDEO_BYTES="$(stat -c '%s' "$SOURCE_VIDEO")"
SOURCE_AUDIO_BYTES="$(stat -c '%s' "$SOURCE_AUDIO")"

PLAN_TEMPLATE="$FIXTURE_DIR/plan-template.json"
python3 - "$PLAN_TEMPLATE" "$SOURCE_VIDEO" "$SOURCE_AUDIO" "$SEGMENTS" <<'PY'
import json
import pathlib
import sys

plan_path, video_path, audio_path, segment_count = sys.argv[1:]
segment_count = int(segment_count)
plan = {
    "version": 1,
    "job_id": "native-render-baseline",
    "copy_only": True,
    "canvas": {"width": 640, "height": 360, "fps": 30},
    "timeline": [
        {
            "source": {"type": "video", "url": video_path},
            "duration_seconds": 1.0,
            "include_audio": False,
            "transform": {"scale_mode": "stretch", "slow_zoom": False},
        }
        for _ in range(segment_count)
    ],
    "audio_tracks": [
        {
            "source_url": audio_path,
            "volume": 1.0,
            "start_time_offset": 0.0,
            "loop": False,
        }
    ],
    "output_path": "__OUTPUT_PATH__",
}
pathlib.Path(plan_path).write_text(json.dumps(plan, indent=2), encoding="utf-8")
PY

REPORT="$OUTPUT_DIR/baseline.tsv"
printf 'run\ttotal_ms\tengine_execs\tprocess_forks\tengine_ffprobe_execs\tengine_ffmpeg_execs\tfinal_validation_ffprobe\tasset_copy_ops\tasset_copy_bytes\testimated_final_copy_ops\testimated_final_copy_bytes\tsidecar_temp_bytes\toutput_bytes\tframes_decoded\tframes_encoded\tencode_passes\tconcat_mode\n' >"$REPORT"

render_failures=0
for run in $(seq 1 "$RUNS"); do
  RUN_DIR="$OUTPUT_DIR/run-${run}"
  mkdir -p "$RUN_DIR"
  PLAN="$RUN_DIR/render-plan.json"
  OUTPUT="$RUN_DIR/output.mp4"
  TRACE="$RUN_DIR/strace.log"
  STDERR="$RUN_DIR/engine.stderr.log"
  STDOUT="$RUN_DIR/engine.stdout.log"
  TIMEFILE="$RUN_DIR/time.txt"
  PROBE="$RUN_DIR/final-ffprobe.json"

  python3 - "$PLAN_TEMPLATE" "$PLAN" "$OUTPUT" <<'PY'
import json
import pathlib
import sys

template_path, plan_path, output_path = sys.argv[1:]
plan = json.loads(pathlib.Path(template_path).read_text(encoding="utf-8"))
plan["output_path"] = output_path
pathlib.Path(plan_path).write_text(json.dumps(plan, indent=2), encoding="utf-8")
PY

  set +e
  /usr/bin/time -f '%e %U %S %M' -o "$TIMEFILE" \
    env VELOX_BENCH_DISK_COPY_METRICS=1 strace -f -e trace=process -o "$TRACE" \
    "$ENGINE_BIN" --render --plan "$PLAN" >"$STDOUT" 2>"$STDERR"
  engine_rc=$?
  set -e

  if [[ "$engine_rc" -ne 0 || ! -s "$OUTPUT" || ! -s "$OUTPUT.progress.json" ]]; then
    render_failures=$((render_failures + 1))
    printf '[native-render-baseline][FAIL] run=%s rc=%s evidence=%s\n' "$run" "$engine_rc" "$RUN_DIR" >&2
    tail -100 "$STDERR" >&2 || true
    continue
  fi

  ffprobe -v error -of json -show_streams -show_format "$OUTPUT" >"$PROBE" || {
    render_failures=$((render_failures + 1))
    printf '[native-render-baseline][FAIL] final ffprobe failed for run=%s\n' "$run" >&2
    continue
  }

  # strace records the direct engine exec plus every /bin/sh, ffprobe and
  # ffmpeg child. The current implementation uses system()/popen(), so the
  # shell count is intentional evidence rather than noise.
  engine_execs="$(grep -Ec 'execve\(".*/velox_video_engine"' "$TRACE" || true)"
  process_forks="$(grep -Ec '(^| )((clone|fork|vfork)\()' "$TRACE" || true)"
  engine_ffprobe_execs="$(grep -Ec 'execve\(".*/ffprobe"' "$TRACE" || true)"
  engine_ffmpeg_execs="$(grep -Ec 'execve\(".*/ffmpeg"' "$TRACE" || true)"

  # file_utils::copyFile emits one disk.copy JSON line for each cache/local
  # asset staging copy. The final fs::copy_file is accounted separately below.
  asset_copy_ops="$(grep -c '"metric":"disk.copy"' "$STDERR" 2>/dev/null || true)"
  asset_copy_bytes="$(grep '"metric":"disk.copy"' "$STDERR" 2>/dev/null | \
    sed -n 's/.*"bytes":\([0-9][0-9]*\).*/\1/p' | \
    awk '{sum += $1} END {print sum + 0}')"

  estimated_final_copy_bytes="$(stat -c '%s' "$OUTPUT")"
  sidecar="$OUTPUT.progress.json"
  sidecar_temp_bytes="$(jq -r '.temp_bytes // 0' "$sidecar")"
  frames_decoded="$(jq -r '.frames_decoded // 0' "$sidecar")"
  frames_encoded="$(jq -r '.frames // 0' "$sidecar")"
  encode_passes="$(jq -r '.encode_passes // 0' "$sidecar")"
  concat_mode="$(jq -r '.concat_mode // ""' "$sidecar")"
  total_ms="$(awk '{printf "%.3f", $1 * 1000}' "$TIMEFILE")"

  # One independent final ffprobe is deliberately retained as the quality
  # barrier. It is not included in engine_ffprobe_execs because it runs after
  # strace; final_validation_ffprobe=1 makes that distinction explicit.
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$run" "$total_ms" "$engine_execs" "$process_forks" \
    "$engine_ffprobe_execs" "$engine_ffmpeg_execs" "1" "$asset_copy_ops" \
    "$asset_copy_bytes" "1" "$estimated_final_copy_bytes" "$sidecar_temp_bytes" \
    "$(stat -c '%s' "$OUTPUT")" "$frames_decoded" "$frames_encoded" \
    "$encode_passes" "$concat_mode" >>"$REPORT"

  printf '[native-render-baseline] run=%s total_ms=%s engine_ffprobe=%s engine_ffmpeg=%s asset_copy_bytes=%s output_bytes=%s\n' \
    "$run" "$total_ms" "$engine_ffprobe_execs" "$engine_ffmpeg_execs" \
    "$asset_copy_bytes" "$estimated_final_copy_bytes"
done

python3 - "$REPORT" "$OUTPUT_DIR/summary.json" "$RUNS" "$SEGMENTS" "$SOURCE_VIDEO_BYTES" "$SOURCE_AUDIO_BYTES" <<'PY'
import json
import pathlib
import statistics
import sys

report, summary_path, runs, segments, source_video_bytes, source_audio_bytes = sys.argv[1:]
runs = int(runs)
segments = int(segments)
rows = []
with open(report, encoding="utf-8") as stream:
    headers = stream.readline().rstrip("\n").split("\t")
    for line in stream:
        if not line.strip():
            continue
        values = line.rstrip("\n").split("\t")
        row = dict(zip(headers, values))
        for key in headers:
            if key not in {"concat_mode"}:
                try:
                    row[key] = float(row[key])
                except (ValueError, TypeError):
                    pass
        rows.append(row)

summary = {
    "workload": {
        "runs_requested": runs,
        "runs_completed": len(rows),
        "segments": segments,
        "segment_duration_seconds": 1.0,
        "canvas": "640x360@30",
        "source_video_bytes": int(source_video_bytes),
        "source_audio_bytes": int(source_audio_bytes),
    },
    "measurement_notes": {
        "engine_spawn": "direct velox_video_engine execve under strace; Go RenderClient ProcessStartMs requires worker telemetry",
        "engine_ffprobe": "ffprobe execve children during C++ render",
        "final_validation_ffprobe": "one independent ffprobe after render, retained as quality barrier",
        "asset_copy_bytes": "disk.copy bytes emitted by file_utils::copyFile for local/cache asset staging",
        "estimated_final_copy_bytes": "size of the output from the explicit final fs::copy_file; estimate, not syscall instrumentation",
        "sidecar_temp_bytes": "C++ temp_bytes counter: generated segment/concat/final-mux files, not input staging copies",
    },
    "metrics": {},
}
for key in [
    "total_ms", "engine_execs", "process_forks", "engine_ffprobe_execs",
    "engine_ffmpeg_execs", "asset_copy_ops", "asset_copy_bytes",
    "estimated_final_copy_bytes", "sidecar_temp_bytes", "frames_decoded",
    "frames_encoded", "encode_passes",
]:
    values = [float(row[key]) for row in rows if key in row]
    if values:
        summary["metrics"][key] = {
            "mean": statistics.mean(values),
            "median": statistics.median(values),
            "min": min(values),
            "max": max(values),
        }
summary["metrics"]["final_validation_ffprobe"] = {"per_render": 1, "total": len(rows)}
pathlib.Path(summary_path).write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
print(json.dumps(summary, indent=2))
PY

if [[ "$render_failures" -ne 0 ]]; then
  fail_run "${render_failures} render(s) failed; evidence=${OUTPUT_DIR}"
fi

printf '[native-render-baseline][PASS] evidence=%s\n' "$OUTPUT_DIR"
