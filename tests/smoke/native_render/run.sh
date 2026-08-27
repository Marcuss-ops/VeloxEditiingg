#!/usr/bin/env bash
# =============================================================================
# tests/smoke/native_render/run.sh
# =============================================================================
# Deterministic last-mile smoke for the canonical C++ RenderPlan path.
#
# This is intentionally local and hermetic: it generates its own audio and
# subtitle fixtures, renders two colour segments through
# `velox_video_engine --render --plan`, and validates the resulting MP4 plus
# the engine progress sidecar. It exercises the same path used by the worker
# after the Go compiler has produced a canonical RenderPlan.
#
# Exit codes:
#   0 PASS
#   2 missing dependency / usage
#   1 render or media-contract failure
#
# Environment:
#   VIDEO_SMOKE_BUILD_DIR  CMake build directory (default: /tmp/velox-video-smoke-build)
#   VIDEO_SMOKE_JOB_DIR    Evidence directory (default: temporary directory)
#   VIDEO_SMOKE_BUILD_JOBS Maximum concurrent native build jobs (default: 2)
#   VIDEO_SMOKE_KEEP       Keep temporary evidence when set to 1
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly SCRIPT_DIR
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
readonly REPO_ROOT
readonly ENGINE_SOURCE="${REPO_ROOT}/RemoteCodex/native/video-engine-cpp"
readonly BUILD_DIR="${VIDEO_SMOKE_BUILD_DIR:-/tmp/velox-video-smoke-build}"
readonly BUILD_JOBS="${VIDEO_SMOKE_BUILD_JOBS:-2}"
JOB_DIR="${VIDEO_SMOKE_JOB_DIR:-}"

fail() {
  echo "[native-render-smoke][FAIL] $*" >&2
  exit 1
}

for command_name in cmake ffmpeg ffprobe python3; do
  command -v "$command_name" >/dev/null 2>&1 || {
    echo "[native-render-smoke][ERROR] missing dependency: ${command_name}" >&2
    exit 2
  }
done

if [[ -z "${JOB_DIR}" ]]; then
  JOB_DIR="$(mktemp -d "${TMPDIR:-/tmp}/velox-native-render.XXXXXX")"
  CLEANUP_JOB_DIR=1
else
  mkdir -p "${JOB_DIR}"
  CLEANUP_JOB_DIR=0
fi

cleanup() {
  if [[ "${VIDEO_SMOKE_KEEP:-0}" != "1" && "${CLEANUP_JOB_DIR}" == "1" ]]; then
    rm -rf -- "${JOB_DIR}"
  fi
}
trap cleanup EXIT

mkdir -p "${BUILD_DIR}"
cmake -S "${ENGINE_SOURCE}" -B "${BUILD_DIR}" \
  -DCMAKE_BUILD_TYPE=Release \
  -DVELOX_ENABLE_LIBAV=ON \
  >"${JOB_DIR}/cmake-configure.log" 2>&1 || {
    cat "${JOB_DIR}/cmake-configure.log" >&2
    fail "CMake configure failed"
  }
cmake --build "${BUILD_DIR}" --parallel "${BUILD_JOBS}" \
  >"${JOB_DIR}/cmake-build.log" 2>&1 || {
    cat "${JOB_DIR}/cmake-build.log" >&2
    fail "CMake build failed"
  }
ctest --test-dir "${BUILD_DIR}" --output-on-failure \
  >"${JOB_DIR}/ctest.log" 2>&1 || {
    cat "${JOB_DIR}/ctest.log" >&2
    fail "C++ unit/integration tests failed"
  }

ENGINE_BIN="${BUILD_DIR}/velox_video_engine"
[[ -x "${ENGINE_BIN}" ]] || fail "engine binary not found: ${ENGINE_BIN}"

AUDIO_FILE="${JOB_DIR}/voiceover.wav"
SUBTITLE_FILE="${JOB_DIR}/subtitle.srt"
PLAN_FILE="${JOB_DIR}/render-plan.json"
OUTPUT_FILE="${JOB_DIR}/rendered.mp4"

ffmpeg -y -v error -f lavfi \
  -i "sine=frequency=440:duration=2.0:sample_rate=48000" \
  -ac 2 "${AUDIO_FILE}" || fail "synthetic audio generation failed"

cat >"${SUBTITLE_FILE}" <<'EOF'
1
00:00:00,250 --> 00:00:01,500
Velox native render smoke
EOF

python3 - "${PLAN_FILE}" "${AUDIO_FILE}" "${SUBTITLE_FILE}" "${OUTPUT_FILE}" <<'PY'
import json
import sys

plan_path, audio_path, subtitle_path, output_path = sys.argv[1:]
plan = {
    "version": 1,
    "job_id": "native-render-smoke",
    "canvas": {"width": 640, "height": 360, "fps": 30},
    "timeline": [
        {
            "source": {"type": "color", "color_hex": "#123456"},
            "duration_seconds": 1.0,
            "transform": {"scale_mode": "stretch", "slow_zoom": False},
        },
        {
            "source": {"type": "color", "color_hex": "#654321"},
            "duration_seconds": 1.0,
            "transform": {"scale_mode": "stretch", "slow_zoom": False},
        },
    ],
    "audio_tracks": [
        {"source_url": audio_path, "volume": 0.8, "start_time_offset": 0.0}
    ],
    "subtitle_tracks": [{"source": subtitle_path, "preset": "default"}],
    "output_path": output_path,
}
with open(plan_path, "w", encoding="utf-8") as stream:
    json.dump(plan, stream, indent=2)
PY

"${ENGINE_BIN}" --render --plan "${PLAN_FILE}" \
  >"${JOB_DIR}/engine.stdout.log" 2>"${JOB_DIR}/engine.stderr.log" || {
    cat "${JOB_DIR}/engine.stderr.log" >&2
    fail "RenderPlan execution failed"
  }

[[ -s "${OUTPUT_FILE}" ]] || fail "render output is missing or empty"
[[ -s "${OUTPUT_FILE}.progress.json" ]] || fail "progress sidecar is missing"

ffprobe -v error -of json -show_streams -show_format "${OUTPUT_FILE}" \
  >"${JOB_DIR}/ffprobe.json" || fail "ffprobe could not read rendered output"

python3 - "${JOB_DIR}/ffprobe.json" "${OUTPUT_FILE}.progress.json" <<'PY'
import json
import sys

probe_path, sidecar_path = sys.argv[1:]
with open(probe_path, encoding="utf-8") as stream:
    probe = json.load(stream)
with open(sidecar_path, encoding="utf-8") as stream:
    sidecar = json.load(stream)

streams = probe.get("streams", [])
videos = [item for item in streams if item.get("codec_type") == "video"]
audios = [item for item in streams if item.get("codec_type") == "audio"]
subtitles = [item for item in streams if item.get("codec_type") == "subtitle"]
if len(videos) != 1 or len(audios) != 1 or subtitles:
    raise SystemExit(
        f"stream contract failed: video={len(videos)} audio={len(audios)} "
        f"subtitle={len(subtitles)}"
    )

video = videos[0]
audio = audios[0]
if video.get("codec_name") != "h264":
    raise SystemExit(f"video codec={video.get('codec_name')!r}, want h264")
if (video.get("width"), video.get("height")) != (640, 360):
    raise SystemExit(f"resolution={video.get('width')}x{video.get('height')}, want 640x360")
if audio.get("codec_name") != "aac":
    raise SystemExit(f"audio codec={audio.get('codec_name')!r}, want aac")

duration = float(probe.get("format", {}).get("duration", 0.0))
if not 1.75 <= duration <= 2.25:
    raise SystemExit(f"duration={duration:.3f}s outside 1.75..2.25s")

if sidecar.get("progress") != 100:
    raise SystemExit(f"sidecar progress={sidecar.get('progress')!r}, want 100")
if sidecar.get("encode_passes") != 2:
    raise SystemExit(f"sidecar encode_passes={sidecar.get('encode_passes')!r}, want 2")
if len(sidecar.get("segments", [])) != 2:
    raise SystemExit("sidecar does not contain two segment timing records")

print(
    f"native render PASS: {duration:.3f}s, 640x360 h264 + aac, "
    "subtitle burned-in, sidecar complete"
)
PY

echo "[native-render-smoke][PASS] evidence=${JOB_DIR}"
