#!/usr/bin/env bash
# scripts/ci/check-zero-render-gate.sh
#
# Zero-render release gate for the currently supported visual-replacement
# contract. Existing-video replacement is deliberately refused before any
# output is created; this gate pins that fail-closed behavior.
#
# The gate asserts, simultaneously:
#   video_reencode_forbidden error
#   frames_encoded       == 0
#   encode_passes        == 0
#   output file          absent
#
# This is the durable enforcement: six months from now, if a worker change
# accidentally turns a prepared segment into decode → encode, this gate turns
# red instead of silently shipping the regression.
#
# Environment:
#   VIDEO_ZERO_RENDER_BUILD_DIR  CMake build directory (default: /tmp/velox-zero-render-build)
#   VIDEO_ZERO_RENDER_EVIDENCE   Evidence directory (default: a temp dir)
#   VIDEO_ZERO_RENDER_KEEP       Keep evidence when set to 1
#
# Exit codes:
#   0   zero-render invariants hold
#   1   build / test execution failure
#   2   transcode detected (a zero-render invariant was violated)
#   3   missing dependency (cmake, ffmpeg, ffprobe, python3)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly SCRIPT_DIR
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
readonly REPO_ROOT
readonly ENGINE_SOURCE="${REPO_ROOT}/RemoteCodex/native/video-engine-cpp"
readonly BUILD_DIR="${VIDEO_ZERO_RENDER_BUILD_DIR:-/tmp/velox-zero-render-build}"

for command_name in cmake ffmpeg ffprobe python3; do
  command -v "$command_name" >/dev/null 2>&1 || {
    echo "[zero-render-gate][ERROR] missing dependency: ${command_name}" >&2
    exit 3
  }
done

if [[ -z "${VIDEO_ZERO_RENDER_EVIDENCE:-}" ]]; then
  EVIDENCE_DIR="$(mktemp -d "${TMPDIR:-/tmp}/velox-zero-render.XXXXXX")"
  CLEANUP_EVIDENCE=1
else
  EVIDENCE_DIR="${VIDEO_ZERO_RENDER_EVIDENCE}"
  mkdir -p "${EVIDENCE_DIR}"
  CLEANUP_EVIDENCE=0
fi

cleanup() {
  if [[ "${VIDEO_ZERO_RENDER_KEEP:-0}" != "1" && "${CLEANUP_EVIDENCE}" == "1" ]]; then
    rm -rf -- "${EVIDENCE_DIR}"
  fi
}
trap cleanup EXIT

echo "[zero-render-gate] configuring engine (LibAV packet mux)"
mkdir -p "${BUILD_DIR}"
cmake -S "${ENGINE_SOURCE}" -B "${BUILD_DIR}" \
  -DCMAKE_BUILD_TYPE=Release \
  -DVELOX_ENABLE_LIBAV=ON \
  >"${EVIDENCE_DIR}/cmake-configure.log" 2>&1 || {
    cat "${EVIDENCE_DIR}/cmake-configure.log" >&2
    echo "[zero-render-gate][FAIL] CMake configure failed" >&2
    exit 1
  }

echo "[zero-render-gate] building golden test target"
cmake --build "${BUILD_DIR}" --target velox_render_visual_replacement_golden_tests --parallel \
  >"${EVIDENCE_DIR}/cmake-build.log" 2>&1 || {
    cat "${EVIDENCE_DIR}/cmake-build.log" >&2
    echo "[zero-render-gate][FAIL] golden test build failed" >&2
    exit 1
  }

echo "[zero-render-gate] running golden end-to-end test"
GOLDEN_EVIDENCE="${EVIDENCE_DIR}/golden"
mkdir -p "${GOLDEN_EVIDENCE}"
VELOX_GOLDEN_EVIDENCE_DIR="${GOLDEN_EVIDENCE}" \
  "${BUILD_DIR}/velox_render_visual_replacement_golden_tests" \
  >"${EVIDENCE_DIR}/golden.stdout.log" 2>&1 || {
    cat "${EVIDENCE_DIR}/golden.stdout.log" >&2
    echo "[zero-render-gate][FAIL] golden test failed" >&2
    exit 1
  }

OUTPUT="${TMPDIR:-/tmp}/velox-overlay-zero-reencode-golden.mp4"
[[ ! -e "${OUTPUT}" ]] || {
  echo "[zero-render-gate][FAIL] refused replacement produced output: ${OUTPUT}" >&2
  exit 1
}
echo "[zero-render-gate][PASS] evidence=${EVIDENCE_DIR}"
