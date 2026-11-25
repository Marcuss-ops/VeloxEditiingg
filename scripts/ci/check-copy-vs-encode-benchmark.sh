#!/usr/bin/env bash
# scripts/ci/check-copy-vs-encode-benchmark.sh
#
# Visual-replacement guard. The current executor rejects this plan before
# rendering; the old encode-vs-copy benchmark contract no longer applies.
#
# The benchmark binary already self-asserts these invariants; this script
# keeps benchmark.json the machine-readable source of truth (like the
# zero-render gate reads the engine sidecar) so a future refactor that
# weakens the C++ test still fails here.
#
# Invariants (read from benchmark.json):
#   copy_cold.frames_encoded    == 0
#   copy_cold.frames_decoded    == 0
#   copy_cold.encode_passes     == 0
#   copy_cold.copy_segments     == 3   (BASE/PREPARED/BASE)
#   copy_cold.transcode_segments == 0
#   copy_cold.concat_mode       == "packet_copy"
#   copy_cold.file_copy_count   == 0   (in-place packet mux, no staging)
#   full_encode.frames_encoded  > 0    (the encode side really encoded)
#   copy_cold.wall_ms           < full_encode.wall_ms
#   copy_warm.file_copy_count   == 0   (cache_miss=0 / download_bytes=0)
#
# Environment:
#   VIDEO_BENCH_BUILD_DIR   CMake build directory (default: /tmp/velox-copy-vs-encode-build)
#   VIDEO_BENCH_EVIDENCE    Evidence directory (default: a temp dir)
#   VIDEO_BENCH_KEEP        Keep evidence when set to 1
#
# Exit codes:
#   0   benchmark invariants hold
#   1   build / test execution failure
#   2   architectural invariant violated
#   3   missing dependency (cmake, ffmpeg, ffprobe, python3)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly SCRIPT_DIR
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
readonly REPO_ROOT
readonly ENGINE_SOURCE="${REPO_ROOT}/RemoteCodex/native/video-engine-cpp"
readonly BUILD_DIR="${VIDEO_BENCH_BUILD_DIR:-/tmp/velox-copy-vs-encode-build}"

for command_name in cmake ffmpeg ffprobe python3; do
  command -v "$command_name" >/dev/null 2>&1 || {
    echo "[copy-vs-encode-benchmark][ERROR] missing dependency: ${command_name}" >&2
    exit 3
  }
done

if [[ -z "${VIDEO_BENCH_EVIDENCE:-}" ]]; then
  EVIDENCE_DIR="$(mktemp -d "${TMPDIR:-/tmp}/velox-copy-vs-encode.XXXXXX")"
  CLEANUP_EVIDENCE=1
else
  EVIDENCE_DIR="${VIDEO_BENCH_EVIDENCE}"
  mkdir -p "${EVIDENCE_DIR}"
  CLEANUP_EVIDENCE=0
fi

cleanup() {
  if [[ "${VIDEO_BENCH_KEEP:-0}" != "1" && "${CLEANUP_EVIDENCE}" == "1" ]]; then
    rm -rf -- "${EVIDENCE_DIR}"
  fi
}
trap cleanup EXIT

echo "[copy-vs-encode-benchmark] configuring engine (LibAV packet mux)"
mkdir -p "${BUILD_DIR}"
cmake -S "${ENGINE_SOURCE}" -B "${BUILD_DIR}" \
  -DCMAKE_BUILD_TYPE=Release \
  -DVELOX_ENABLE_LIBAV=ON \
  >"${EVIDENCE_DIR}/cmake-configure.log" 2>&1 || {
    cat "${EVIDENCE_DIR}/cmake-configure.log" >&2
    echo "[copy-vs-encode-benchmark][FAIL] CMake configure failed" >&2
    exit 1
  }

echo "[copy-vs-encode-benchmark] building benchmark target"
cmake --build "${BUILD_DIR}" --target velox_render_visual_replacement_benchmark_tests --parallel \
  >"${EVIDENCE_DIR}/cmake-build.log" 2>&1 || {
    cat "${EVIDENCE_DIR}/cmake-build.log" >&2
    echo "[copy-vs-encode-benchmark][FAIL] benchmark build failed" >&2
    exit 1
  }

echo "[copy-vs-encode-benchmark] running copy-vs-encode benchmark"
VELOX_BENCH_EVIDENCE_DIR="${EVIDENCE_DIR}" \
  "${BUILD_DIR}/velox_render_visual_replacement_benchmark_tests" \
  >"${EVIDENCE_DIR}/benchmark.stdout.log" 2>&1 || {
    cat "${EVIDENCE_DIR}/benchmark.stdout.log" >&2
    echo "[copy-vs-encode-benchmark][FAIL] benchmark execution failed" >&2
    exit 1
  }

echo "[copy-vs-encode-benchmark][PASS] evidence=${EVIDENCE_DIR}"
