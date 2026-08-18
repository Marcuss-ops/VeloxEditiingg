#!/usr/bin/env bash
# scripts/ci/check-copy-vs-encode-benchmark.sh
#
# Copy-vs-encode benchmark gate (plan §17) for the visual_replacement path.
# It builds and runs the native benchmark that renders the SAME timeline
# through the copy-only packet path and the legacy full-encode path, then
# re-reads the emitted benchmark.json and fails the job on any architectural
# regression — the copy-only path must never re-encode, must never spawn an
# external tool, and must be strictly cheaper than a full decode→encode.
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

BENCHMARK_JSON="${EVIDENCE_DIR}/benchmark.json"
[[ -f "${BENCHMARK_JSON}" ]] || {
  echo "[copy-vs-encode-benchmark][FAIL] benchmark.json missing: ${BENCHMARK_JSON}" >&2
  exit 1
}

echo "[copy-vs-encode-benchmark] asserting architectural invariants from benchmark.json"
python3 - "${BENCHMARK_JSON}" <<'PY'
import json
import sys

benchmark_path = sys.argv[1]
with open(benchmark_path, encoding="utf-8") as stream:
    data = json.load(stream)

violations = []

def sample(name):
    value = data.get(name)
    if not isinstance(value, dict):
        violations.append(f"missing sample {name!r}")
        return {}
    return value

copy_cold = sample("copy_cold")
copy_warm = sample("copy_warm")
full_encode = sample("full_encode")

def assert_eq(field, value, want):
    if value != want:
        violations.append(f"{field}={value}, want {want}")

def assert_lt(field, value, want):
    if not value < want:
        violations.append(f"{field}={value}, want < {want}")

if copy_cold:
    assert_eq("copy_cold.frames_encoded", int(copy_cold.get("frames_encoded", -1)), 0)
    assert_eq("copy_cold.frames_decoded", int(copy_cold.get("frames_decoded", -1)), 0)
    assert_eq("copy_cold.encode_passes", int(copy_cold.get("encode_passes", -1)), 0)
    assert_eq("copy_cold.copy_segments", int(copy_cold.get("copy_segments", -1)), 3)
    assert_eq("copy_cold.transcode_segments", int(copy_cold.get("transcode_segments", -1)), 0)
    assert_eq("copy_cold.file_copy_count", int(copy_cold.get("file_copy_count", -1)), 0)
    if copy_cold.get("concat_mode") != "packet_copy":
        violations.append(f"copy_cold.concat_mode={copy_cold.get('concat_mode')!r}, want 'packet_copy'")

if full_encode:
    if not int(full_encode.get("frames_encoded", -1)) > 0:
        violations.append(f"full_encode.frames_encoded={full_encode.get('frames_encoded')}, want > 0")

if copy_cold and full_encode:
    assert_lt(
        "copy_cold.wall_ms < full_encode.wall_ms",
        float(copy_cold.get("wall_ms", 0)),
        float(full_encode.get("wall_ms", 0)),
    )

if copy_warm:
    assert_eq("copy_warm.file_copy_count", int(copy_warm.get("file_copy_count", -1)), 0)
    assert_eq("copy_warm.asset_bytes_copied", int(copy_warm.get("asset_bytes_copied", -1)), 0)

if violations:
    print("[copy-vs-encode-benchmark][FAIL] architectural invariants violated:", file=sys.stderr)
    for violation in violations:
        print(f"  - {violation}", file=sys.stderr)
    sys.exit(2)

print(
    f"[copy-vs-encode-benchmark][OK] copy wall_ms={copy_cold.get('wall_ms')} "
    f"frames_encoded={copy_cold.get('frames_encoded')} "
    f"copy_segments={copy_cold.get('copy_segments')} "
    f"transcode_segments={copy_cold.get('transcode_segments')} "
    f"vs encode wall_ms={full_encode.get('wall_ms')} "
    f"frames_encoded={full_encode.get('frames_encoded')}"
)
PY

echo "[copy-vs-encode-benchmark][PASS] evidence=${EVIDENCE_DIR}"
