#!/usr/bin/env bash
# scripts/ci/check-zero-render-gate.sh
#
# Zero-render release gate (plan §16) for the visual_replacement copy-only
# path. It builds and runs the golden end-to-end test, then re-reads the
# engine sidecar and fails the job if ANY transcode happened — even when the
# final video looks correct.
#
# The gate asserts, simultaneously:
#   frames_encoded      == 0
#   frames_decoded      == 0
#   frames_composited   == 0
#   encode_passes       == 0
#   transcode_segments  == 0
#   copy_segments       >= 3   (BASE/PREPARED/BASE for the golden job)
#   concat_mode         == "packet_copy"
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

SIDECAR="${GOLDEN_EVIDENCE}/golden-output.mp4.progress.json"
[[ -f "${SIDECAR}" ]] || {
  echo "[zero-render-gate][FAIL] sidecar missing: ${SIDECAR}" >&2
  exit 1
}

echo "[zero-render-gate] asserting zero-render invariants from sidecar"
python3 - "${SIDECAR}" <<'PY'
import json
import sys

sidecar_path = sys.argv[1]
with open(sidecar_path, encoding="utf-8") as stream:
    sidecar = json.load(stream)

# The gate fails on ANY transcode, even if the final video looks correct.
violations = []

def assert_eq(field, value, want):
    if value != want:
        violations.append(f"{field}={value}, want {want}")

def assert_ge(field, value, want):
    if value < want:
        violations.append(f"{field}={value}, want >= {want}")

assert_eq("frames_encoded", int(sidecar.get("frames", -1)), 0)
assert_eq("frames_decoded", int(sidecar.get("frames_decoded", -1)), 0)
assert_eq("frames_composited", int(sidecar.get("frames_composited", -1)), 0)
assert_eq("encode_passes", int(sidecar.get("encode_passes", -1)), 0)
assert_eq("transcode_segments", int(sidecar.get("transcode_segments", -1)), 0)
assert_ge("copy_segments", int(sidecar.get("copy_segments", -1)), 3)

mode = sidecar.get("concat_mode", "")
if mode != "packet_copy":
    violations.append(f"concat_mode={mode!r}, want 'packet_copy'")

if violations:
    print("[zero-render-gate][FAIL] zero-render invariants violated:", file=sys.stderr)
    for violation in violations:
        print(f"  - {violation}", file=sys.stderr)
    sys.exit(2)

print(
    f"[zero-render-gate][OK] copy_segments={sidecar.get('copy_segments')} "
    f"transcode_segments={sidecar.get('transcode_segments')} "
    f"frames_encoded={sidecar.get('frames')} "
    f"frames_decoded={sidecar.get('frames_decoded')} "
    f"concat_mode={mode!r}"
)
PY

echo "[zero-render-gate][PASS] evidence=${EVIDENCE_DIR}"
