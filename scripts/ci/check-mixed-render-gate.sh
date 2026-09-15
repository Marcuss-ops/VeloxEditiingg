#!/usr/bin/env bash
# check-mixed-render-gate.sh — native mixed packet-copy golden gate.
#
# This gate runs before the worker image is built. The C++ golden test asserts
# that a successful mixed render is packet-only (zero frames/encodes), rejects
# incompatible sources deterministically, and never reaches the legacy video
# renderer. The published-image canary repeats the same contract against the
# immutable runtime image.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly SCRIPT_DIR
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
readonly REPO_ROOT
readonly ENGINE_SOURCE="${REPO_ROOT}/RemoteCodex/native/video-engine-cpp"
readonly BUILD_DIR="${VIDEO_MIXED_BUILD_DIR:-/tmp/velox-mixed-render-build}"

for command_name in cmake ffmpeg ffprobe; do
  command -v "$command_name" >/dev/null 2>&1 || {
    echo "[mixed-render-gate][ERROR] missing dependency: ${command_name}" >&2
    exit 3
  }
done

echo "[mixed-render-gate] configuring native engine"
cmake -S "${ENGINE_SOURCE}" -B "${BUILD_DIR}" \
  -DCMAKE_BUILD_TYPE=Release \
  -DVELOX_ENABLE_LIBAV=ON

echo "[mixed-render-gate] building mixed golden test"
cmake --build "${BUILD_DIR}" --target velox_render_mixed_tests --parallel

echo "[mixed-render-gate] running mixed packet-copy contract"
"${BUILD_DIR}/velox_render_mixed_tests"

echo "[mixed-render-gate][PASS] mixed_packet capability contract holds"
