#!/usr/bin/env bash
# RW-PROD-003 §5 — bootstrap-selftest-regenerate driver.
#
# Drives the C++ engine to render the canonical 1×1 black frame (same
# plan shape as pkg/bootstrap.buildOnePixelPlan) and writes the SHA-256
# of the produced output into tests/fixtures/engine_selftest_baseline.sha256
# using sha256sum's `<hex>  <file>` format.
#
# Operator gate: this script MUST NOT run unattended. CI rejects it
# unless APPROVE_REGEN=1 is exported, mirroring the manual approval
# pattern from gen-production-pki.sh.

set -eu

if [[ "$${APPROVE_REGEN:-}" != "1" ]]; then
  echo "[bootstrap-selftest-regenerate] refusing to run — set APPROVE_REGEN=1 (manual gate, RW-PROD-003 §5)" >&2
  exit 2
fi

BIN="$${VELOX_VIDEO_ENGINE_CPP_BIN:-/usr/local/bin/velox_video_engine}"
if [[ ! -x "$$BIN" ]]; then
  echo "[bootstrap-selftest-regenerate] engine binary not executable at $$BIN" >&2
  exit 3
fi

WORK="$$(mktemp -d -t velox-bootstrap-regen-XXXXXX)"
trap 'rm -rf "$$WORK"' EXIT

# Plan shape mirrors pkg/bootstrap.buildOnePixelPlan byte-for-byte
# (canvas 1×1@1fps, 1 timeline item with ColorHex=#000000, 0.1s dur).
cat > "$$WORK/plan.json" <<'JSON'
{
  "version": 1,
  "job_id": "bootstrap.engine_selftest",
  "canvas": {"width": 1, "height": 1, "fps": 1},
  "timeline": [
    {"source": {"type": "color", "color_hex": "#000000"}, "duration_seconds": 0.1}
  ],
  "output_path": "__OUT__"
}
JSON
sed -i "s|__OUT__|$$WORK/frame.mp4|" "$$WORK/plan.json"

"$$BIN" --render --plan "$$WORK/plan.json"

if [[ ! -s "$$WORK/frame.mp4" ]]; then
  echo "[bootstrap-selftest-regenerate] engine produced no output at $$WORK/frame.mp4" >&2
  exit 4
fi

OUT_PATH="tests/fixtures/engine_selftest_baseline.sha256"
( cd "$$(dirname "$$BIN")/.." >/dev/null 2>&1 || true; cd "$$(dirname "$$0")/.." )
sha256sum "$$WORK/frame.mp4" > "$$OUT_PATH"
echo "[bootstrap-selftest-regenerate] wrote $$OUT_PATH with SHA of $$WORK/frame.mp4"
