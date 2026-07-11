#!/usr/bin/env bash
# RW-PROD-003 §5 — bootstrap-selftest-regenerate driver.
#
# Creates the canonical 2×2 black-frame RenderPlan used by
# pkg/bootstrap.runEngineSelfRender, drives it through the C++ engine's
# `--render --plan` entrypoint, and writes the SHA-256 of the output into
# tests/fixtures/engine_selftest_baseline.sha256.
#
# Operator gate: this script MUST NOT run unattended. CI rejects it
# unless APPROVE_REGEN=1 is exported, mirroring the manual approval
# pattern from gen-production-pki.sh.

set -euo pipefail

if [[ "${APPROVE_REGEN:-}" != "1" ]]; then
  echo "[bootstrap-selftest-regenerate] refusing to run — set APPROVE_REGEN=1 (manual gate, RW-PROD-003 §5)" >&2
  exit 2
fi

BIN="${VELOX_VIDEO_ENGINE_CPP_BIN:-/usr/local/bin/velox_video_engine}"
if [[ ! -x "$BIN" ]]; then
  echo "[bootstrap-selftest-regenerate] engine binary not executable at $BIN" >&2
  exit 3
fi

MODULE_DIR="$(cd "$(dirname "$0")/.." && pwd)"
WORK="$(mktemp -d -t velox-bootstrap-regen-XXXXXX)"
trap 'rm -rf "$WORK"' EXIT

# ── Step 1: write the canonical RenderPlan ──────────────────────────
cat > "$WORK/render_plan.json" <<EOF
{
  "version": 1,
  "job_id": "bootstrap.engine_selftest",
  "canvas": {
    "width": 2,
    "height": 2,
    "fps": 1
  },
  "timeline": [
    {
      "source": {
        "type": "color",
        "color_hex": "#000000"
      },
      "duration_seconds": 0.1
    }
  ],
  "output_path": "$WORK/frame.mp4"
}
EOF

# ── Step 2: drive the engine through the canonical render entrypoint ───────
echo "[bootstrap-selftest-regenerate] driving engine: $BIN --render --plan $WORK/render_plan.json" >&2
"$BIN" --render --plan "$WORK/render_plan.json"

if [[ ! -s "$WORK/frame.mp4" ]]; then
  echo "[bootstrap-selftest-regenerate] engine produced no output at $WORK/frame.mp4" >&2
  exit 4
fi

# ── Step 3: compute SHA-256 and write the fixture ───────────────────
OUT_PATH="$MODULE_DIR/tests/fixtures/engine_selftest_baseline.sha256"
sha256sum "$WORK/frame.mp4" | sed "s|$WORK/|  tests/fixtures/|" > "$OUT_PATH"
echo "[bootstrap-selftest-regenerate] wrote $OUT_PATH"
cat "$OUT_PATH" >&2
