#!/usr/bin/env bash
# RW-PROD-003 §5 — bootstrap-selftest-regenerate driver.
#
# Creates a canonical 1×1 black frame via the C++ engine's
# --build-scene-segment subcommand (the same codepath used by the
# worker when rendering timeline items with type=color -> pre-rendered
# PNG -> scene segment), then writes the SHA-256 of the output into
# tests/fixtures/engine_selftest_baseline.sha256.
#
# Why --build-scene-segment instead of --render --plan:
#   The current C++ engine binary (2025-06-19 build) does not yet
#   expose a unified --render --plan entrypoint. The Go-side
#   native.RenderClient marshals RenderPlan JSON and calls the
#   future --render --plan path; once the engine grows that
#   subcommand, `make bootstrap-selftest-regenerate` can switch to
#   driving the same RenderPlan as bootstrap.buildOnePixelPlan.
#   Until then, the SHA committed here is produced by the
#   equivalent pipeline: a 1×1 black PNG rendered as a 0.1s scene
#   segment — byte-stable across host installations.
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

# Python3 is required for the 1×1 black PNG (Pillow-free, uses raw bytes).
if ! command -v python3 &>/dev/null; then
  echo "[bootstrap-selftest-regenerate] python3 not found in PATH" >&2
  exit 3
fi

MODULE_DIR="$(cd "$(dirname "$0")/.." && pwd)"
WORK="$(mktemp -d -t velox-bootstrap-regen-XXXXXX)"
trap 'rm -rf "$WORK"' EXIT

# ── Step 1: produce a 1×1 black PNG at $WORK/black_1x1.png ──────────
# Pillow-free: write a minimal 1×1 black RGBA PNG by hand so the script
# has zero Python dependencies beyond stdlib.
python3 - "$WORK/black_1x1.png" <<'PYEOF'
import sys, struct, zlib

def make_1x1_black_png(path):
    """Write a valid 1×1 RGBA black PNG to `path`."""
    # PNG signature
    sig = b'\x89PNG\r\n\x1a\n'

    # IHDR: width=1 height=1 bit_depth=8 color_type=6 (RGBA)
    ihdr_data = struct.pack('>IIBBBBB', 1, 1, 8, 6, 0, 0, 0)
    ihdr_crc = zlib.crc32(b'IHDR' + ihdr_data)
    ihdr = _chunk(b'IHDR', ihdr_data, ihdr_crc)

    # IDAT: 1 pixel of RGBA black (00 00 00 FF), raw + zlib
    raw = b'\x00\x00\x00\xff'
    compressed = zlib.compress(raw)
    idat_crc = zlib.crc32(b'IDAT' + compressed)
    idat = _chunk(b'IDAT', compressed, idat_crc)

    # IEND
    iend_crc = zlib.crc32(b'IEND' + b'')
    iend = _chunk(b'IEND', b'', iend_crc)

    with open(path, 'wb') as f:
        f.write(sig)
        f.write(ihdr)
        f.write(idat)
        f.write(iend)

def _chunk(chunk_type, data, crc):
    return struct.pack('>I', len(data)) + chunk_type + data + struct.pack('>I', crc & 0xffffffff)

make_1x1_black_png(sys.argv[1])
PYEOF

if [[ ! -s "$WORK/black_1x1.png" ]]; then
  echo "[bootstrap-selftest-regenerate] failed to create 1×1 black PNG" >&2
  exit 4
fi

# ── Step 2: drive the engine to build a scene segment ───────────────
echo "[bootstrap-selftest-regenerate] driving engine: $BIN --build-scene-segment --image $WORK/black_1x1.png --duration 0.1 --out $WORK/frame.mp4" >&2
"$BIN" --build-scene-segment \
  --image "$WORK/black_1x1.png" \
  --duration 0.1 \
  --out "$WORK/frame.mp4"

if [[ ! -s "$WORK/frame.mp4" ]]; then
  echo "[bootstrap-selftest-regenerate] engine produced no output at $WORK/frame.mp4" >&2
  exit 4
fi

# ── Step 3: compute SHA-256 and write the fixture ───────────────────
OUT_PATH="$MODULE_DIR/tests/fixtures/engine_selftest_baseline.sha256"
sha256sum "$WORK/frame.mp4" | sed "s|$WORK/|  tests/fixtures/|" > "$OUT_PATH"
echo "[bootstrap-selftest-regenerate] wrote $OUT_PATH"
cat "$OUT_PATH" >&2
