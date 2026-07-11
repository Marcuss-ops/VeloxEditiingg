#!/usr/bin/env bash
# RW-PROD-003 §5 — `make bootstrap-smoke` driver.
#
# Builds an isolated work-dir under mktemp, drops in:
#   1. a stub BUNDLE_HASH.txt with value == $STUB_HASH
#   2. a stub ffmpeg + ffprobe that exit 0 with a parsed "ffprobe version 4.x"
#      string and an "Encoder libx264 [...]" help line — enough to satisfy
#      bootstrap.runFFmpegSelfTest without consuming real CPU
#   3. a minimal cfg JSON wired at the bootstrap-stable fields
#
# Then it runs `go test ./pkg/bootstrap/... -run TestRun_AllOK_Smoke`
# in the module directory. Exit 0 only when every step is OK.

set -u

# Module-relative paths; we expect bash to run from the module dir.
WORK_DIR="$$(mktemp -d -t velox-bootstrap-smoke-XXXXXX)"
trap 'rm -rf "$$WORK_DIR"' EXIT

STUB_BIN_DIR="$$WORK_DIR/stub_bin"
mkdir -p "$$STUB_BIN_DIR"

# Stub ffmpeg — pure exit-0; ffmpeg -h encoder=libx264 prints a fake
# encoder line that pkg/bootstrap's regex matches.
cat > "$$STUB_BIN_DIR/ffmpeg" <<'SH'
#!/usr/bin/env bash
if [[ "$$*" == *"-version"* ]]; then
  echo "ffmpeg version 4.4.2-stub Copyright (c) 2000-202X the ffmpeg developers"
  exit 0
fi
if [[ "$$*" == *"encoder=libx264"* ]]; then
  echo "Encoder libx264 [libx264 H.264 / AVC / MPEG-4 AVC / MPEG-4 part 10] (codec libx264)"
  echo "    General capabilities: threads"
  exit 0
fi
exit 0
SH
chmod +x "$$STUB_BIN_DIR/ffmpeg"

# Stub ffprobe — same: pretend to be version 4.x.
cat > "$$STUB_BIN_DIR/ffprobe" <<'SH'
#!/usr/bin/env bash
if [[ "$$*" == *"-version"* ]]; then
  echo "ffprobe version 4.4.2-stub Copyright (c) 2000-202X the FFmpeg developers"
  exit 0
fi
exit 0
SH
chmod +x "$$STUB_BIN_DIR/ffprobe"

# BUNDLE_HASH.txt must exist under one of pkg/bundle.canonicalCandidates
# so the bootstrap pass succeeds.
STUB_HASH="stubhash$$(date +%s)$$RANDOM"
echo "$$STUB_HASH" > "$$WORK_DIR/BUNDLE_HASH.txt"

# Drive the bootstrap test in the Go module — Run() is invoked against
# a fake render client that writes a known 1×1 black frame SHA. The
# baseline fixture is regenerated per smoke (the test's t.TempDir
# baseline matches what the fake RenderClient writes).
STUB_PATH="$$STUB_BIN_DIR"
export PATH="$$STUB_PATH:$$PATH"

cd "$$(dirname "$$0")/.." || exit 1
go test -count=1 -timeout=120s -run 'TestRun_AllOK_Smoke' ./pkg/bootstrap/... || exit 1

echo "[bootstrap-smoke] OK: STUB_HASH=$$STUB_HASH work_dir=$$WORK_DIR"
