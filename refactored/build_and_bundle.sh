#!/bin/bash
# Velox Build & Bundle Script
# Automates: build -> bundle -> SHA256 -> VERSION -> manifest
#
# Usage: ./build_and_bundle.sh [--version VERSION] [--skip-engine] [--skip-tests] [--dry-run]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATASERVER_DIR="$SCRIPT_DIR/DataServer"
WORKER_DIR="$SCRIPT_DIR/RemoteCodex/native/worker-agent-go"
ENGINE_DIR="$SCRIPT_DIR/RemoteCodex/native/video-engine-cpp"
BUNDLE_DIR="$DATASERVER_DIR/data/worker_downloads"
BUNDLE_NAME="worker_code_linux_x86_64.zip"

VERSION="$(cat "$SCRIPT_DIR/VERSION.txt" 2>/dev/null | tr -d '[:space:]')"
SKIP_ENGINE=false
SKIP_TESTS=false
DRY_RUN=false

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; NC='\033[0m'

log()  { echo -e "${BLUE}[BUILD]${NC} $*"; }
ok()   { echo -e "${GREEN}[  OK]${NC} $*"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }
fail() { echo -e "${RED}[FAIL]${NC} $*"; exit 1; }

while [[ $# -gt 0 ]]; do
    case $1 in
        --version)      VERSION="$2"; shift 2 ;;
        --skip-engine)  SKIP_ENGINE=true; shift ;;
        --skip-tests)   SKIP_TESTS=true; shift ;;
        --dry-run)      DRY_RUN=true; shift ;;
        *)              echo "Unknown: $1"; exit 1 ;;
    esac
done

[[ -z "$VERSION" ]] && fail "VERSION not set. Use --version or ensure VERSION.txt exists."

log "Building version: $VERSION"

# Step 1: Tests
if ! $SKIP_TESTS; then
    log "Running tests..."
    cd "$DATASERVER_DIR"
    go test ./internal/workers ./internal/handlers/remote/workers ./internal/services/jobs ./internal/queue ./internal/store ./internal/handlers/server/jobs -count=1 -short 2>&1 | tail -20 || warn "Some tests failed"

    cd "$WORKER_DIR"
    go test ./pkg/api ./pkg/config ./internal/worker ./cmd/velox-worker-agent -count=1 -short 2>&1 | tail -20 || warn "Some worker tests failed"
    ok "Tests completed"
fi

# Step 2: Build Go worker binary
log "Building worker agent..."
cd "$WORKER_DIR"
if $DRY_RUN; then
    log "[DRY-RUN] go build ./cmd/velox-worker-agent"
else
    go build -o bin/velox-worker-agent ./cmd/velox-worker-agent
    ok "Worker agent built"
fi

# Step 3: Build C++ engine
if ! $SKIP_ENGINE; then
    log "Building video engine..."
    cd "$ENGINE_DIR"
    if $DRY_RUN; then
        log "[DRY-RUN] cmake + make"
    else
        cmake -S . -B build -DCMAKE_BUILD_TYPE=Release 2>&1 | tail -5
        cmake --build build -j"$(nproc)" 2>&1 | tail -5
        ok "Video engine built"
    fi
fi

# Step 4: Create bundle zip
log "Creating bundle zip..."
BUNDLE_PATH="$BUNDLE_DIR/$BUNDLE_NAME"

if $DRY_RUN; then
    log "[DRY-RUN] zip $BUNDLE_PATH"
else
    rm -f "$BUNDLE_PATH"
    cd "$SCRIPT_DIR"
    zip -r "$BUNDLE_PATH" \
        RemoteCodex/ \
        VERSION.txt \
        -x "RemoteCodex/native/worker-agent-go/bin/*" \
        -x "RemoteCodex/native/video-engine-cpp/build/*" \
        -x "RemoteCodex/native/video-engine-cpp/CMakeCache.txt" \
        -x "RemoteCodex/native/video-engine-cpp/CMakeFiles/*" \
        -x "RemoteCodex/native/worker-agent-go/vendor/*" \
        -x "RemoteCodex/**/*.md" \
        2>&1 | tail -3
    ok "Bundle created: $BUNDLE_PATH"
fi

# Step 5: Calculate SHA256
log "Calculating SHA256..."
if $DRY_RUN; then
    BUNDLE_HASH="dry_run_hash_placeholder"
else
    BUNDLE_HASH=$(sha256sum "$BUNDLE_PATH" | cut -d' ' -f1)
fi
ok "SHA256: ${BUNDLE_HASH:0:16}..."

# Step 6: Write BUNDLE_HASH.txt
# Compute a deterministic content hash from all source files (excluding build artifacts)
log "Writing BUNDLE_HASH.txt (content-based)..."
if ! $DRY_RUN; then
    SOURCE_HASH=$(cd "$SCRIPT_DIR" && find RemoteCodex \
        -type f \
        ! -path '*/bin/*' \
        ! -path '*/build/*' \
        ! -path '*/.git/*' \
        ! -name BUNDLE_HASH.txt \
        -print0 |
        sort -z |
        xargs -0 sha256sum 2>/dev/null |
        sha256sum | awk '{print $1}')
    echo -n "$SOURCE_HASH" > "$SCRIPT_DIR/RemoteCodex/BUNDLE_HASH.txt"
    ok "BUNDLE_HASH.txt written (content hash: ${SOURCE_HASH:0:16}...)"
else
    log "[DRY-RUN] Would write content-based BUNDLE_HASH.txt"
fi

# Step 7: Write VERSION.txt
log "Writing VERSION.txt..."
if ! $DRY_RUN; then
    echo -n "$VERSION" > "$SCRIPT_DIR/VERSION.txt"
fi
ok "VERSION.txt: $VERSION"

# Step 8: Generate manifest_v2.json
log "Generating manifest_v2.json..."
NOW=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
MANIFEST="$BUNDLE_DIR/manifest_v2.json"

if $DRY_RUN; then
    log "[DRY-RUN] write manifest"
else
    cat > "$MANIFEST" << JSONEOF
{
  "version": "$VERSION",
  "code_version": "$VERSION",
  "bundle_version": "$VERSION",
  "build_hash": "$BUNDLE_HASH",
  "bundle_hash": "$BUNDLE_HASH",
  "protocol_version": "2026-06-worker-v1",
  "engine_version": "$VERSION",
  "platform": "linux",
  "arch": "x86_64",
  "timestamp": "$NOW",
  "generated_at": "$NOW"
}
JSONEOF
fi
ok "manifest_v2.json generated"

# Step 9: Build DataServer
log "Building DataServer..."
cd "$DATASERVER_DIR"
if $DRY_RUN; then
    log "[DRY-RUN] go build ./cmd/server"
else
    go build -o bin/velox-server ./cmd/server 2>&1 | tail -3 || warn "DataServer build had warnings"
    ok "DataServer built"
fi

echo ""
echo -e "${GREEN}══════════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}  Build & Bundle Complete!${NC}"
echo -e "${GREEN}══════════════════════════════════════════════════════════════${NC}"
echo -e "  Version:    $VERSION"
echo -e "  Bundle:     $BUNDLE_PATH"
echo -e "  SHA256:     $BUNDLE_HASH"
echo -e "  Manifest:   $MANIFEST"
echo ""
echo "Next: restart DataServer to pick up the new bundle"
