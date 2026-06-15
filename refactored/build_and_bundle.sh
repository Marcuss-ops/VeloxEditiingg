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

# Ensure output directory exists (self-healing after clean)
mkdir -p "$BUNDLE_DIR"

# Step 1: Tests
if ! $SKIP_TESTS; then
    log "Running tests..."
    cd "$DATASERVER_DIR"
    go test ./internal/workers ./internal/handlers/remote/workers ./internal/services/jobs ./internal/queue ./internal/store ./internal/handlers/server/jobs -count=1 -short 2>&1 | tail -20

    cd "$WORKER_DIR"
    go test ./pkg/api ./pkg/config ./internal/worker ./cmd/velox-worker-agent -count=1 -short 2>&1 | tail -20
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

# Step 4: Write BUILD_INFO.json (before zip so sha256 covers it)
log "Writing BUILD_INFO.json..."
GIT_COMMIT=$(cd "$SCRIPT_DIR" && git rev-parse --short HEAD 2>/dev/null || echo "unknown")
NOW=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
if ! $DRY_RUN; then
    SOURCE_HASH=$(cd "$SCRIPT_DIR" && find RemoteCodex \
        -type f \
        ! -path '*/bin/*' \
        ! -path '*/build/*' \
        ! -path '*/.git/*' \
        ! -name BUILD_INFO.json \
        -print0 |
        sort -z |
        xargs -0 sha256sum 2>/dev/null |
        sha256sum | awk '{print $1}')
    cat > "$SCRIPT_DIR/RemoteCodex/BUILD_INFO.json" << JSONEOF
{
  "version": "$VERSION",
  "git_commit": "$GIT_COMMIT",
  "source_hash": "$SOURCE_HASH",
  "protocol_version": "2026-06-worker-v1",
  "engine_version": "$VERSION",
  "platform": "linux",
  "arch": "x86_64",
  "built_at": "$NOW"
}
JSONEOF
    ok "BUILD_INFO.json written (source hash: ${SOURCE_HASH:0:16}...)"
else
    SOURCE_HASH="dry_run_source_hash"
    log "[DRY-RUN] Would write BUILD_INFO.json"
fi

# Step 5: Create bundle zip (atomic: .tmp → mv)
log "Creating bundle zip..."
BUNDLE_PATH="$BUNDLE_DIR/$BUNDLE_NAME"
TMP_BUNDLE="${BUNDLE_PATH}.tmp"

if $DRY_RUN; then
    log "[DRY-RUN] zip $BUNDLE_PATH"
else
    rm -f "$TMP_BUNDLE" "$BUNDLE_PATH"
    cd "$SCRIPT_DIR"
    zip -r "$TMP_BUNDLE" \
        RemoteCodex/ \
        VERSION.txt \
        -x "RemoteCodex/native/worker-agent-go/bin/*" \
        -x "RemoteCodex/native/video-engine-cpp/build/*" \
        -x "RemoteCodex/native/video-engine-cpp/CMakeCache.txt" \
        -x "RemoteCodex/native/video-engine-cpp/CMakeFiles/*" \
        -x "RemoteCodex/native/worker-agent-go/vendor/*" \
        -x "RemoteCodex/**/*.md" \
        2>&1 | tail -3
    ok "Bundle created (tmp): $TMP_BUNDLE"
fi

# Step 6: Calculate SHA256 and atomic publish
log "Calculating SHA256..."
if $DRY_RUN; then
    BUNDLE_HASH="dry_run_hash_placeholder"
else
    BUNDLE_HASH=$(sha256sum "$TMP_BUNDLE" | cut -d' ' -f1)
    echo -n "$BUNDLE_HASH  $BUNDLE_NAME" > "${TMP_BUNDLE}.sha256"
    # Atomic publish: only replace final files after everything is verified
    mv "$TMP_BUNDLE" "$BUNDLE_PATH"
    mv "${TMP_BUNDLE}.sha256" "${BUNDLE_PATH}.sha256"
fi
ok "SHA256: ${BUNDLE_HASH:0:16}... (sidecar: ${BUNDLE_NAME}.sha256)"

# Step 7: Write VERSION.txt
log "Writing VERSION.txt..."
if ! $DRY_RUN; then
    echo -n "$VERSION" > "$SCRIPT_DIR/VERSION.txt"
fi
ok "VERSION.txt: $VERSION"

# Step 8: Generate manifest_v2.json (master-side metadata)
log "Generating manifest_v2.json..."
MANIFEST="$BUNDLE_DIR/manifest_v2.json"

if $DRY_RUN; then
    log "[DRY-RUN] write manifest"
else
    cat > "$MANIFEST" << JSONEOF
{
  "version": "$VERSION",
  "git_commit": "$GIT_COMMIT",
  "bundle_hash": "$BUNDLE_HASH",
  "source_hash": "$SOURCE_HASH",
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
    go build -o bin/velox-server ./cmd/server 2>&1 | tail -3
    ok "DataServer built"
fi

# Atomic publication: write to .tmp first, then rename
log "Atomic publication..."
if ! $DRY_RUN; then
    TMP_BUNDLE="${BUNDLE_PATH}.tmp"
    
    # Validate bundle contains required files
    for file in \
        RemoteCodex/native/worker-agent-go/Dockerfile \
        RemoteCodex/native/video-engine-cpp/CMakeLists.txt \
        RemoteCodex/scripts/build-video-engine.sh \
        RemoteCodex/scripts/worker-entrypoint.sh \
        VERSION.txt
    do
        if ! unzip -Z1 "$BUNDLE_PATH" 2>/dev/null | grep -qx "$file"; then
            fail "Bundle missing: $file"
        fi
    done
    
    # Verify no legacy layout leaked
    if unzip -Z1 "$BUNDLE_PATH" 2>/dev/null | grep -q '^refactored/'; then
        fail "Bundle contains legacy refactored/ layout"
    fi
    ok "Bundle validation passed"
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
