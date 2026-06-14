#!/bin/bash
# ============================================================
# DoD Check — Definition of Done verification
# ============================================================
# Exit codes: 0 = all PASS, 1 = FAIL, 2 = WARN/SKIP present
# Usage: ./dod_check.sh [--worker pi1] [--verbose]
# ============================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GIT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
APP="$GIT_ROOT/refactored"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; NC='\033[0m'

FAIL=0
WARN=0
SKIP=0
PASS=0
VERBOSE=false
WORKER=""

log()  { echo -e "${BLUE}[DoD]${NC} $*"; }
ok()   { echo -e "${GREEN}[PASS]${NC} $*"; PASS=$((PASS + 1)); }
warn() { echo -e "${YELLOW}[WARN]${NC} $*"; WARN=$((WARN + 1)); }
fail() { echo -e "${RED}[FAIL]${NC} $*"; FAIL=$((FAIL + 1)); }
skip() { echo -e "${YELLOW}[SKIP]${NC} $*"; SKIP=$((SKIP + 1)); }

while [[ $# -gt 0 ]]; do
    case $1 in
        --worker)  WORKER="$2"; shift 2 ;;
        --verbose) VERBOSE=true; shift ;;
        *)         echo "Unknown: $1"; exit 1 ;;
    esac
done

# ── Gate 1: Repository checks ──────────────────────────────────
log "Gate 1: Repository checks"

if [ ! -f "$APP/.github/workflows/ci.yml" ] && [ ! -f "$GIT_ROOT/.github/workflows/ci.yml" ]; then
    fail "CI workflow not found"
else
    ok "CI workflow present"
fi

if grep -RnsE 'current/refactored|/app/refactored' "$APP/DataServer/data/ansible/playbooks" 2>/dev/null | grep -v '.yml.bak' | grep -v '# '; then
    fail "Legacy layout still present in playbooks"
else
    ok "No legacy layout references"
fi

for f in \
    "$APP/RemoteCodex/native/worker-agent-go/Dockerfile" \
    "$APP/RemoteCodex/scripts/build-video-engine.sh" \
    "$APP/RemoteCodex/scripts/worker-entrypoint.sh" \
    "$APP/RemoteCodex/native/video-engine-cpp/CMakeLists.txt"
do
    if [ -f "$f" ]; then
        ok "Found: $(basename "$f")"
    else
        fail "Missing: $f"
    fi
done

# ── Gate 2: Go tests ───────────────────────────────────────────
log "Gate 2: Go tests"

if cd "$APP/DataServer" && go test ./... -count=1 -short >/dev/null 2>&1; then
    ok "DataServer tests pass"
else
    fail "DataServer tests failed"
fi

if cd "$APP/RemoteCodex/native/worker-agent-go" && go test ./... -count=1 -short >/dev/null 2>&1; then
    ok "Worker agent tests pass"
else
    fail "Worker agent tests failed"
fi

if [ -f "$APP/RemoteCodex/native/worker-agent-go/bin/velox-worker-agent" ]; then
    ok "Worker agent binary exists"
else
    fail "Worker agent binary not built"
fi

# ── Gate 3: Ansible syntax ─────────────────────────────────────
log "Gate 3: Ansible syntax"

PLAYBOOKS="$APP/DataServer/data/ansible/playbooks"
INVENTORY="$PLAYBOOKS/inventory.example.ini"

for pb in install_workers.yml update_workers.yml normalize_worker_systemd.yml restart_workers.yml preflight_workers.yml; do
    if ansible-playbook --syntax-check -i "$INVENTORY" "$PLAYBOOKS/$pb" >/dev/null 2>&1; then
        ok "Syntax: $pb"
    else
        fail "Syntax error: $pb"
    fi
done

# ── Gate 4: Bundle verification ────────────────────────────────
log "Gate 4: Bundle verification"

BUNDLE="$APP/DataServer/data/worker_downloads/worker_code_linux_x86_64.zip"

if [ -s "$BUNDLE" ]; then
    ok "Bundle exists: $BUNDLE"
else
    fail "Bundle missing or empty: $BUNDLE"
fi

if [ -s "$BUNDLE" ]; then
    for file in \
        RemoteCodex/native/worker-agent-go/Dockerfile \
        RemoteCodex/native/video-engine-cpp/CMakeLists.txt \
        RemoteCodex/scripts/build-video-engine.sh \
        RemoteCodex/scripts/worker-entrypoint.sh
    do
        if unzip -Z1 "$BUNDLE" 2>/dev/null | grep -qx "$file"; then
            ok "Bundle contains: $file"
        else
            fail "Bundle missing: $file"
        fi
    done

    if unzip -Z1 "$BUNDLE" 2>/dev/null | grep -q '^refactored/'; then
        fail "Bundle contains legacy refactored/ layout"
    else
        ok "No legacy layout in bundle"
    fi
fi

# ── Gate 5: Docker build ───────────────────────────────────────
log "Gate 5: Docker build"

if command -v docker >/dev/null 2>&1; then
    cd "$APP/RemoteCodex"
    if docker build --pull --no-cache -f native/worker-agent-go/Dockerfile -t velox-worker:dod . >/dev/null 2>&1; then
        ok "Docker image built"

        if docker run --rm --entrypoint /bin/bash velox-worker:dod -lc '
            set -Eeuo pipefail
            test -x /usr/local/bin/velox-worker-agent
            test -x /usr/local/bin/velox_video_engine
            test -x /usr/local/bin/worker-entrypoint.sh
            test -x /usr/local/bin/build-video-engine.sh
            ! ldd /usr/local/bin/velox_video_engine | grep -q "not found"
        ' >/dev/null 2>&1; then
            ok "Docker binaries verified"
        else
            fail "Docker binary check failed"
        fi
    else
        fail "Docker build failed"
    fi
else
    skip "Docker not available"
fi

# ── Gate 6: C++ smoke render ──────────────────────────────────
log "Gate 6: C++ smoke render"

if command -v docker >/dev/null 2>&1 && docker image inspect velox-worker:dod >/dev/null 2>&1; then
    if docker run --rm --entrypoint /bin/bash velox-worker:dod -lc '
        set -Eeuo pipefail
        mkdir -p /tmp/dod-smoke
        ffmpeg -hide_banner -loglevel error \
            -f lavfi -i color=c=black:s=1280x720 \
            -frames:v 1 /tmp/dod-smoke/frame.jpg
        /usr/local/bin/velox_video_engine \
            --build-scene-segment \
            --image /tmp/dod-smoke/frame.jpg \
            --duration 1 \
            --out /tmp/dod-smoke/output.mp4
        test -s /tmp/dod-smoke/output.mp4
    ' >/dev/null 2>&1; then
        ok "C++ smoke render passed"
    else
        fail "C++ smoke render failed"
    fi
else
    skip "Docker image not available for smoke test"
fi

# ── Gate 7-10: Runtime checks (require --worker) ──────────────
if [ -n "$WORKER" ]; then
    log "Gate 7: Install verification on $WORKER"

    SERVICE="velox-worker-${WORKER}"
    CONTAINER="velox-worker-${WORKER}"

    # Check service is active
    if ssh "$WORKER" "systemctl is-active --quiet $SERVICE" 2>/dev/null; then
        ok "Service $SERVICE active on $WORKER"
    else
        fail "Service $SERVICE not active on $WORKER"
    fi

    # Check Docker container
    if ssh "$WORKER" "docker inspect $CONTAINER >/dev/null 2>&1"; then
        ok "Container $CONTAINER running on $WORKER"
    else
        fail "Container $CONTAINER not running on $WORKER"
    fi

    # Check C++ engine in container
    if ssh "$WORKER" "docker exec $CONTAINER test -x /usr/local/bin/velox_video_engine" 2>/dev/null; then
        ok "C++ engine present in container on $WORKER"
    else
        fail "C++ engine missing in container on $WORKER"
    fi

    # Check health endpoint
    if ssh "$WORKER" "docker exec $CONTAINER curl -sf http://127.0.0.1:8081/health >/dev/null 2>&1"; then
        ok "Health endpoint responding on $WORKER"
    else
        fail "Health endpoint not responding on $WORKER"
    fi

    # 3 consecutive restarts
    log "Gate 8: Three consecutive restarts on $WORKER"
    ENGINE_HASH_BEFORE=$(ssh "$WORKER" "docker exec $CONTAINER sha256sum /usr/local/bin/velox_video_engine | awk '{print \$1}'" 2>/dev/null || echo "")

    RESTART_OK=true
    for i in 1 2 3; do
        ssh "$WORKER" "sudo systemctl restart $SERVICE" 2>/dev/null || { RESTART_OK=false; break; }
        sleep 5
        if ! ssh "$WORKER" "systemctl is-active --quiet $SERVICE && docker exec $CONTAINER curl -sf http://127.0.0.1:8081/health >/dev/null 2>&1" 2>/dev/null; then
            RESTART_OK=false
            break
        fi
    done

    if $RESTART_OK; then
        ok "3 consecutive restarts on $WORKER"
    else
        fail "Restart test failed on $WORKER"
    fi

    ENGINE_HASH_AFTER=$(ssh "$WORKER" "docker exec $CONTAINER sha256sum /usr/local/bin/velox_video_engine | awk '{print \$1}'" 2>/dev/null || echo "")
    if [ -n "$ENGINE_HASH_BEFORE" ] && [ "$ENGINE_HASH_BEFORE" = "$ENGINE_HASH_AFTER" ]; then
        ok "Engine binary unchanged after restarts"
    elif [ -z "$ENGINE_HASH_BEFORE" ]; then
        warn "Could not verify engine hash"
    else
        fail "Engine binary changed after restarts"
    fi
else
    skip "Gate 7-10: runtime checks (use --worker <name>)"
fi

# ── Summary ─────────────────────────────────────────────────────
echo ""
echo -e "══════════════════════════════════════════════════════════════"
echo -e "  DoD Check Summary"
echo -e "══════════════════════════════════════════════════════════════"
echo -e "  ${GREEN}PASS: $PASS${NC}"
echo -e "  ${RED}FAIL: $FAIL${NC}"
echo -e "  ${YELLOW}WARN: $WARN${NC}"
echo -e "  ${YELLOW}SKIP: $SKIP${NC}"
echo -e "══════════════════════════════════════════════════════════════"

if [ $FAIL -gt 0 ]; then
    echo -e "${RED}RESULT: FAIL${NC}"
    exit 1
elif [ $WARN -gt 0 ] || [ $SKIP -gt 0 ]; then
    echo -e "${YELLOW}RESULT: INCOMPLETE (WARN=$WARN, SKIP=$SKIP)${NC}"
    exit 2
else
    echo -e "${GREEN}RESULT: PASS${NC}"
    exit 0
fi
