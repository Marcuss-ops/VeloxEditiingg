#!/usr/bin/env bash
# ============================================================
# Velox — Definition of Done Validation Script
# ============================================================
# Automates PASS/FAIL checks for gates 1–9 and 13 of the DoD.
# Gates 10–12 (runtime state, restart, e2e) require worker access
# and must be verified manually or via a separate integration run.
#
# Usage:
#   ./scripts/dod-check.sh [--worker HOST] [--master URL]
#
# Exit code: 0 = all automated checks PASS, 1 = at least one FAIL
# ============================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

PASS=0
FAIL=0
SKIP=0
WARN=0

# Optional remote args
WORKER_HOST="${WORKER_HOST:-}"
MASTER_URL="${MASTER_URL:-http://127.0.0.1:8000}"

while [[ $# -gt 0 ]]; do
    case $1 in
        --worker) WORKER_HOST="$2"; shift 2 ;;
        --master) MASTER_URL="$2"; shift 2 ;;
        *) echo "Unknown: $1"; exit 1 ;;
    esac
done

pass() { PASS=$((PASS + 1)); echo -e "  ${GREEN}✅ PASS${NC}  $*"; }
fail() { FAIL=$((FAIL + 1)); echo -e "  ${RED}❌ FAIL${NC}  $*"; }
skip() { SKIP=$((SKIP + 1)); echo -e "  ${YELLOW}⏭  SKIP${NC}  $*"; }
warn() { WARN=$((WARN + 1)); echo -e "  ${YELLOW}⚠️  WARN${NC}  $*"; }
header() { echo -e "\n${BLUE}━━━ $* ━━━${NC}"; }

# ── Shared variables ─────────────────────────────────────────
AGENT_DIR="$REPO_ROOT/RemoteCodex/native/worker-agent-go"
BINARY="$AGENT_DIR/bin/velox-worker-agent"
DOCKERFILE="$REPO_ROOT/RemoteCodex/native/worker-agent-go/Dockerfile"
SYSTEMD_SETUP="$REPO_ROOT/DataServer/data/ansible/playbooks/tasks/systemd_setup.yml"
INSTALL_PLAYBOOK="$REPO_ROOT/DataServer/data/ansible/playbooks/install_workers.yml"
BUILD_SCRIPT="$REPO_ROOT/build_and_bundle.sh"
CI_FILE="$REPO_ROOT/.github/workflows/ci.yml"

# ============================================================
header "Gate 1 — Security"
# ============================================================

# 1a. inventory.ini not tracked by git
if git -C "$REPO_ROOT" ls-files --error-unmatch DataServer/data/ansible/playbooks/inventory.ini &>/dev/null; then
    fail "inventory.ini is still tracked by git"
else
    pass "inventory.ini not tracked by git"
fi

# 1b. No passwords in tracked config/inventory files (not source code)
HITS=$(git -C "$REPO_ROOT" grep -rncE '(ansible_ssh_pass|ansible_become_pass)' -- '*.ini' '*.yml' '*.yaml' '*.env' '*.cfg' '*.conf' '*.json' ':!*.example.*' 2>/dev/null | grep -v ':0$' || true)
if [[ -n "$HITS" ]]; then
    fail "Credential patterns found in config files:"
    echo "$HITS" | head -10
else
    pass "No credential patterns in config files"
fi

# 1c. inventory.example.ini exists
if [[ -f "$REPO_ROOT/DataServer/data/ansible/playbooks/inventory.example.ini" ]]; then
    pass "inventory.example.ini exists"
else
    fail "inventory.example.ini missing"
fi

# ============================================================
header "Gate 2 — Go build reproducible (clone → make agent → docker build)"
# ============================================================

# 2a. Binary can be built from clean state
rm -f "$BINARY"
if (cd "$AGENT_DIR" && make agent >/dev/null 2>&1); then
    if [[ -f "$BINARY" ]]; then
        pass "make agent produces binary from clean state"
    else
        fail "make agent ran but binary not found at $BINARY"
    fi
else
    fail "make agent failed"
fi

# 2b. Dockerfile is valid and references the required binary
if [[ -f "$DOCKERFILE" ]]; then
    if grep -q 'COPY native/worker-agent-go/bin/velox-worker-agent' "$DOCKERFILE"; then
        pass "Dockerfile references pre-built Go agent binary"
    else
        fail "Dockerfile does not COPY the Go agent binary"
    fi
    if [[ -f "$BINARY" ]]; then
        pass "Binary exists for Docker build context"
    else
        fail "Binary $BINARY missing — Docker build would fail"
    fi
else
    fail "Dockerfile missing at $DOCKERFILE"
fi

# 2c. Full Docker build (opt-in, slow)
if [[ "${DOD_FULL_DOCKER_BUILD:-0}" == "1" ]] && command -v docker &>/dev/null; then
    if docker build \
        -f "$DOCKERFILE" \
        -t velox-worker:dod-test \
        "$REPO_ROOT/RemoteCodex" >/dev/null 2>&1; then
        pass "Docker build succeeds (full build)"
    else
        fail "Docker build failed"
    fi
else
    skip "Full Docker build (set DOD_FULL_DOCKER_BUILD=1 to enable)"
fi

# ============================================================
header "Gate 3 — C++ deterministic build"
# ============================================================

# Check that build-video-engine.sh exists and does out-of-source build
if [[ -f "$REPO_ROOT/RemoteCodex/scripts/build-video-engine.sh" ]]; then
    if grep -q "BUILD_ROOT\|BUILD_DIR\|out-of-source" "$REPO_ROOT/RemoteCodex/scripts/build-video-engine.sh" 2>/dev/null; then
        pass "C++ build script uses out-of-source build"
    else
        warn "C++ build script exists but out-of-source pattern not confirmed"
    fi
else
    fail "build-video-engine.sh missing"
fi

# Check that the worker invokes the explicit full-video pipeline
if grep -q -- '--full-video' "$REPO_ROOT/RemoteCodex/native/worker-agent-go/pkg/video/native_engine.go" 2>/dev/null; then
    pass "Worker invokes the C++ engine with --full-video"
else
    fail "Worker does not pass --full-video to the C++ engine"
fi

# Check entrypoint validates GLIBC
if [[ -f "$REPO_ROOT/RemoteCodex/scripts/worker-entrypoint.sh" ]]; then
    if grep -q "GLIBC\|ldd\|not found" "$REPO_ROOT/RemoteCodex/scripts/worker-entrypoint.sh"; then
        pass "Entrypoint validates GLIBC and library dependencies"
    else
        fail "Entrypoint missing GLIBC validation"
    fi
else
    fail "worker-entrypoint.sh missing"
fi

# ============================================================
header "Gate 4 — Ansible deploy path independence"
# ============================================================

# Check rsync src uses playbook_dir
if grep -q 'playbook_dir' "$INSTALL_PLAYBOOK" 2>/dev/null; then
    pass "install_workers.yml uses playbook_dir-relative path"
else
    fail "install_workers.yml has hardcoded rsync source path"
fi

# Check no hardcoded /opt/velox/current/refactored/ as rsync src
if grep -E 'src:\s+/opt/velox' "$INSTALL_PLAYBOOK" &>/dev/null; then
    fail "Hardcoded /opt/velox path found as rsync source"
else
    pass "No hardcoded /opt/velox path as rsync source"
fi

# ============================================================
header "Gate 5 — Config generation without host binary"
# ============================================================

# Check config generation uses Docker, not host binary
if grep -q 'docker run.*velox-worker:latest.*-generate-config' "$SYSTEMD_SETUP" 2>/dev/null; then
    pass "Config generation uses Docker container"
else
    fail "Config generation may use host binary instead of Docker"
fi

# Check /usr/local/bin/velox-worker-agent is NOT called directly on host
# (it's OK inside docker run — that means it's called inside the container)
CONFIG_TASK=$(sed -n '/Ensure worker config exists/,/register: worker_config/p' "$SYSTEMD_SETUP" 2>/dev/null || true)
if echo "$CONFIG_TASK" | grep '/usr/local/bin/velox-worker-agent' | grep -qvE 'docker run|--entrypoint' 2>/dev/null; then
    fail "Config task directly calls /usr/local/bin/velox-worker-agent on host"
else
    pass "Config task does not call host-installed binary"
fi

# ============================================================
header "Gate 6 — Image always rebuilt"
# ============================================================

# Check systemd_setup.yml does NOT have "skip rebuild" logic
if grep -q 'skip rebuild\|already present.*exit 0' "$SYSTEMD_SETUP" 2>/dev/null; then
    fail "Docker build has skip-rebuild logic"
else
    pass "No skip-rebuild logic in Docker build task"
fi

# Check --pull is present for fresh base images
if grep -q '\-\-pull' "$SYSTEMD_SETUP" 2>/dev/null; then
    pass "Docker build uses --pull for fresh base images"
else
    warn "Docker build missing --pull flag"
fi

# ============================================================
header "Gate 7 — Healthcheck consistent"
# ============================================================

# Check HEALTHCHECK does NOT reference port 8000
if grep -q 'HEALTHCHECK' "$DOCKERFILE" 2>/dev/null; then
    if grep 'HEALTHCHECK' "$DOCKERFILE" | grep -q 'localhost:8081/health'; then
        pass "HEALTHCHECK targets the worker health server on 8081"
    elif grep 'HEALTHCHECK' "$DOCKERFILE" | grep -q 'localhost:8000'; then
        fail "HEALTHCHECK references port 8000 (Prometheus is disabled)"
    else
        warn "HEALTHCHECK present but endpoint not recognized"
    fi

    # Check it verifies something real
    if grep 'HEALTHCHECK' "$DOCKERFILE" | grep -q 'pgrep'; then
        pass "HEALTHCHECK uses process check (pgrep)"
    elif grep 'HEALTHCHECK' "$DOCKERFILE" | grep -q 'curl.*health'; then
        warn "HEALTHCHECK uses HTTP endpoint — verify it exists in the worker"
    fi
else
    warn "No HEALTHCHECK in Dockerfile"
fi

# Check the update flow rebuilds and reinjects the worker agent binary
if grep -q 'Build Go worker agent on controller' "$REPO_ROOT/DataServer/data/ansible/playbooks/update_workers.yml" \
    && grep -q 'Copy fresh worker agent binary into extracted bundle' "$REPO_ROOT/DataServer/data/ansible/playbooks/update_workers.yml"; then
    pass "Update playbook rebuilds and reinjects the worker agent binary"
else
    fail "Update playbook does not guarantee a fresh worker agent binary"
fi

# ============================================================
header "Gate 8 — Bundle hash auto-generated"
# ============================================================

if grep -q 'sha256sum\|sha256\|SOURCE_HASH' "$BUILD_SCRIPT" 2>/dev/null; then
    if grep -q 'BUNDLE_HASH.txt' "$BUILD_SCRIPT" 2>/dev/null; then
        pass "build_and_bundle.sh generates BUNDLE_HASH.txt automatically"
    else
        fail "build_and_bundle.sh computes hash but doesn't write BUNDLE_HASH.txt"
    fi
else
    fail "build_and_bundle.sh does not auto-generate bundle hash"
fi

# Check BUNDLE_HASH.txt exists
if [[ -f "$REPO_ROOT/RemoteCodex/BUNDLE_HASH.txt" ]]; then
    HASH_LEN=$(tr -d '[:space:]' < "$REPO_ROOT/RemoteCodex/BUNDLE_HASH.txt" | wc -c)
    if [[ "$HASH_LEN" -eq 64 ]]; then
        pass "BUNDLE_HASH.txt contains a valid 64-char SHA256"
    else
        warn "BUNDLE_HASH.txt has $HASH_LEN chars (expected 64)"
    fi
else
    fail "RemoteCodex/BUNDLE_HASH.txt missing"
fi

# ============================================================
header "Gate 9 — Prometheus explicitly configured"
# ============================================================

# Check prometheus_port is set to 0 in systemd_setup.yml config generation
if grep -q 'prometheus_port.*0' "$SYSTEMD_SETUP" 2>/dev/null; then
    pass "prometheus_port set to 0 in config generation"
else
    fail "prometheus_port not explicitly disabled"
fi

# ============================================================
header "Gate 13 — CI mandatory"
# ============================================================

if [[ -f "$CI_FILE" ]]; then
    pass "CI workflow exists at .github/workflows/ci.yml"

    # Check it runs Go tests
    if grep -q 'go test' "$CI_FILE"; then
        pass "CI runs Go tests"
    else
        fail "CI missing Go tests"
    fi

    # Check it builds Docker
    if grep -q 'docker\|buildx' "$CI_FILE"; then
        pass "CI includes Docker build"
    else
        fail "CI missing Docker build"
    fi

    # Check it runs ansible-lint
    if grep -q 'ansible-lint\|ansible.*lint\|syntax-check' "$CI_FILE"; then
        pass "CI includes Ansible lint"
    else
        fail "CI missing Ansible lint"
    fi
else
    fail "No CI workflow found"
fi

# ============================================================
header "Gates 10–12 — Runtime (requires --worker)"
# ============================================================

if [[ -n "$WORKER_HOST" ]]; then
    echo "  Runtime checks against $WORKER_HOST are not yet automated."
    echo "  Verify manually:"
    echo "    - systemctl is-active velox-worker-*.service"
    echo "    - docker ps --filter name=velox-worker"
    echo "    - 3 consecutive restarts"
    echo "    - 1 real video job COMPLETED"
else
    skip "Gates 10–12: pass --worker HOST to enable runtime checks"
fi

# ============================================================
header "Summary"
# ============================================================

echo ""
echo -e "  ${GREEN}PASS: $PASS${NC}  ${RED}FAIL: $FAIL${NC}  ${YELLOW}WARN: $WARN${NC}  ${YELLOW}SKIP: $SKIP${NC}"
echo ""

if [[ $FAIL -gt 0 ]]; then
    echo -e "${RED}DoD NOT MET — $FAIL gate(s) failed${NC}"
    exit 1
else
    echo -e "${GREEN}All automated gates PASS${NC}"
    exit 0
fi
