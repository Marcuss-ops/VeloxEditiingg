#!/bin/bash
# ============================================================
# DoD Check — Definition of Done Verification (Unified)
# ============================================================
# Combines all gates from the former dod_check.sh (pre-deploy)
# and dod-check.sh (code review) into one unified script.
#
# Exit codes: 0 = all PASS, 1 = FAIL, 2 = WARN/SKIP present
# Usage: ./dod_check.sh [--worker HOST] [--verbose]
# ============================================================

set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/common.sh"

# ── Project-specific paths ──────────────────────────────────────
AGENT_DIR="$REPO_ROOT/RemoteCodex/native/worker-agent-go"
BINARY="$AGENT_DIR/bin/velox-worker-agent"
DOCKERFILE="$AGENT_DIR/Dockerfile"
PLAYBOOKS="$REPO_ROOT/DataServer/data/ansible/playbooks"
INVENTORY="$PLAYBOOKS/inventory.example.ini"
BUILD_SCRIPT="$REPO_ROOT/build_and_bundle.sh"
BUNDLE="$REPO_ROOT/DataServer/data/worker_downloads/worker_code_linux_x86_64.zip"
DATA_DIR="$REPO_ROOT/DataServer/data"
DB_PATH="$DATA_DIR/velox.db"

# ============================================================
header "Gate 1 — Repository & Security"
# ============================================================

# 1a. CI workflow exists
if [ ! -f "$REPO_ROOT/.github/workflows/ci.yml" ]; then
    fail "CI workflow not found at .github/workflows/ci.yml"
else
    ok "CI workflow present"
fi

# 1b. No legacy layout references in playbooks. The trailing `(/|$|")`
# anchor is required so that paths like `something_refactored.md` (a doc
# or filename that *mentions* the refactor) don't trip the gate — we only
# want to flag actual path components ending in `/refactored/`.
if grep -RnsE 'current/refactored(/|$|")|/app/refactored(/|$|")|/home/[a-z]+/(Pyt|Documents|Projects|work)/[^ "]*refactored(/|$|")' "$PLAYBOOKS" 2>/dev/null | grep -v '.yml.bak' | grep -v '# '; then
    fail "Legacy layout or absolute dev-machine path still present in playbooks"
else
    ok "No legacy layout or absolute-dev-path references in playbooks"
fi

# 1c. inventory.ini not tracked by git
if git -C "$REPO_ROOT" ls-files --error-unmatch DataServer/data/ansible/playbooks/inventory.ini &>/dev/null; then
    fail "inventory.ini is still tracked by git"
else
    ok "inventory.ini not tracked by git"
fi

# 1d. No passwords in tracked config/inventory files
HITS=$(git -C "$REPO_ROOT" grep -rncE '(ansible_ssh_pass|ansible_become_pass)' -- '*.ini' '*.yml' '*.yaml' '*.env' '*.cfg' '*.conf' '*.json' ':!*.example.*' 2>/dev/null | grep -v ':0$' || true)
if [[ -n "$HITS" ]]; then
    fail "Credential patterns found in config files:"
    echo "$HITS" | head -10
else
    ok "No credential patterns in config files"
fi

# 1e. inventory.example.ini exists
if [[ -f "$INVENTORY" ]]; then
    ok "inventory.example.ini exists"
else
    fail "inventory.example.ini missing"
fi

# 1f. Required source files exist
for f in \
    "$DOCKERFILE" \
    "$REPO_ROOT/RemoteCodex/scripts/build-video-engine.sh" \
    "$REPO_ROOT/RemoteCodex/scripts/worker-entrypoint.sh" \
    "$REPO_ROOT/RemoteCodex/native/video-engine-cpp/CMakeLists.txt"
do
    if [ -f "$f" ]; then
        ok "Found: $(basename "$f")"
    else
        fail "Missing: $f"
    fi
done

# ============================================================
header "Gate 2 — Go: build, binary & tests"
# ============================================================

# 2a. Binary can be built from clean state
rm -f "$BINARY"
if (cd "$AGENT_DIR" && make agent >/dev/null 2>&1); then
    if [[ -f "$BINARY" ]]; then
        ok "make agent produces binary from clean state"
    else
        fail "make agent ran but binary not found"
    fi
else
    fail "make agent failed"
fi

# 2b. Dockerfile references the Go binary
if [[ -f "$DOCKERFILE" ]]; then
    if grep -q 'COPY native/worker-agent-go/bin/velox-worker-agent' "$DOCKERFILE"; then
        ok "Dockerfile references pre-built Go agent binary"
    else
        fail "Dockerfile does not COPY the Go agent binary"
    fi
    if [[ -f "$BINARY" ]]; then
        ok "Binary exists for Docker build context"
    else
        fail "Binary missing — Docker build would fail"
    fi
else
    fail "Dockerfile missing"
fi

# 2c. DataServer tests pass
if (cd "$REPO_ROOT/DataServer" && go test ./... -count=1 -short >/dev/null 2>&1); then
    ok "DataServer tests pass"
else
    fail "DataServer tests failed"
fi

# 2d. Worker agent tests pass
if (cd "$AGENT_DIR" && go test ./... -count=1 -short >/dev/null 2>&1); then
    ok "Worker agent tests pass"
else
    fail "Worker agent tests failed"
fi

# ============================================================
header "Gate 3 — Ansible: syntax, deploy & config"
# ============================================================

# 3a. Syntax check all playbooks
for pb in install_workers.yml update_workers.yml normalize_worker_systemd.yml \
          restart_workers.yml preflight_workers.yml; do
    if ansible-playbook --syntax-check -i "$INVENTORY" "$PLAYBOOKS/$pb" >/dev/null 2>&1; then
        ok "Syntax: $pb"
    else
        fail "Syntax error: $pb"
    fi
done

# 3b. Deploy path independence
INSTALL_PLAYBOOK="$PLAYBOOKS/install_workers.yml"
if grep -q 'playbook_dir' "$INSTALL_PLAYBOOK" 2>/dev/null; then
    ok "install_workers.yml uses playbook_dir-relative path"
else
    fail "install_workers.yml has hardcoded rsync source path"
fi
if grep -E 'src:\s+/opt/velox' "$INSTALL_PLAYBOOK" &>/dev/null; then
    fail "Hardcoded /opt/velox path found as rsync source"
else
    ok "No hardcoded /opt/velox path as rsync source"
fi

# 3c. Config generation uses Docker, not host binary
SYSTEMD_SETUP="$PLAYBOOKS/tasks/systemd_setup.yml"
if grep -q 'docker run.*velox-worker:latest.*-generate-config' "$SYSTEMD_SETUP" 2>/dev/null; then
    ok "Config generation uses Docker container"
else
    fail "Config generation may use host binary instead of Docker"
fi

# 3d. Worker agent not called directly on host
CONFIG_TASK=$(sed -n '/Ensure worker config exists/,/register: worker_config/p' "$SYSTEMD_SETUP" 2>/dev/null || true)
if echo "$CONFIG_TASK" | grep '/usr/local/bin/velox-worker-agent' | grep -qvE 'docker run|--entrypoint' 2>/dev/null; then
    fail "Config task directly calls /usr/local/bin/velox-worker-agent on host"
else
    ok "Config task does not call host-installed binary"
fi

# ============================================================
header "Gate 4 — Docker: build, healthcheck & rebuild"
# ============================================================

# 4a. Docker build + binary verification
if command -v docker >/dev/null 2>&1; then
    if [[ "${DOD_FULL_DOCKER_BUILD:-0}" == "1" ]]; then
        if docker build --pull --no-cache -f "$DOCKERFILE" -t velox-worker:dod "$REPO_ROOT/RemoteCodex" >/dev/null 2>&1; then
            ok "Docker image built (full)"
            if docker run --rm --entrypoint /bin/bash velox-worker:dod -lc '
                set -Eeuo pipefail
                test -x /usr/local/bin/velox-worker-agent
                test -x /usr/local/bin/velox_video_engine
                test -x /usr/local/bin/worker-entrypoint.sh
                test -x /usr/local/bin/build-video-engine.sh
                ! ldd /usr/local/bin/velox_video_engine 2>/dev/null | grep -q "not found"
            ' >/dev/null 2>&1; then
                ok "Docker binaries verified (agent, engine, scripts, no missing libs)"
            else
                fail "Docker binary check failed — missing binary or unresolved library"
            fi
        else
            fail "Docker build failed"
        fi
    else
        if [[ -f "$BINARY" ]]; then
            ok "Binary exists (set DOD_FULL_DOCKER_BUILD=1 for full build test)"
        fi
        skip "Full Docker build (set DOD_FULL_DOCKER_BUILD=1 to enable)"
    fi
else
    skip "Docker not available"
fi

# 4b. Healthcheck configuration
if [[ -f "$DOCKERFILE" ]] && grep -q 'HEALTHCHECK' "$DOCKERFILE" 2>/dev/null; then
    if grep 'HEALTHCHECK' "$DOCKERFILE" | grep -q 'localhost:8081/health'; then
        ok "HEALTHCHECK targets worker health server on 8081"
    elif grep 'HEALTHCHECK' "$DOCKERFILE" | grep -q 'localhost:8000'; then
        fail "HEALTHCHECK references port 8000 (Prometheus is disabled)"
    else
        warn "HEALTHCHECK present but endpoint not recognized"
    fi
    if grep 'HEALTHCHECK' "$DOCKERFILE" | grep -q 'pgrep'; then
        ok "HEALTHCHECK uses process check (pgrep)"
    fi
else
    warn "No HEALTHCHECK in Dockerfile"
fi

# 4c. Image always rebuilt (no skip-rebuild logic)
if grep -q 'skip rebuild\|already present.*exit 0' "$SYSTEMD_SETUP" 2>/dev/null; then
    fail "Docker build has skip-rebuild logic"
else
    ok "No skip-rebuild logic in Docker build task"
fi

# 4d. --pull flag for fresh base images
if grep -q '\-\-pull' "$SYSTEMD_SETUP" 2>/dev/null; then
    ok "Docker build uses --pull for fresh base images"
else
    warn "Docker build missing --pull flag"
fi

# 4e. Update flow rebuilds and reinjects worker agent
if grep -q 'Build Go worker agent on controller' "$PLAYBOOKS/update_workers.yml" \
    && grep -q 'Copy fresh worker agent binary into extracted bundle' "$PLAYBOOKS/update_workers.yml" 2>/dev/null; then
    ok "Update playbook rebuilds and reinjects binary"
else
    fail "Update playbook does not guarantee fresh binary"
fi

# ============================================================
header "Gate 5 — C++ engine: build & smoke render"
# ============================================================

# 5a. Build script uses out-of-source build
if [[ -f "$REPO_ROOT/RemoteCodex/scripts/build-video-engine.sh" ]]; then
    if grep -q "BUILD_ROOT\|BUILD_DIR\|out-of-source" "$REPO_ROOT/RemoteCodex/scripts/build-video-engine.sh" 2>/dev/null; then
        ok "C++ build script uses out-of-source build"
    else
        warn "C++ build script exists but out-of-source pattern not confirmed"
    fi
else
    fail "build-video-engine.sh missing"
fi

# 5b. Worker invokes C++ engine with --full-video
if grep -q -- '--full-video' "$AGENT_DIR/pkg/video/native_engine.go" 2>/dev/null; then
    ok "Worker invokes C++ engine with --full-video"
else
    fail "Worker does not pass --full-video to C++ engine"
fi

# 5c. Entrypoint validates GLIBC
if [[ -f "$REPO_ROOT/RemoteCodex/scripts/worker-entrypoint.sh" ]]; then
    if grep -q "GLIBC\|ldd\|not found" "$REPO_ROOT/RemoteCodex/scripts/worker-entrypoint.sh"; then
        ok "Entrypoint validates GLIBC and library dependencies"
    else
        fail "Entrypoint missing GLIBC validation"
    fi
fi

# 5d. C++ smoke render (requires Docker image from Gate 4)
if command -v docker >/dev/null 2>&1 && docker image inspect velox-worker:dod >/dev/null 2>&1; then
    if docker run --rm --entrypoint /bin/bash velox-worker:dod -lc '
        set -Eeuo pipefail
        mkdir -p /tmp/dod-smoke
        ffmpeg -hide_banner -loglevel error \
            -f lavfi -i color=c=black:s=1280x720 -frames:v 1 /tmp/dod-smoke/frame.jpg
        /usr/local/bin/velox_video_engine --build-scene-segment \
            --image /tmp/dod-smoke/frame.jpg --duration 1 --out /tmp/dod-smoke/output.mp4
        test -s /tmp/dod-smoke/output.mp4
    ' >/dev/null 2>&1; then
        ok "C++ smoke render passed"
    else
        fail "C++ smoke render failed"
    fi
else
    skip "Docker image not available for C++ smoke test"
fi

# ============================================================
header "Gate 6 — Bundle: contents & hash"
# ============================================================

# 6a. Bundle exists
if [ -s "$BUNDLE" ]; then
    ok "Bundle exists: $(basename "$BUNDLE")"
else
    fail "Bundle missing or empty"
fi

# 6b. Bundle contents
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

    # 6c. No legacy refactored/ layout in bundle
    if unzip -Z1 "$BUNDLE" 2>/dev/null | grep -q '^refactored/'; then
        fail "Bundle contains legacy refactored/ layout"
    else
        ok "No legacy layout in bundle"
    fi
fi

# 6d. Build script auto-generates bundle hash
if grep -q 'sha256sum\|sha256\|SOURCE_HASH' "$BUILD_SCRIPT" 2>/dev/null; then
    if grep -q 'BUNDLE_HASH.txt' "$BUILD_SCRIPT" 2>/dev/null; then
        ok "build_and_bundle.sh generates BUNDLE_HASH.txt automatically"
    else
        fail "build_and_bundle.sh computes hash but does not write BUNDLE_HASH.txt"
    fi
else
    fail "build_and_bundle.sh does not auto-generate bundle hash"
fi

# 6e. BUNDLE_HASH.txt exists and is valid
if [[ -f "$REPO_ROOT/RemoteCodex/BUNDLE_HASH.txt" ]]; then
    HASH_LEN=$(tr -d '[:space:]' < "$REPO_ROOT/RemoteCodex/BUNDLE_HASH.txt" | wc -c)
    if [[ "$HASH_LEN" -eq 64 ]]; then
        ok "BUNDLE_HASH.txt contains a valid 64-char SHA256"
    else
        warn "BUNDLE_HASH.txt has $HASH_LEN chars (expected 64)"
    fi
else
    fail "RemoteCodex/BUNDLE_HASH.txt missing"
fi

# ============================================================
header "Gate 7 — Prometheus & CI checks"
# ============================================================

# 8a. Prometheus explicitly configured (port set to 0)
if grep -q 'prometheus_port.*0' "$SYSTEMD_SETUP" 2>/dev/null; then
    ok "prometheus_port set to 0 in config generation"
else
    fail "prometheus_port not explicitly disabled"
fi

# 8b. CI workflow checks (if exists)
if [[ -f "$REPO_ROOT/.github/workflows/ci.yml" ]]; then
    CI_FILE="$REPO_ROOT/.github/workflows/ci.yml"
    if grep -q 'go test' "$CI_FILE"; then ok "CI runs Go tests"; else fail "CI missing Go tests"; fi
    if grep -q 'docker\|buildx' "$CI_FILE"; then ok "CI includes Docker build"; else fail "CI missing Docker build"; fi
    if grep -q 'ansible-lint\|ansible.*lint\|syntax-check' "$CI_FILE"; then ok "CI includes Ansible lint"; else fail "CI missing Ansible lint"; fi
fi

# ============================================================
header "Gate 8-11 — Runtime checks (requires --worker)"
# ============================================================

if [ -n "$WORKER" ]; then
    $VERBOSE && log "  Runtime checks against $WORKER"

    # 9. Service active
    if ssh "$WORKER" "systemctl is-active --quiet velox-worker-${WORKER}" 2>/dev/null; then
        ok "Service velox-worker-${WORKER} active"
    else
        fail "Service velox-worker-${WORKER} not active"
    fi

    # 10. Container running
    CONTAINER="velox-worker-${WORKER}"
    if ssh "$WORKER" "docker inspect $CONTAINER >/dev/null 2>&1"; then
        ok "Container $CONTAINER running"
    else
        fail "Container $CONTAINER not running"
    fi

    # 11a. C++ engine present in container
    if ssh "$WORKER" "docker exec $CONTAINER test -x /usr/local/bin/velox_video_engine" 2>/dev/null; then
        ok "C++ engine present in container"
    else
        fail "C++ engine missing in container"
    fi

    # 11b. Health endpoint responding
    if ssh "$WORKER" "docker exec $CONTAINER curl -sf http://127.0.0.1:8081/health >/dev/null 2>&1"; then
        ok "Health endpoint responding on 8081"
    else
        fail "Health endpoint not responding"
    fi

    # 12. Three consecutive restarts
    log "  Gate 12: Three consecutive restarts on $WORKER"
    ENGINE_HASH_BEFORE=$(ssh "$WORKER" "docker exec $CONTAINER sha256sum /usr/local/bin/velox_video_engine | awk '{print \$1}'" 2>/dev/null || echo "")

    RESTART_OK=true
    for i in 1 2 3; do
        ssh "$WORKER" "sudo systemctl restart velox-worker-${WORKER}" 2>/dev/null || { RESTART_OK=false; break; }
        sleep 5
        if ! ssh "$WORKER" "systemctl is-active --quiet velox-worker-${WORKER} && \
            docker exec $CONTAINER curl -sf http://127.0.0.1:8081/health >/dev/null 2>&1" 2>/dev/null; then
            RESTART_OK=false
            break
        fi
    done

    if $RESTART_OK; then
        ok "3 consecutive restarts succeeded"
    else
        fail "Restart test failed"
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
    skip "Gates 9-12: runtime checks (use --worker <name>)"
fi

# ============================================================
summary
# ============================================================
