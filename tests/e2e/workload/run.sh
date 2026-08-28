#!/usr/bin/env bash
# =============================================================================
# tests/e2e/workload/run.sh — PR 5 real workload E2E
# =============================================================================
# Runs the full Hello → artifact → SUCCEEDED pipeline with deterministic
# fixtures, strict media/SHA checks, metrics checks, worker visibility, and
# blocking database finalization assertions.
#
# Usage:
#   make e2e-workload
#   E2E_WORKDIR=/tmp/vx-wl make e2e-workload
#   bash tests/e2e/workload/run.sh
# =============================================================================

set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$ROOT/../../.." && pwd)"
WORKDIR="${E2E_WORKDIR:-/tmp/velox-e2e-workload}"

# ─── Paths ───────────────────────────────────────────────────────────────────
BIN_DIR="$WORKDIR/bin"
DATA_DIR="$WORKDIR/data"
STAGING_DIR="$DATA_DIR/staging"
FIXTURE_DIR="$DATA_DIR/fixtures"
STORAGE_DIR="$WORKDIR/storage"
LOG_DIR="$WORKDIR/logs"

MASTER_BIN="$BIN_DIR/velox-server"
WORKER_BIN="$BIN_DIR/velox-worker-agent"
MASTER_LOG="$LOG_DIR/master.log"
WORKER_LOG="$LOG_DIR/worker.log"
MASTER_ENV="$WORKDIR/master.env"
WORKER_CFG="$WORKDIR/worker.json"
MASTER_PIDFILE="$WORKDIR/master.pid"
WORKER_PIDFILE="$WORKDIR/worker.pid"

MASTER_PORT="${E2E_MASTER_PORT:-8080}"
GRPC_PORT="${E2E_GRPC_PORT:-50051}"
WORKER_HEALTH_PORT="${E2E_WORKER_HEALTH_PORT:-0}"
WORKER_PROMETHEUS_PORT="${E2E_WORKER_PROMETHEUS_PORT:-0}"
ADMIN_TOKEN="e2e-workload-token"
WORKER_ID="e2e-workload-worker-1"
DESTINATION_ID="e2e-local"

VERSION="$(tr -d '[:space:]' < "$REPO_ROOT/VERSION.txt" 2>/dev/null || echo "dev")"

# ─── Colors ─────────────────────────────────────────────────────────────────
C_GREEN='\033[32m'; C_RED='\033[31m'; C_CYAN='\033[36m'; C_RST='\033[0m'
pass() { printf "${C_GREEN}PASS${C_RST}  %s\n" "$*"; }
fail() { printf "${C_RED}FAIL${C_RST}  %s\n" "$*"; return 1; }
info() { printf "${C_CYAN}.. %s${C_RST}\n" "$*"; }

# ─── Source shared utilities and phase scripts ──────────────────────────────
# shellcheck disable=SC1090
for helper in \
  "$ROOT/lib/process.sh" \
  "$ROOT/lib/build.sh" \
  "$ROOT/lib/fixtures.sh" \
  "$ROOT/lib/master.sh" \
  "$ROOT/lib/worker.sh" \
  "$ROOT/lib/submit.sh" \
  "$ROOT/lib/worker_wait.sh" \
  "$ROOT/lib/verify.sh" \
  "$ROOT/lib/artifact_assertions.sh" \
  "$ROOT/lib/video_assertions.sh" \
  "$ROOT/lib/metrics_assertions.sh"; do
  source "$helper"
done

# ─── Initialize process management ──────────────────────────────────────────
resolve_ports
setup_traps

main() {
  echo ""
  echo "══════════════════════════════════════════════════════════════"
  echo "  PR 5 — Velox Real Workload E2E"
  echo "══════════════════════════════════════════════════════════════"
  echo ""
  info "workdir  = $WORKDIR"
  info "version  = $VERSION"
  info "binaries = $MASTER_BIN / $WORKER_BIN"
  echo ""

  # Phase 0 — pre-flight dependency checks.
  local missing=0
  for dep in go ffmpeg sqlite3 python3; do
    command -v "$dep" >/dev/null 2>&1 || {
      fail "$dep not found — install before running make e2e-workload"; missing=1; }
  done
  (( missing == 0 )) || exit 3

  phase_build
  phase_fixtures
  phase_master_start
  phase_submit
  phase_worker_start
  phase_live_milestones
  phase_poll_and_verify

  echo ""
  echo "══════════════════════════════════════════════════════════════"
  pass "ALL VERIFICATIONS PASSED — Velox E2E workload complete"
  echo "══════════════════════════════════════════════════════════════"
  echo ""
}

main "$@"
