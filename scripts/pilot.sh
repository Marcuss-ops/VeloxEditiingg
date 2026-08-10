#!/usr/bin/env bash
# scripts/pilot.sh
#
# Velox pilot launcher — one-command reproduce of the 4-of-5 links pipeline
# on any sandbox. Encapsulates the dev-bypass environment variables
# (VELOX_GRPC_ALLOW_INSECURE_DEV / VELOX_ALLOW_INSECURE_GRPC_DEV /
# VELOX_ASSET_REWRITE_DEV_BYPASS) so the next operator does not inherit
# 5 rounds of iterative patch history.
#
# Usage:
#   ./scripts/pilot.sh [command]
#
# Commands:
#   all           Full pipeline: build → start → submit → work → poll (default)
#   build         Build master + worker + engine binaries
#   start         Start master (with all dev bypasses)
#   submit        Generate test fixtures + submit an images.v1 job
#   work          Start worker (with all dev bypasses)
#   stop          Stop master + worker processes
#   status        Show running processes + master health
#   log           Tail master log
#
# Environment:
#   PILOT_DIR     Working directory (default: /tmp/velox-pilot)
#   SKIP_BUILD    If set, skip building binaries (reuse existing)
#   SKIP_CLEANUP  If set, do not stop processes on exit
#
# Exit codes:
#   0   Success
#   1   General failure
#   2   Build failure
#   3   Environment/deps missing
#   126 Timeout
#
# ─── WARNING: Dev bypasses ────────────────────────────────────────────────────
# This script sets THREE dev-bypass environment variables that are
# PRODUCTION-UNSAFE. They exist so the pilot can run end-to-end without
# mTLS certs or production asset wiring on a throwaway sandbox:
#
#   VELOX_GRPC_ALLOW_INSECURE_DEV=true   (master side: plaintext gRPC)
#   VELOX_ALLOW_INSECURE_GRPC_DEV=true   (worker side: plaintext gRPC)
#   VELOX_ASSET_REWRITE_DEV_BYPASS=true  (master: allow any file:// path)
#
# NEVER use this script against a production database or a reachable
# network. These env vars are deliberately separate from the production
# deployment paths (mTLS, VELOX_GRPC_TLS_*, allowedRoots) so they
# cannot be set by accident in production configs.
# ──────────────────────────────────────────────────────────────────────────────

set -euo pipefail

# ─── Repo root (always works regardless of CWD) ──────────────────────────────
# shellcheck disable=SC2155,SC2034
readonly REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck disable=SC2155,SC2034
readonly SCRIPT_NAME="$(basename "$0")"

# ─── Configuration ───────────────────────────────────────────────────────────
PILOT_DIR="${PILOT_DIR:-/tmp/velox-pilot-$(date +%s)-$$}"
readonly LOGDIR="${PILOT_DIR}/logs"
readonly PID_DIR="${PILOT_DIR}"
readonly DATA_DIR="${PILOT_DIR}/data"
readonly STAGING_DIR="${PILOT_DIR}/staging"
readonly STORAGE_DIR="${PILOT_DIR}/storage"

pick_free_port() {
  python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()'
}

readonly MASTER_PORT="${PILOT_MASTER_PORT:-$(pick_free_port)}"
readonly GRPC_PORT="${PILOT_GRPC_PORT:-$(pick_free_port)}"
readonly WORKER_HEALTH_PORT="${PILOT_WORKER_HEALTH_PORT:-$(pick_free_port)}"
readonly ADMIN_TOKEN="test-admin-token"
readonly WORKER_ID="pilot-worker-1"
readonly DESTINATION_ID="e2e-local"

# Binaries (built from repo)
readonly MASTER_BIN="${PILOT_DIR}/bin/velox-server"
readonly WORKER_BIN="${PILOT_DIR}/bin/velox-worker-agent"
readonly ENGINE_BIN="${PILOT_DIR}/bin/velox_video_engine"

# Paths
readonly MASTER_LOG="${LOGDIR}/master.log"
readonly WORKER_LOG="${LOGDIR}/worker.log"
readonly MASTER_PIDFILE="${PID_DIR}/master.pid"
readonly WORKER_PIDFILE="${PID_DIR}/worker.pid"
readonly MASTER_ENV="${PID_DIR}/master.env"
readonly WORKER_CONFIG="${PID_DIR}/worker.json"
readonly JOB_FILE="${PID_DIR}/job.json"

# Version from canonical source
VERSION="$(tr -d '[:space:]' < "${REPO_ROOT}/VERSION.txt" 2>/dev/null || echo "dev")"

# ─── Dev bypasses (pilot-only; see WARNING above) ────────────────────────────
# Scoped exports: only the cmd_* functions that need them set the bypass
# variables, NOT script top. Prevents `./scripts/pilot.sh status` or
# `./scripts/pilot.sh stop` from leaking plaintext-gRPC + allow-any-path
# env vars into the calling shell on invocation.
# Worker-side bypass is set explicitly in cmd_work() via env prefix
# (VELOX_ALLOW_INSECURE_GRPC_DEV is a separate var enforced by the worker's
# transport_factory.go).

# ─── Terminal helpers ────────────────────────────────────────────────────────
log()    { printf '\e[36m[pilot]\e[0m %s\n' "$*"; }
ok()     { printf '\e[32m[pilot]\e[0m %s\n' "$*"; }
warn()   { printf '\e[33m[pilot][WARN]\e[0m %s\n' "$*" >&2; }
die()    { printf '\e[31m[pilot][FAIL]\e[0m %s\n' "$*" >&2; exit "${2:-1}"; }
banner() { echo; echo "──────────────────────────────────────────────────────"; echo "  $*"; echo "──────────────────────────────────────────────────────"; }

# ─── Cleanup trap ────────────────────────────────────────────────────────────

# Source command domains after configuration/helpers are initialized.
source "${REPO_ROOT}/scripts/pilot/build.sh"
source "${REPO_ROOT}/scripts/pilot/lifecycle.sh"
source "${REPO_ROOT}/scripts/pilot/job.sh"
source "${REPO_ROOT}/scripts/pilot/poll.sh"
source "${REPO_ROOT}/scripts/pilot/cleanup.sh"

# Cleanup is registered after all sourced command domains are available.
trap cleanup EXIT
# Main dispatch
# ═══════════════════════════════════════════════════════════════════════════════
main() {
  local cmd="${1:-all}"

  case "$cmd" in
    all)     cmd_all ;;
    build)   cmd_build ;;
    start)   cmd_start ;;
    submit)  cmd_submit ;;
    work)    cmd_work ;;
    stop)    cmd_stop ;;
    status)  cmd_status ;;
    log)     cmd_log ;;
    --help|-h|help)
      sed -n '/^# Usage:/,/^# Exit codes:/p' "$0" | sed 's/^# //p'
      ;;
    *)
      echo "Unknown command: ${cmd}"
      echo "Usage: $0 {all|build|start|submit|work|stop|status|log}"
      exit 2
      ;;
  esac
}

main "$@"
