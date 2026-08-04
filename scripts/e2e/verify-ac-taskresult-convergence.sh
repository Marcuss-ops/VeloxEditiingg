#!/usr/bin/env bash
# Adapter for the golden E2E harness. The convergence logic is centralized in
# scripts/ci/check-ac-taskresult-convergence.sh; this file only derives the
# worker-local paths from the canonical golden test layout.
set -euo pipefail

DB=""; JOB_ID=""; WORKER_STATE_DIR=""; WORKER_LOG=""
usage() {
  echo "usage: $0 --db DB --job-id ID --worker-state-dir DIR --worker-log FILE" >&2
  exit 3
}
while [[ $# -gt 0 ]]; do
  case "$1" in
    --db) DB="$2"; shift 2;;
    --job-id) JOB_ID="$2"; shift 2;;
    --worker-state-dir) WORKER_STATE_DIR="$2"; shift 2;;
    --worker-log) WORKER_LOG="$2"; shift 2;;
    --help|-h) usage;;
    *) usage;;
  esac
done
[[ -n "$DB" && -n "$JOB_ID" && -n "$WORKER_STATE_DIR" && -n "$WORKER_LOG" ]] || usage

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
SPOOL_DB="$WORKER_STATE_DIR/executor_spool/worker_output_spool.sqlite3"
[[ -f "$SPOOL_DB" ]] || { echo "FATAL: worker spool database not found: $SPOOL_DB" >&2; exit 1; }
[[ -f "$WORKER_LOG" ]] || { echo "FATAL: worker log not found: $WORKER_LOG" >&2; exit 1; }
exec "$REPO_ROOT/scripts/ci/check-ac-taskresult-convergence.sh" \
  --db "$DB" \
  --job-id "$JOB_ID" \
  --worker-spool-db "$SPOOL_DB" \
  --worker-log "$WORKER_LOG"
