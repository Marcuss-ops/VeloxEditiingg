#!/usr/bin/env bash
# Sourced by scripts/pilot.sh; definitions only.

cleanup() {
  if [[ "${SKIP_CLEANUP:-0}" != "1" ]]; then
    log "cleanup: stopping processes"
    [[ -f "$MASTER_PIDFILE" ]] && kill -- "-$(cat "$MASTER_PIDFILE")" 2>/dev/null || true
    [[ -f "$WORKER_PIDFILE" ]] && kill -- "-$(cat "$WORKER_PIDFILE")" 2>/dev/null || true
    wait 2>/dev/null || true
    # Remove pid files so subsequent cmd_status reports correctly.
    rm -f "$MASTER_PIDFILE" "$WORKER_PIDFILE"
  fi
}
