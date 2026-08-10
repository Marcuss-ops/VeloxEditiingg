# remote-worker-cert-rollback.sh — extracted worker certification lifecycle domain.
# Loaded by scripts/cert/lib/remote-worker-cert-lifecycle.sh.
# shellcheck shell=bash

rw_smoke_cleanup_command() {
  # These are the exact temp locations removed by SSHWorkerExec.CleanupWorkerTemp.
  # The command contains no operator-provided shell fragment.
  printf '%s' "find /var/lib/velox-worker/smoke -maxdepth 1 -type f -name 'smoke-*.*' -printf '%f\\n' 2>/dev/null || true; find /tmp/velox-smoke -mindepth 2 -maxdepth 2 -type f -printf '%P\\n' 2>/dev/null || true"
}
