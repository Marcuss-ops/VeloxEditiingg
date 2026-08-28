# shellcheck shell=bash
# process.sh — shared process management, cleanup, and port utilities.
# Sourced by run.sh after variables are defined; all symbols inherit
# from the caller's scope (no explicit exports needed).

# ─── Child PID tracking ─────────────────────────────────────────────────────
declare -a CHILD_PIDS=()
push_pid() { CHILD_PIDS+=("$1"); }

# ─── Port helpers ────────────────────────────────────────────────────────────
pick_free_port() {
  python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()'
}

assert_port_free() {
  local port="$1"
  if ss -ltn "sport = :${port}" | grep -q LISTEN; then
    fail "required E2E port ${port} is occupied; choose E2E_*_PORT explicitly"
    exit 3
  fi
}

# ─── Cleanup ─────────────────────────────────────────────────────────────────
kill_all() {
  local sig="${1:-TERM}"
  for pid in "${CHILD_PIDS[@]}"; do
    kill -0 "$pid" 2>/dev/null && kill -- -"$pid" 2>/dev/null || true
  done
  if [[ "$sig" == "TERM" ]]; then
    sleep 1
    for pid in "${CHILD_PIDS[@]}"; do
      kill -0 "$pid" 2>/dev/null && kill -- -"$pid" 2>/dev/null || true
    done
    for pid in "${CHILD_PIDS[@]}"; do wait "$pid" 2>/dev/null || true; done
  fi
}

on_exit() {
  kill_all TERM
  rm -f "$MASTER_PIDFILE" "$WORKER_PIDFILE"
}

setup_traps() {
  trap on_exit EXIT
  trap 'kill_all TERM; exit 130' INT
  trap 'kill_all TERM; exit 143' TERM
}

# ─── Resolve dynamic ports ──────────────────────────────────────────────────
resolve_ports() {
  if [[ -z "${E2E_MASTER_PORT:-}" ]]; then MASTER_PORT="$(pick_free_port)"; fi
  if [[ -z "${E2E_GRPC_PORT:-}" ]]; then GRPC_PORT="$(pick_free_port)"; fi
  if [[ "$WORKER_HEALTH_PORT" == "0" ]]; then WORKER_HEALTH_PORT="$(pick_free_port)"; fi
  if [[ "$WORKER_PROMETHEUS_PORT" == "0" ]]; then WORKER_PROMETHEUS_PORT="$(pick_free_port)"; fi
}
