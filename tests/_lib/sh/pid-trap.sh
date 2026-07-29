#!/usr/bin/env bash
# =============================================================================
# tests/_lib/sh/pid-trap.sh — child-PID tracking + TERM→KILL escalation.
# =============================================================================
# Owns the parallel arrays _LIB_CHILD_PIDS / _LIB_CHILD_LABELS that record
# every background process spawned by a test script. On exit / INT / TERM
# (set up by each consumer script independently — this file does NOT install
# traps itself; that intentionally stays with the consumer so the post-kill
# cleanup hook can vary per scenario), lib_kill_all sends TERM to each tracked
# process, escalates to KILL after 1s, and `wait`s on each to let the kernel
# reap.
#
# Public surface: lib_push_pid, lib_kill_all, lib_reset_children.
# Idempotent: re-declare guards prevent clobbering when sourced twice.
# =============================================================================

# Idempotent guards — declare-once per shell.
if [[ ! -v _LIB_CHILD_PIDS ]]; then
  declare -ga _LIB_CHILD_PIDS=()
fi
if [[ ! -v _LIB_CHILD_LABELS ]]; then
  declare -ga _LIB_CHILD_LABELS=()
fi

# lib_push_pid <pid> <label> — record a spawned child PID + human label.
lib_push_pid() {
  _LIB_CHILD_PIDS+=("$1")
  _LIB_CHILD_LABELS+=("$2")
}

# lib_reset_children — clear the tracking arrays. Each scenario in a multi-
# case matrix (e.g. gRPC 6-case) calls this at the top of its case_N_*()
# function so that the per-scenario child PID set is scoped to that case,
# not accumulated across cases. Master + worker PIDs for the previous
# case are already reaped via lib_kill_all TERM before the next case starts.
lib_reset_children() {
  _LIB_CHILD_PIDS=()
  _LIB_CHILD_LABELS=()
}

# lib_kill_all [SIGNAL] — TERM by default; escalates to KILL after 1s when
# called with TERM. Always returns 0 (best-effort reaping).
lib_kill_all() {
  local sig="${1:-TERM}"
  local n=${#_LIB_CHILD_PIDS[@]}
  if (( n == 0 )); then return 0; fi
  printf "[lib] sending %s to %d child(ren): %s\n" "$sig" "$n" "${_LIB_CHILD_LABELS[*]}"
  for i in "${!_LIB_CHILD_PIDS[@]}"; do
    local pid="${_LIB_CHILD_PIDS[$i]}"
    if kill -0 "$pid" 2>/dev/null; then
      kill -"$sig" "$pid" 2>/dev/null || true
    fi
  done
  if [[ "$sig" == "TERM" ]]; then
    sleep 1
    for i in "${!_LIB_CHILD_PIDS[@]}"; do
      local pid="${_LIB_CHILD_PIDS[$i]}"
      if kill -0 "$pid" 2>/dev/null; then
        printf "[lib] escalating to KILL: pid=%s label=%s\n" "$pid" "${_LIB_CHILD_LABELS[$i]}"
        kill -KILL "$pid" 2>/dev/null || true
      fi
    done
    for pid in "${_LIB_CHILD_PIDS[@]}"; do
      wait "$pid" 2>/dev/null || true
    done
  fi
}
