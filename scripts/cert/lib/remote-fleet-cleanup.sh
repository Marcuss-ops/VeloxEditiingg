#!/usr/bin/env bash
# shellcheck disable=SC2034
# Shared globals are supplied by the certify-remote-fleet.sh entrypoint.
# Cleanup and EXIT-trap lifecycle for certify-remote-fleet.sh.
# Sourced by the Bash entrypoint; relies on shared globals and invariant checks.

run_cleanup_command() {
  local command="$2" output rc timeout_s="${RW_FLEET_CLEANUP_TIMEOUT_S:-30}"
  [[ "$timeout_s" =~ ^[1-9][0-9]*$ ]] || {
    printf '%s' FAILED
    return 1
  }
  [[ -n "$command" ]] || {
    printf '%s' NOT_CONFIGURED
    return 1
  }
  output="$(mktemp "${TMPDIR:-/tmp}/velox-fleet-cleanup.XXXXXX")" || {
    printf '%s' FAILED
    return 1
  }
  if timeout "$timeout_s" bash -c "$command" >"$output" 2>&1; then
    rc=0
  else
    rc=$?
  fi
  rm -f -- "$output"
  if (( rc == 0 )); then
    printf '%s' PASS
    return 0
  fi
  printf '%s' FAILED
  return 1
}

fleet_cleanup() {
  local rc=$? network_rc=0 worker_rc=0 temp_rc=0 cleanup_report
  (( FLEET_CLEANUP_RUNNING == 0 )) || return "$rc"
  FLEET_CLEANUP_RUNNING=1

  # Early exits still receive a final invariant observation and a durable
  # cleanup report. Normal runs write the richer fleet report in main().
  if [[ -n "${FLEET_DIR:-}" && "$INVARIANT_CHECKED" == 0 ]]; then
    fleet_check_invariants || rc=1
  fi

  if [[ "${RW_FLEET_NETWORK_RULES_APPLIED:-0}" == 1 ]]; then
    if CLEANUP_NETWORK_STATUS="$(run_cleanup_command network "${RW_FLEET_RESTORE_NETWORK_CMD:-}")"; then
      :
    else
      network_rc=1
    fi
  else
    CLEANUP_NETWORK_STATUS=NOT_APPLIED
  fi

  if [[ "${RW_FLEET_ENSURE_WORKER_STARTED:-0}" == 1 ]]; then
    if CLEANUP_WORKER_STATUS="$(run_cleanup_command worker "${RW_FLEET_WORKER_START_CMD:-}")"; then
      :
    else
      worker_rc=1
    fi
  else
    CLEANUP_WORKER_STATUS=NOT_REQUIRED
  fi

  if [[ -n "${FLEET_TMP_DIR:-}" && -d "$FLEET_TMP_DIR" ]]; then
    if rm -rf -- "$FLEET_TMP_DIR"; then
      CLEANUP_TEMP_STATUS=PASS
    else
      CLEANUP_TEMP_STATUS=FAIL
      temp_rc=1
    fi
  else
    CLEANUP_TEMP_STATUS=NOT_FOUND
  fi
  if [[ -n "${FLEET_RESULTS_FILE:-}" && "$FLEET_FINALIZING" != 1 ]]; then
    rm -f -- "$FLEET_RESULTS_FILE" || temp_rc=1
  fi
  if [[ -n "${FLEET_DIR:-}" && "${FLEET_REPORT_WRITTEN:-0}" != 1 ]]; then
    cleanup_report="${FLEET_DIR}/cleanup-report.json"
    jq -n --arg schema 'velox.remote_worker.fleet.cleanup.v1' \
      --arg run_id "${FLEET_RUN_ID:-}" --arg mode "${FLEET_MODE:-}" \
      --arg status "$([[ "$rc" -eq 0 && "$INVARIANT_STATUS" == PASS ]] && printf PASS || printf FAIL)" \
      --arg invariant_status "${INVARIANT_STATUS:-NOT_RUN}" \
      --arg invariant_diagnostic "${INVARIANT_DIAGNOSTIC:-}" \
      --arg network "${CLEANUP_NETWORK_STATUS:-NOT_RUN}" \
      --arg worker "${CLEANUP_WORKER_STATUS:-NOT_RUN}" \
      --arg temporary "${CLEANUP_TEMP_STATUS:-NOT_RUN}" \
      --argjson leases "${INVARIANT_LEASES:-null}" --argjson jobs "${INVARIANT_JOBS:-null}" \
      --argjson tasks "${INVARIANT_TASKS:-null}" --argjson operations "${INVARIANT_OPERATIONS:-null}" \
      '{schema:$schema,run_id:$run_id,mode:$mode,status:$status,cleanup:{network:$network,worker:$worker,temporary:$temporary},invariants:{status:$invariant_status,diagnostic:(if $invariant_diagnostic=="" then null else $invariant_diagnostic end),orphan_leases:$leases,orphan_jobs:$jobs,orphan_tasks:$tasks,orphan_operations:$operations}}' >"$cleanup_report" || rc=1
  fi
  (( network_rc == 0 && worker_rc == 0 && temp_rc == 0 )) || rc=1
  return "$rc"
}
