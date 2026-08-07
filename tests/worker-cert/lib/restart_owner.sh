#!/usr/bin/env bash
# =============================================================================
# tests/worker-cert/lib/restart_owner.sh — canonical worker restart ownership.
#
# The production contract is systemd-owned:
#   systemd velox-worker.service: enabled + active, Restart=always,
#   RestartSec=10; Docker RestartPolicy=no.
#
# A runtime inspection command must emit one key=value line for each field:
#   systemd_is_enabled=enabled
#   systemd_is_active=active
#   systemd_restart=always
#   systemd_restart_sec=10s
#   docker_restart_policy=no
# =============================================================================

canonical_restart_owner_check_output() {
  local output="$1"
  local enabled active restart restart_sec docker_policy

  RESTART_OWNER_SYSTEMD_ENABLED=""
  RESTART_OWNER_SYSTEMD_ACTIVE=""
  RESTART_OWNER_SYSTEMD_RESTART=""
  RESTART_OWNER_SYSTEMD_RESTART_SEC=""
  RESTART_OWNER_DOCKER_POLICY=""

  enabled="$(awk -F= '$1 == "systemd_is_enabled" {print substr($0, index($0, "=") + 1); exit}' <<<"$output")"
  active="$(awk -F= '$1 == "systemd_is_active" {print substr($0, index($0, "=") + 1); exit}' <<<"$output")"
  restart="$(awk -F= '$1 == "systemd_restart" {print substr($0, index($0, "=") + 1); exit}' <<<"$output")"
  restart_sec="$(awk -F= '$1 == "systemd_restart_sec" {print substr($0, index($0, "=") + 1); exit}' <<<"$output")"
  docker_policy="$(awk -F= '$1 == "docker_restart_policy" {print substr($0, index($0, "=") + 1); exit}' <<<"$output")"

  export RESTART_OWNER_SYSTEMD_ENABLED="$enabled"
  export RESTART_OWNER_SYSTEMD_ACTIVE="$active"
  export RESTART_OWNER_SYSTEMD_RESTART="$restart"
  export RESTART_OWNER_SYSTEMD_RESTART_SEC="$restart_sec"
  export RESTART_OWNER_DOCKER_POLICY="$docker_policy"

  [[ "$enabled" == "enabled" ]] || {
    printf '[restart-owner] FAIL systemd_is_enabled=%s (expected enabled)\n' "${enabled:-<missing>}" >&2
    return 1
  }
  [[ "$active" == "active" ]] || {
    printf '[restart-owner] FAIL systemd_is_active=%s (expected active)\n' "${active:-<missing>}" >&2
    return 1
  }
  [[ "$restart" == "always" ]] || {
    printf '[restart-owner] FAIL systemd_restart=%s (expected always)\n' "${restart:-<missing>}" >&2
    return 1
  }
  [[ "$restart_sec" == "10" || "$restart_sec" == "10s" ]] || {
    printf '[restart-owner] FAIL systemd_restart_sec=%s (expected 10s)\n' "${restart_sec:-<missing>}" >&2
    return 1
  }
  [[ "$docker_policy" == "no" ]] || {
    printf '[restart-owner] FAIL docker_restart_policy=%s (expected no; systemd is the sole owner)\n' "${docker_policy:-<missing>}" >&2
    return 1
  }

  printf '[restart-owner] PASS systemd owns restart lifecycle; Docker policy=no\n'
}

canonical_restart_owner_check_command() {
  local check_command="$1" output rc
  [[ -n "$check_command" ]] || {
    printf '[restart-owner] FAIL runtime inspection command is required\n' >&2
    return 1
  }
  [[ "$check_command" != *$'\n'* && "$check_command" != *$'\r'* ]] || {
    printf '[restart-owner] FAIL runtime inspection command contains CR/LF\n' >&2
    return 1
  }
  output="$(bash -c "$check_command" 2>&1)"
  rc=$?
  if (( rc != 0 )); then
    printf '[restart-owner] FAIL runtime inspection command exited rc=%d\n' "$rc" >&2
    return 1
  fi
  canonical_restart_owner_check_output "$output"
}
