#!/usr/bin/env bash
# shellcheck disable=SC2034
# Shared globals are supplied by the certify-remote-fleet.sh entrypoint.
# Fleet orphan/invariant checks for certify-remote-fleet.sh.
# Sourced by the Bash entrypoint; relies on shared globals only.

fleet_check_invariants() {
  local mode="${RW_FLEET_INVARIANTS_MODE:-required}" command output
  INVARIANT_STATUS=NOT_RUN
  INVARIANT_CHECKED=0
  INVARIANT_DIAGNOSTIC=""
  INVARIANT_LEASES=null
  INVARIANT_JOBS=null
  INVARIANT_TASKS=null
  INVARIANT_OPERATIONS=null
  case "$mode" in
    skip)      INVARIANT_STATUS=SKIPPED
      INVARIANT_CHECKED=1
      INVARIANT_DIAGNOSTIC="explicitly skipped by RW_FLEET_INVARIANTS_MODE=skip"
      return 0

      ;;
    command|required) ;;
    *)
      INVARIANT_STATUS=FAIL
      INVARIANT_CHECKED=1
      INVARIANT_DIAGNOSTIC="RW_FLEET_INVARIANTS_MODE must be required, command, or skip"
      return 1
      ;;
  esac
  command="${RW_FLEET_ORPHAN_CHECK_CMD:-}"
  if [[ -z "$command" ]]; then    INVARIANT_STATUS=FAIL
    INVARIANT_CHECKED=1
    INVARIANT_DIAGNOSTIC="RW_FLEET_ORPHAN_CHECK_CMD is required unless RW_FLEET_INVARIANTS_MODE=skip"
    return 1

  fi
  [[ "$command" != *$'\r'* && "$command" != *$'\n'* ]] || {
    INVARIANT_STATUS=FAIL
    INVARIANT_CHECKED=1
    INVARIANT_DIAGNOSTIC="RW_FLEET_ORPHAN_CHECK_CMD contains CR/LF"
    return 1
  }
  output="$(mktemp "${TMPDIR:-/tmp}/velox-fleet-invariants.XXXXXX")" || {
    INVARIANT_STATUS=FAIL
    INVARIANT_CHECKED=1
    INVARIANT_DIAGNOSTIC="cannot create invariant scratch file"
    return 1
  }
  local timeout_s="${RW_FLEET_INVARIANT_TIMEOUT_S:-30}"
  if ! [[ "$timeout_s" =~ ^[1-9][0-9]*$ ]]; then
    INVARIANT_STATUS=FAIL
    INVARIANT_CHECKED=1
    INVARIANT_DIAGNOSTIC="RW_FLEET_INVARIANT_TIMEOUT_S must be a positive integer"
    rm -f -- "$output"
    return 1
  fi
  if timeout "$timeout_s" bash -c "$command" >"$output" 2>&1 && jq -e \
      '(.leases // 0) as $leases |
       (.jobs // 0) as $jobs |
       (.tasks // 0) as $tasks |
       (.operations // 0) as $operations |
       ($leases|type)=="number" and ($leases % 1 == 0) and ($leases >= 0) and
       ($jobs|type)=="number" and ($jobs % 1 == 0) and ($jobs >= 0) and
       ($tasks|type)=="number" and ($tasks % 1 == 0) and ($tasks >= 0) and
       ($operations|type)=="number" and ($operations % 1 == 0) and ($operations >= 0)' "$output" >/dev/null 2>&1; then
    INVARIANT_LEASES="$(jq -r '.leases // 0' "$output")"
    INVARIANT_JOBS="$(jq -r '.jobs // 0' "$output")"
    INVARIANT_TASKS="$(jq -r '.tasks // 0' "$output")"
    INVARIANT_OPERATIONS="$(jq -r '.operations // 0' "$output")"
    INVARIANT_STATUS=PASS
    INVARIANT_CHECKED=1
    if (( INVARIANT_LEASES + INVARIANT_JOBS + INVARIANT_TASKS + INVARIANT_OPERATIONS > 0 )); then
      INVARIANT_STATUS=FAIL
      INVARIANT_DIAGNOSTIC="orphaned resources detected"
    fi
  else
    INVARIANT_STATUS=FAIL
    INVARIANT_CHECKED=1
    INVARIANT_DIAGNOSTIC="orphan check failed or returned invalid JSON"
  fi
  rm -f -- "$output"
  [[ "$INVARIANT_STATUS" == PASS ]]
}
