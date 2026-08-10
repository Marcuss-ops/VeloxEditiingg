#!/usr/bin/env bash
# shellcheck disable=SC2034
# Shared globals are supplied by the certify-remote-fleet.sh entrypoint.
# Safety gates and argument validation for certify-remote-fleet.sh.
# Sourced by the Bash entrypoint; relies on fleet_die and shared globals.

require_command() {
  command -v "$1" >/dev/null 2>&1 || {
    fleet_die "required command not found: $1"
    return 1
  }
}

validate_worker_id() {
  local worker="$1"
  [[ -n "$worker" && "$worker" != *[[:space:]]* ]] || {
    fleet_die "worker ID contains whitespace: $worker"
    return 1
  }
  [[ "$worker" =~ ^[A-Za-z0-9][A-Za-z0-9._:-]*$ ]] || {
    fleet_die "invalid worker ID: $worker"
    return 1
  }
}

trim_worker_id() {
  # Trim only CSV separator padding; internal whitespace is rejected rather
  # than silently changing the requested identity.
  printf '%s' "$1" | sed 's/^[[:space:]]*//; s/[[:space:]]*$//'
}

validate_mode() {
  case "$1" in
    quick|full|destructive) return 0 ;;
    *) fleet_die "mode must be quick, full, or destructive (got: $1)"; return 1 ;;
  esac
}

validate_safe_path_value() {
  local name="$1" value="$2"
  [[ -n "$value" && "$value" != *$'\n'* && "$value" != *$'\r'* ]] || {
    fleet_die "${name} must be non-empty and contain no newlines"
    return 1
  }
}

production_like() {
  local env_name="${VELOX_CERT_ENV:-${VELOX_ENV:-${ENVIRONMENT:-}}}"
  local url="${MASTER_URL:-${VELOX_MASTER_URL:-}}"
  local production_flag="${VELOX_PRODUCTION:-${PRODUCTION:-0}}"
  env_name="${env_name,,}"
  url="${url,,}"
  [[ "$production_flag" == 1 || "$production_flag" == true ]] && return 0
  case "$env_name" in
    prod|production|live|release) return 0 ;;
  esac
  case "$url" in
    *production*|*prod.*|*prod-*|*://prod/*|*://prod:*) return 0 ;;
  esac
  return 1
}

guard_destructive() {
  local env_name="${VELOX_CERT_ENV:-${VELOX_ENV:-${ENVIRONMENT:-}}}"
  env_name="${env_name,,}"
  case "$env_name" in
    staging|canary|development|test|local) ;;
    prod|production|live|release)
      fleet_die "destructive mode is blocked in environment '${env_name}'"
      return 1
      ;;
    *)
      fleet_die "destructive mode requires explicit VELOX_CERT_ENV=staging|canary|development|test|local"
      return 1
      ;;
  esac
  if production_like; then
    fleet_die "destructive mode is blocked for production-like MASTER_URL/environment"
    return 1
  fi
  [[ "${VELOX_CERT_ALLOW_DESTRUCTIVE:-0}" == 1 ]] || {
    fleet_die "set VELOX_CERT_ALLOW_DESTRUCTIVE=1 to opt into destructive mode"
    return 1
  }
  [[ "${VELOX_CERT_DESTRUCTIVE_ACK:-}" == I_UNDERSTAND_DESTRUCTIVE_CERT ]] || {
    fleet_die "set VELOX_CERT_DESTRUCTIVE_ACK=I_UNDERSTAND_DESTRUCTIVE_CERT to confirm destructive testing"
    return 1
  }
  [[ -n "${RW_WORKER_CRASH_CMD:-}" ]] || {
    fleet_die "RW_WORKER_CRASH_CMD is required for destructive mode"
    return 1
  }
  [[ -n "${RW_WORKER_RESTART_OWNER_CHECK_CMD:-}" ]] || {
    fleet_die "RW_WORKER_RESTART_OWNER_CHECK_CMD is required for destructive mode"
    return 1
  }
  [[ -n "${RW_JOB_DESTINATION_ID:-}" ]] || {
    fleet_die "RW_JOB_DESTINATION_ID is required for destructive mode"
    return 1
  }
  [[ -n "${RW_FLEET_WORKER_START_CMD:-}" ]] || {
    fleet_die "RW_FLEET_WORKER_START_CMD is required for destructive cleanup"
    return 1
  }
  if [[ "${RW_FLEET_NETWORK_RULES_APPLIED:-0}" == 1 && -z "${RW_FLEET_RESTORE_NETWORK_CMD:-}" ]]; then
    fleet_die "RW_FLEET_RESTORE_NETWORK_CMD is required when RW_FLEET_NETWORK_RULES_APPLIED=1"
    return 1
  fi
  validate_safe_path_value RW_WORKER_CRASH_CMD "$RW_WORKER_CRASH_CMD" || return 1
  validate_safe_path_value RW_WORKER_RESTART_OWNER_CHECK_CMD "$RW_WORKER_RESTART_OWNER_CHECK_CMD" || return 1
  [[ -x "$DESTRUCTIVE_RUNNER" || -f "$DESTRUCTIVE_RUNNER" ]] || {
    fleet_die "destructive runner not found: $DESTRUCTIVE_RUNNER"
    return 1
  }
}

