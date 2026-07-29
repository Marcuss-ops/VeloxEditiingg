#!/usr/bin/env bash
# =============================================================================
# tests/_lib/sh/logging.sh — ISO-8601 timestamped log_{debug,info,warn,error}.
# =============================================================================
# Reads LOG_LEVEL env (debug|info|warn|error; default info). Always writes to
# stderr so the stdout stream stays clean for downstream piping. Idempotent:
# re-sourcing in the same shell refreshes LOG_THRESHOLD without re-declaring.
# =============================================================================

: "${LOG_LEVEL:=info}"

# Map a human log level name to a numeric threshold (lower = more verbose).
_lib_log_threshold_for() {
  case "${LOG_LEVEL}" in
    debug) echo 0 ;;
    info)  echo 1 ;;
    warn)  echo 2 ;;
    error) echo 3 ;;
    *)     echo 1 ;;
  esac
}

# Idempotent init (re-sourcing refreshes without losing the global).
LOG_THRESHOLD="$(NO_COLOR=1 _lib_log_threshold_for)"
export LOG_THRESHOLD

# Internal: ISO-8601 UTC timestamp (millisecond granularity on platforms that
# support it; falls back to second granularity otherwise).
_lib_ts() { date -u +'%Y-%m-%dT%H:%M:%SZ'; }

log_debug() { (( LOG_THRESHOLD <= 0 )) && printf '%s DEBUG %s\n' "$(_lib_ts)" "$*" >&2 || true; }
log_info()  { (( LOG_THRESHOLD <= 1 )) && printf '%s INFO  %s\n' "$(_lib_ts)" "$*" >&2 || true; }
log_warn()  { (( LOG_THRESHOLD <= 2 )) && printf '%s WARN  %s\n' "$(_lib_ts)" "$*" >&2 || true; }
log_error() { (( LOG_THRESHOLD <= 3 )) && printf '%s ERROR %s\n' "$(_lib_ts)" "$*" >&2 || true; }
