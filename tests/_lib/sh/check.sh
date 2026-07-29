# =============================================================================
# tests/_lib/sh/check.sh — assert helpers (return nonzero on failure).
# =============================================================================
# All helpers LOG via log_error / log_warn (sourced from logging.sh). Failure
# messages are uniform so consumers can grep the transcript without per-file
# tweaks.
# =============================================================================

# check_file_readable <path> — verify <path> exists and is readable.
check_file_readable() {
  local f="$1"
  [[ -r "$f" ]] || { log_error "file not readable: $f"; return 1; }
}

# check_positive_int <value> <name> — verify value is a positive integer.
check_positive_int() {
  local v="$1" name="$2"
  if ! [[ "$v" =~ ^[0-9]+$ ]] || (( v <= 0 )); then
    log_error "check_positive_int: ${name}=${v} not a positive integer"
    return 1
  fi
}

# check_hex_hash <hex-string> — verify value matches a 64-char hex string.
check_hex_hash() {
  local h="$1"
  if [[ ! "$h" =~ ^[0-9a-fA-F]{64}$ ]]; then
    log_error "check_hex_hash: not a 64-char hex string (got len=${#h})"
    return 1
  fi
}

# verify_mode_consistent — verify $VERIFY_MODE is mock|live (assume the
# variable has been set by the consumer's config block).
verify_mode_consistent() {
  case "${VERIFY_MODE:-}" in
    mock|live) return 0 ;;
    *) log_error "VERIFY_MODE not consistent: ${VERIFY_MODE:-<unset>}"; return 1 ;;
  esac
}

# verify_or_warn <description> <command> [args...] — run the command, log_warn
# on failure (not log_error: verify_or_warn is non-fatal by design — the
# caller decides whether the warn escalates to test FAIL).
verify_or_warn() {
  local desc="$1"; shift
  if "$@"; then
    log_debug "verify_or_warn OK: ${desc}"
    return 0
  fi
  log_warn "verify_or_warn FAILED: ${desc}"
  return 1
}
