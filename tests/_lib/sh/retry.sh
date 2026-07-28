# =============================================================================
# tests/_lib/sh/retry.sh — retry-with-backoff (linear delay).
# =============================================================================
# Usage: retry_with_backoff <attempts> <sleep_seconds> -- <cmd> [args...]
# Returns 0 on first success; 1 on exhaustion. Emits log_warn per attempt
# failure and log_error on exhaustion.
# =============================================================================
# This is a synchronous helper. For async / parallel retries with a per-trial
# timeout, use it from a subshell or wrap in `timeout`.
# =============================================================================

retry_with_backoff() {
  local attempts="$1" sleep_seconds="$2"; shift 2
  # Tolerate the documented `--` separator (canonical bash idiom: the
  # caller writes `retry_with_backoff N S -- cmd args...` to terminate
  # option parsing). If absent, fall through and invoke what remains.
  # Without this strip, `"$@"` after `shift 2` would treat the literal
  # `--` as a command name → `command not found`, aborting under set -e
  # before the retry loop ever starts.
  #
  # NOTE: `${1:-}` (NOT bare `$1`) is required for `set -u` safety when
  # the caller invoked the helper with zero trailing positional args;
  # bare `$1` would abort the whole test under `set -u`. Do NOT
  # `simplify` this to `$1` in a future cleanup pass.
  if [[ "${1:-}" == "--" ]]; then shift; fi
  if (( attempts <= 0 )); then
    log_error "retry_with_backoff: attempts must be positive (got $attempts)"
    return 1
  fi
  if [[ $# -lt 1 ]]; then
    log_error "retry_with_backoff: missing command after '--' (got $# positional args)"
    return 1
  fi
  local i=0
  while (( i < attempts )); do
    i=$(( i + 1 ))
    if "$@"; then
      return 0
    fi
    log_warn "retry attempt $i/$attempts failed (rc=$?)"
    sleep "$sleep_seconds"
  done
  log_error "retry exhausted $attempts attempts"
  return 1
}
