# =============================================================================
# tests/_lib/sh/ensure.sh — idempotent resource setters (mkdir / tmpdir / cli).
# =============================================================================

# ensure_dir <path> — create directory if missing; log_error + return 1 on failure.
ensure_dir() {
  [[ $# -gt 0 ]] || { log_error "ensure_dir requires at least one path"; return 1; }
  local d
  for d in "$@"; do
    [[ -d "$d" ]] && continue
    mkdir -p "$d" || { log_error "ensure_dir failed: $d"; return 1; }
  done
  return 0
}

# mkdir_p <path> — thin alias for ensure_dir (preserves prior call sites in
# run.sh that used mkdir_p; semantically identical post-extraction).
mkdir_p() { ensure_dir "$1"; }

# ensure_clean_tmpdir <root_prefix> — mktemp under root_prefix; returns the
# new dir path via stdout; also assigns it to $TMP_DIR (legacy field for
# consumers that read TMP_DIR for cleanup_trap teardown).
ensure_clean_tmpdir() {
  local root="${1:-/tmp}"
  TMP_DIR="$(mktemp -d "${root}.XXXXXX" 2>/dev/null || mktemp -d)"
  ensure_dir "${TMP_DIR}"
  printf '%s' "${TMP_DIR}"
}

# ensure_command_available <cmd> — return 0 if cmd is on PATH; otherwise
# log_warn + return 1.
ensure_command_available() {
  local cmd="$1"
  if command -v "$cmd" >/dev/null 2>&1; then return 0; fi
  log_warn "command '$cmd' not found on PATH"
  return 1
}
