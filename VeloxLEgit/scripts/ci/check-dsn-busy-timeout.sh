#!/usr/bin/env bash
# scripts/ci/check-dsn-busy-timeout.sh
#
# SQLite in-memory concurrency guard. Forbids in-memory SQLite DSN
# strings (`:memory:`, `file::memory:`, `mode=memory`, `cache=shared`)
# that lack the mandatory `_busy_timeout=5000` query parameter.
#
# Why this matters: absent this pragma, the go-sqlite3 driver fails
# FAST with SQLITE_BUSY ("database is locked") the moment a second
# goroutine tries to begin writing while another holds a read txn or
# another writer is mid-flight. Test suites that own private in-mem
# DBs across goroutines (canonical: DataServer/internal/store/*_test.go)
# see spurious flaky failures that drown out real bugs. The
# canonical fix: append `_busy_timeout=5000` (= 5 seconds) so the
# writer waits before returning busy.
#
# Scope-categorised check (in-scope MUST have the param; out-of-scope
# is surface-only so the operator sees outstanding debt without
# blocking CI):
#
#   IN-SCOPE (FAIL by default)
#     - DataServer/internal/completion/**
#     - DataServer/internal/forwarding/**
#     - DataServer/internal/placement/**
#     - DataServer/internal/store/**
#     These 4 packages were named explicitly in the Blocco 5 / B1
#     follow-up scope after the Blocco 1 code-review flagged
#     `:memory:` + `file::memory:?cache=shared` DSNs without the
#     busy-timeout pragma.
#
#   OUT-OF-SCOPE (WARN by default; FAIL only under STRICT_DSN_BUSY=1)
#     - Any other Go file in DataServer that opens an in-memory
#       SQLite DSN. The check still SURFACES these so operators see
#       outstanding debt; it doesn't BLOCK CI on them by default so
#       this commit stays a single-package fix. STRICT mode is the
#       Phase 6 ramp-up toggle that escalates all out-of-scope hits
#       to fail-loud once the legacy debt is paid off.
#
# Comment filter (AWK block-comment-state machine, mirrored from
# scripts/ci/check-sql-ownership.sh — see THAT script's docstring for
# the rationale). Drops:
#
#   - line comments (// …) — content lines whose first non-whitespace
#     run after the path:lineno: prefix is `//`,
#   - block-comment openers (/* …) and same-line closers,
#   - and everything between an unclosed /* and the matching */.
#
# This filter prevents matching pure prose like a comment that
# documents `file::memory:?cache=shared&_busy_timeout=5000` for
# future maintainers — only REAL CODE lines with real `sql.Open(...,
# …)` or `:memory:` token are eligible.
#
# Exit codes: 0 OK; 1 in-scope violation OR STRICT-mode violation.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

IN_MEM_PATTERN='(:memory:|file::memory:|mode=memory|cache=shared)'
REQUIRED_PARAM='_busy_timeout=5000'

# In-scope packages (FAIL by default).
SCOPE_PACKAGES=(
  "DataServer/internal/completion/"
  "DataServer/internal/forwarding/"
  "DataServer/internal/placement/"
  "DataServer/internal/store/"
)

in_scope() {
  local file="$1"
  local pkg
  for pkg in "${SCOPE_PACKAGES[@]}"; do
    if [[ "$file" == "$pkg"* ]]; then
      return 0
    fi
  done
  return 1
}

# AWK block-comment-state filter. We invoke grep -nE per single file
# inside the loop, so the input format is `<lineno>:<content>` with a
# SINGLE leading colon (grep prepends the FILENAME only when invoked
# on ≥2 files via -r/-H; per-file invocation defaults to lines-only
# output). The filter emits the same encoding for any line whose
# content is NOT a Go comment.
#
# Anchor rationale: `^[0-9]+:[[:space:]]*` matches the lineno + the
# post-colon whitespace. We deliberately do NOT match a trailing
# `// foo` inline comment on a code line — the line still has a
# real DSN literal we want to flag if that literal lacks the
# busy-timeout pragma.
COMMENT_FILTER='
  BEGIN { in_block = 0 }
  {
    # Inside an unclosed /* … */ block: drop until we see the closer.
    if (in_block == 1) {
      if (index($0, "*/") > 0) { in_block = 0 }
      next
    }
    # Drop ordinary line comments: lineno:<ws>//…
    if ($0 ~ /^[0-9]+:[[:space:]]*\/\//) { next }
    # Drop block-comment openers — and same-line closers (the emitter
    # still re-checks for a closer regardless to toggle in_block
    # correctly on the partial-close path).
    if ($0 ~ /^[0-9]+:[[:space:]]*\/\*/) {
      if (index($0, "*/") == 0) { in_block = 1 }
      next
    }
    print
  }
'

violations=0
warnings=0

mapfile -t files < <(find DataServer -type f -name '*.go' | sort)

for f in "${files[@]}"; do
  # Two-stage pipeline: (1) grep -n finds every line that mentions
  # any in-memory SQLite marker, (2) AWK drops Go comments, (3)
  # `grep -vE REQUIRED_PARAM` keeps only the lines that ALSO lack
  # the canonical busy-timeout pragma. The result is a list of REAL
  # CODE LINE HITS where a real DSN-string literal is missing the
  # pragma. The `|| true` on the final grep guards set -e under
  # pipefail when the final grep has no matches (returns 1).
  hits="$(
    grep -nE "${IN_MEM_PATTERN}" "$f" 2>/dev/null \
      | awk "${COMMENT_FILTER}" \
      | grep -vE "${REQUIRED_PARAM}" \
      || true
  )"

  if [[ -z "$hits" ]]; then
    continue
  fi

  if in_scope "$f"; then
    # In-scope failure: always fail the gate (regardless of strict mode).
    printf '✗ [in-scope] missing _busy_timeout=5000 on in-mem DSN: %s\n' "$f" >&2
    while IFS= read -r line; do
      [[ -n "$line" ]] && printf '    %s\n' "$line" >&2
    done <<< "$hits"
    violations=$((violations + 1))
  else
    # Out-of-scope: warn by default, fail under STRICT.
    if [[ "${STRICT_DSN_BUSY:-0}" == "1" ]]; then
      printf '✗ [strict, out-of-scope] missing _busy_timeout=5000: %s\n' "$f" >&2
      while IFS= read -r line; do
        [[ -n "$line" ]] && printf '    %s\n' "$line" >&2
      done <<< "$hits"
      violations=$((violations + 1))
    else
      printf '⚠ [out-of-scope] legacy file missing _busy_timeout=5000 (STRICT_DSN_BUSY=1 to fail): %s\n' "$f" >&2
      warnings=$((warnings + 1))
    fi
  fi
done

# Always surface the non-strict-warning count so operators see
# outstanding legacy debt even when the gate passes.
if [[ "$warnings" -gt 0 ]]; then
  printf '\ncheck-dsn-busy-timeout: %d non-strict legacy warning(s); set STRICT_DSN_BUSY=1 to escalate; add `&_busy_timeout=5000` (or `?_busy_timeout=5000`) to retire each entry\n' \
    "$warnings" >&2
fi

if [[ "$violations" -gt 0 ]]; then
  printf '\ncheck-dsn-busy-timeout: %d blocking violation(s) -- append "?_busy_timeout=5000" or "&_busy_timeout=5000" to the DSN literal\n' \
    "$violations" >&2
  exit 1
fi

printf '✓ check-dsn-busy-timeout: OK (%d files scanned, %d in-scope OK, %d non-strict legacy warnings)\n' \
  "${#files[@]}" "$(printf '%s\n' "${files[@]}" | grep -cE 'DataServer/internal/(completion|forwarding|placement|store)/' || true)" "$warnings"
