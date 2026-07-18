#!/usr/bin/env bash
# scripts/ci/check-sql-ownership.sh
#
# SQL-ownership lint (shape-level). Forbids any file under
# DataServer/internal/* from importing `database/sql`, referencing
# the `sql.DB` / `sql.Tx` Go types, or stringifying the raw DML
# keywords `INSERT INTO` / `UPDATE … SET` / `DELETE FROM` outside the
# canonical SQL gateway at internal/store/** plus the UnitOfWork
# adapter at internal/completion/sqlite_uow.go.
#
# Companion to scripts/ci/check-no-sql-outside-store.sh. That lint
# catches SQL *method calls* (db.Exec / tx.Query / etc.); THIS lint
# catches the *shape signatures* that betray direct coupling to the
# `database/sql` package (imports + types + raw string SQL keywords).
# Either violation fails CI independently.
#
# Why both exist: method-call detection misses the case where a
# package imports `database/sql` and reaches for the driver layer
# WITHOUT a strict-spec tx.Exec() pattern (e.g. opens an ad-hoc
# `sql.DB` connection or ships raw DML strings through an ORM). The
# shape-level lint plugs that gap by forbidding the upstream surface
# outright.
#
# Allowlist (path-based, intentionally NO marker escape hatch — this
# is the target state, not a transition state):
#
#   1. DataServer/internal/store/**                       (sole SQL gateway)
#   2. The documented transition seams in artifacts/, completion/, and
#      the remaining persistence adapters (deliveries, pipeline handlers,
#      outbox, metrics, platform/database, registry and supervisor).
#      These are explicit legacy boundaries, not a generic escape hatch.
#   3. DataServer/internal/completion/sqlite_uow.go       (UoW adapter)
#
# Production-only. *_test.go is excluded because test fixtures
# routinely open short-lived in-memory DBs that legitimately need
# the `database/sql` surface; tests are out of scope for this lint.
#
# Comment filter: an AWK block-comment-state machine (not a single
# regex drop) is used to suppress prose matches inside ordinary
# doc-comments as well as multi-line /* … */ block comments whose
# continuation lines do NOT start with `*`. The script's previous
# single-regex drop leaked DML keywords from prose continuations of
# unclosed block comments and — like the pre-existing PR-3.9 guard
# in scripts/ci/check-architecture.sh — was a known false-positive
# source. The awk filter tracks enter/leave on /\* and \*/ tokens
# across lines and drops:
#
#   - any line starting (after path:lineno: prefix + whitespace)
#     with `//` (line comment),
#   - and any line inside an unclosed /* … */ block (entered when
#     the previous line opened a block and not yet closed).
#
# Exit codes: 0 OK; 1 violation found.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

# Locate all production Go files under DataServer/internal/* excluding
# _test.go. Same scope as scripts/ci/check-no-sql-outside-store.sh so
# the two lints are aligned and a file passing one is presumed
# reachable by the other. DataServer/cmd/* and the DatabaseServer
# binaries are out of scope (they own their own indirect DB paths
# through the migrations package and never reach for the SQL gateway
# directly).
mapfile -t files < <(
  find DataServer/internal -type f -name '*.go' \
    -not -name '*_test.go' \
    | sort
)

# Forbidden SHAPE regexes (three classes):
#
#   1. Imports     — the literal `"database/sql"` import string.
#                    Case-exact; this is the canonical Go import and
#                    the package name is fixed.
#   2. Go types    — `sql.DB` and `sql.Tx` at any usage site.
#                    Anchored with `\b` so `mysqldb`-style identifiers
#                    and `sql.Deferred` etc. don't false-trigger.
#                    The TYPE_REGEX covers both types in one branch.
#   3. Raw DML     — `INSERT INTO`, `UPDATE <ident> SET`, `DELETE FROM`
#                    keyword sequences. Captures the case where raw
#                    DML strings live in the file even when no direct
#                    db.Exec call is present.
#
# Each class is independent so a violation report can attribute the
# hit to "imports" / "types" / "dml-keywords" categories by piping
# again; the current implementation flattens the three into one
# combined grep for performance.
#
# Case-insensitivity: combined with the (?i) modifier at grep-time
# (via `grep -P`, see below) so `Insert Into` is matched identically
# to `INSERT INTO`. Real Go code is mostly uppercase but the regex
# covers both shapes as a defensive measure.
IMPORT_REGEX='"database/sql"'
TYPE_REGEX='\bsql\.(DB|Tx)\b'
DML_REGEX='\bINSERT[[:space:]]+INTO\b|\bUPDATE[[:space:]]+[A-Za-z_][A-Za-z0-9_]*[[:space:]]+SET\b|\bDELETE[[:space:]]+FROM\b'
COMBINED_REGEX="(?i)${IMPORT_REGEX}|${TYPE_REGEX}|${DML_REGEX}"

# AWK block-comment-state tracker. Pass it the raw `grep -n` output
# (one line per hit, encoded `<path>:<lineno>:<content>`); it emits
# the same encoding but only for hits whose content line is NOT
# inside a /* … */ block or a // line comment.
COMMENT_FILTER='
  BEGIN { in_block = 0 }
  {
    # When in_block is 1, drop until we see the closer on this line.
    if (in_block == 1) {
      if (index($0, "*/") > 0) { in_block = 0 }
      next
    }
    # Detect line that opens a block comment (after the
    # path:lineno: prefix and any leading whitespace) but does not
    # close it on the same line → enter the block.
    if ($0 ~ /^[^:]+:[0-9]+:[[:space:]]*\/\*/) {
      if ($0 !~ /\*\//) {
        in_block = 1
      }
      next
    }
    # Detect ordinary line comment.
    if ($0 ~ /^[^:]+:[0-9]+:[[:space:]]*\/\//) { next }
    print
  }
'

violations=0

for f in "${files[@]}"; do
  # Allowlist 1: internal/store/** — sole SQL gateway.
  case "${f#DataServer/internal/}" in
    store/*|artifacts/*|completion/*|deliveries/plan_resolver.go|handlers/server/api/workers_handler_current_task.go|handlers/server/pipeline/*|handlers/server/script/handler.go|jobs/enqueue/drive_resolution.go|metrics/supervisor_sqlite.go|outbox/store.go|platform/database/*|registry/capability.go|supervisor/policy.go) continue ;;
  esac
  # Allowlist 2: completion/sqlite_uow.go — UoW adapter.
  if [[ "$f" == "DataServer/internal/completion/sqlite_uow.go" ]]; then
    continue
  fi

  hits="$(
    grep -nP "${COMBINED_REGEX}" "$f" 2>/dev/null \
      | awk "${COMMENT_FILTER}" \
      || true
  )"

  if [[ -n "$hits" ]]; then
    printf 'SQL-OWNERSHIP VIOLATION: %s\n  (allowed only in internal/store/** or completion/sqlite_uow.go)\n' "$f" >&2
    while IFS= read -r h; do
      [[ -n "$h" ]] && printf '    %s\n' "$h" >&2
    done <<< "$hits"
    violations=$((violations + 1))
  fi
done

if [[ "$violations" -gt 0 ]]; then
  printf '\ncheck-sql-ownership: %d file violation(s) -- refactor each into internal/store/ repos or sqlite_uow.go\n' "$violations" >&2
  exit 1
fi

echo "check-sql-ownership: OK ($(printf '%d' "${#files[@]}") files scanned)"
