#!/usr/bin/env bash
# scripts/ci/check-db-access.sh
#
# Forbid raw *sql.DB / *sql.Tx access (`db.Exec`, `db.Query`,
# `db.QueryRow`, `db.BeginTx` -- plus the `Context` siblings) OUTSIDE the
# canonical repository/store layer. Hand-rolled SQL in a handler or a
# background job is the prototypical side-channel that breaks the
# single-writer invariant.
#
# Allowed paths (per bounded context):
#   * DataServer/internal/store/                          -- canonical data layer
#   * DataServer/internal/{store,artifacts,workflow,jobs,
#                         deliveries,assets,integrations,
#                         youtube}/**/*_repository.go     -- per-bounded-context
#                                                           repos. The list is
#                                                           explicit; do NOT
#                                                           widen via a
#                                                           wildcard.
#   * DataServer/internal/audit/                          -- read-only auditor
#   * DataServer/internal/dbutil/                         -- shared low-level helpers
#   * DataServer/cmd/server/bootstrap.go                  -- schema bootstrap
#   * `*_test.go`                                         -- unit tests
#
# ALL checks are scoped to the current branch's diff via scoped_grep.
# HEAD main is trivially green; PRs surface new regressions only.
#
# Phase 5: added chain-call patterns (.DB().QueryRowContext,
# .DB().ExecContext, .Handle().Query) to catch repository-
# bypass call sites that the original db/tx/conn patterns missed.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"
source "$(dirname "${BASH_SOURCE[0]}")/lib/diff-scope.sh"

fail_if_no_base_ref

fail() { printf 'DB-ACCESS ERROR: %s\n' "$*" >&2; exit 1; }
violations=0

# Anchor on conventional DB-handle variable names instead of any
# `.Exec(` -- the latter would also match `c.Query(...)` from gin
# handlers, overwhelming false positives.
call_pattern='\b(db|tx|conn)\.(Exec|ExecContext|Query|QueryContext|QueryRow|QueryRowContext|BeginTx|BeginTxContext)\('

hits="$(scoped_grep "$call_pattern" -- \
          'DataServer/**/*.go' \
          ':!DataServer/internal/store/**' \
          ':!DataServer/internal/artifacts/**/*_repository.go' \
          ':!DataServer/internal/workflow/**/*_repository.go' \
          ':!DataServer/internal/jobs/**/*_repository.go' \
          ':!DataServer/internal/deliveries/**/*_repository.go' \
          ':!DataServer/internal/assets/**/*_repository.go' \
          ':!DataServer/internal/integrations/**/*_repository.go' \
          ':!DataServer/internal/youtube/**/*_repository.go' \
          ':!DataServer/internal/audit/**' \
          ':!DataServer/internal/dbutil/**' \
          ':!DataServer/cmd/server/bootstrap.go' \
          ':!DataServer/**/*_test.go' \
          ':!scripts/ci/check-db-access.sh' \
          ':!scripts/ci/lib/diff-scope.sh')"

if [[ -n "$hits" ]]; then
  printf 'NEW direct database call(s) outside repository/store:\n%s\n\n' \
    "$hits" >&2
  violations=$((violations + 1))
fi

# Chain-call patterns: .DB().QueryRowContext(...), r.dbStore.DB().ExecContext(...),
# repository.Handle().Query(...). These bypass the store abstraction by reaching
# through to the raw *sql.DB handle. Only allowed inside the store layer.
chain_pattern='\.DB\(\)\.(Exec|ExecContext|Query|QueryContext|QueryRow|QueryRowContext|BeginTx|BeginTxContext)\('

chain_hits="$(scoped_grep "$chain_pattern" -- \
                'DataServer/**/*.go' \
                ':!DataServer/internal/store/**' \
                ':!DataServer/cmd/server/bootstrap.go' \
                ':!DataServer/**/*_test.go' \
                ':!scripts/ci/check-db-access.sh' \
                ':!scripts/ci/lib/diff-scope.sh')"

if [[ -n "$chain_hits" ]]; then
  printf 'NEW .DB()-chain call(s) outside store (repository bypass):\n%s\n\n' \
    "$chain_hits" >&2
  violations=$((violations + 1))
fi

# Tight guard: `sql.Open(` / `sql.OpenDB(` must only appear in the
# canonical data-layer packages.
open_pattern='\bsql\.Open\(|\bsql\.OpenDB\('

open_hits="$(scoped_grep "$open_pattern" -- \
                'DataServer/**/*.go' \
                ':!DataServer/internal/store/**' \
                ':!DataServer/internal/dbutil/**' \
                ':!DataServer/cmd/server/bootstrap.go' \
                ':!DataServer/**/*_test.go' \
                ':!scripts/ci/check-db-access.sh' \
                ':!scripts/ci/lib/diff-scope.sh')"
if [[ -n "$open_hits" ]]; then
  printf 'NEW sql.Open outside store:\n%s\n\n' "$open_hits" >&2
  violations=$((violations + 1))
fi

if [[ "$violations" -gt 0 ]]; then
  printf '%d db-access violation(s) -- see above\n' "$violations" >&2
  exit 1
fi

echo "check-db-access: OK"
