#!/usr/bin/env bash
# scripts/ci/check-no-legacy.sh
#
# Blacklist of CONCRETE forbidden symbols. These are not patterns to be
# loosely interpreted -- each entry is a removed-alias / removed-flag /
# removed-symbol from a completed restructure. Resurrecting any of
# them in a NEW PR would reintroduce parallel-write or fallback drift.
#
# The grep is scoped to the current branch's diff vs BASE_REF (default
# origin/main) via scoped_grep. This means:
#   * HEAD main is trivially GREEN (no diff vs origin/main).
#   * PR diffs surface new regressions only -- pre-existing historical
#     references in `docs/...` / `frontend_standalone/README.md` etc.
#     do not block CI; cleaning them up is a follow-up PR in itself.
#
# Excluded by default (still off-limits within the diff):
#   * docs/**                 -- historical documentation (intentional)
#   * frontend_standalone/**  -- sealed frontend bundle
#   * scripts/ci/check-no-legacy.sh -- this script lists them
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"
# shellcheck source=lib/diff-scope.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib/diff-scope.sh"

# Prohibited patterns checked on the FULL TREE (cannot exist anywhere, even in existing main code)
full_tree_patterns=(
  'refactored/'                       # Old double-root; single-root rule
  'VELOX_DB_DSN'                      # Retired alias for VELOX_DB_PATH
  'march=native'                      # Non-reproducible C++ binaries
  'mtune=native'                      # Same
  # Dead packages removed in Ondata 1 cleanup — must not reappear.
  'services/joblifecycle'             # Removed dead wrapper (Ondata 1)
  'services/progress'                 # Removed dead wrapper (Ondata 1)
  'handlers/web/darkeditor'           # Removed dead handler (Ondata 1)
  # Generic utility packages — use domain-specific packages.
  'internal/util'                     # Removed; use identity/, platform/clock/
)

# Prohibited patterns checked only on the DIFF (forbidden in new/modified code)
diff_patterns=(
  'NewLegacy'                         # Stub left from de-legacy migration
  'DeprecatedService'                 # Same
  'local-workers.sh.deprecated'       # Replaced by data/ansible
)

violations=0

# 1. Full-tree checks
for pattern in "${full_tree_patterns[@]}"; do
  if matches="$(
         git grep -nE "$pattern" -- \
           ':!docs/**' \
           ':!frontend_standalone/**' \
           ':!scripts/ci/check-no-legacy.sh' \
           ':!scripts/ci/lib/diff-scope.sh' 2>/dev/null || true
       )"; [[ -n "$matches" ]]; then
    printf 'FORBIDDEN (exists in repository): %s\n%s\n\n' \
      "$pattern" "$matches" >&2
    violations=$((violations + 1))
  fi
done

# 2. Diff-scoped checks
for pattern in "${diff_patterns[@]}"; do
  if matches="$(
         scoped_grep "$pattern" -- \
           ':!docs/**' \
           ':!frontend_standalone/**' \
           ':!scripts/ci/check-no-legacy.sh' \
           ':!scripts/ci/lib/diff-scope.sh'
       )"; [[ -n "$matches" ]]; then
    printf 'FORBIDDEN (new in this PR): %s\n%s\n\n' \
      "$pattern" "$matches" >&2
    violations=$((violations + 1))
  fi
done

# Worker-agent Makefile fallback chain is forbidden. Pre-restructure the
# worker agent used `git describe` / `echo "dev"` / `wildcard VERSION`
# to fabricate a version when VERSION.txt was missing -- a classic
# CI-cheat path. The Makefile is now hard-failed if VERSION.txt is
# absent (no fallback). NEW regressions (e.g. re-introducing the chain)
# must surface.
if matches="$(
       scoped_grep 'git describe|\bdev\b["\047]|\bwildcard\s+VERSION' -- \
         RemoteCodex/native/worker-agent-go/Makefile
     )"; [[ -n "$matches" ]]; then
  printf 'Worker version fallback is forbidden (new in this PR):\n%s\n\n' \
    "$matches" >&2
  violations=$((violations + 1))
fi

if [[ "$violations" -gt 0 ]]; then
  printf '%d forbidden legacy pattern(s) introduced in this PR\n' \
    "$violations" >&2
  exit 1
fi

echo "check-no-legacy: OK"
