#!/usr/bin/env bash
# scripts/ci/check-registry.sh
#
# Ensure that any handler / resolver / provider is wired through the
# CANONICAL registry rather than a hand-rolled interface / second
# registry of the same shape.
#
# Two rules with two scopes:
#
#   1. WHOLE-TREE: the canonical registry factories
#      (NewRegistry, NewResolverRegistry, workersreg\.New) MUST be
#      referenced from `DataServer/cmd/server/bootstrap.go`. If the
#      symbol disappears entirely from bootstrap, the assembly wiring
#      is broken for the whole repo.
#      NOTE: `NewProviderRegistry` was previously listed here but is a
#      fictitious symbol -- bootstrap actually calls `deliveries.NewRegistry`
#      and that path is already covered by the `NewRegistry` token.
#
#   2. PR-SCOPED: no NEW `Register` / `Lookup` methods outside the
#      canonical registry packages. A new file with the same shape is
#      a v2 of a registry. We scope to BASE_REF so historical
#      artifacts in `internal/app/registry.go` etc. don't block CI;
#      only future introductions are flagged.
#
# Earlier v2 also scanned `var X = map[...]` -- dropped: too broad
# (matches HTTP status text, MIME tables, error dictionaries).
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"
source "$(dirname "${BASH_SOURCE[0]}")/lib/diff-scope.sh"

fail() { printf 'REGISTRY ERROR: %s\n' "$*" >&2; exit 1; }
violations=0

bootstrap_rel="DataServer/cmd/server/bootstrap.go"

# Rule 1: canonical registry factories MUST appear in bootstrap.
canonical_registries=(
  'NewRegistry'                 # outbox + deliveries
  'NewResolverRegistry'         # assets
  'workersreg\.New'             # workers — bare `New`, dot escaped so the
                                # sym match doesn't accept `workersregXNew`
                                # as a false-positive
)

bootstrap_symbols="$(git grep -nE \
  'NewRegistry|NewResolverRegistry|workersreg\.New' \
  -- "$bootstrap_rel" || true)"
if [[ -z "$bootstrap_symbols" ]]; then
  if [[ -e "$bootstrap_rel" ]]; then
    printf 'No canonical registry factory referenced from %s\n' \
      "$bootstrap_rel" >&2
    violations=$((violations + 1))
  else
    printf 'bootstrap file %s missing -- rename protocol violated\n' \
      "$bootstrap_rel" >&2
    violations=$((violations + 1))
  fi
else
  for sym in "${canonical_registries[@]}"; do
    if ! grep -q "$sym" <<<"$bootstrap_symbols"; then
      printf 'Canonical registry factory %s missing from %s\n' \
        "$sym" "$bootstrap_rel" >&2
      violations=$((violations + 1))
    fi
  done
fi

# Rule 2: PR-scoped -- no NEW ad-hoc Register/Lookup interfaces.
duplicate_registry_hits="$(scoped_grep \
  '^[[:space:]]*func[[:space:]]+\([A-Za-z0-9_]+[[:space:]]+\*?[A-Za-z0-9_]+\)[[:space:]]+(Register|Lookup)\(' \
  -- 'DataServer/**/*.go' \
  ':!DataServer/internal/outbox/' \
  ':!DataServer/internal/deliveries/' \
  ':!DataServer/internal/workers/' \
  ':!DataServer/internal/assets/' \
  ':!DataServer/internal/store/' \
  ':!DataServer/**/*_test.go' \
  ':!scripts/ci/check-registry.sh' \
  ':!scripts/ci/lib/diff-scope.sh')"

if [[ -n "$duplicate_registry_hits" ]]; then
  printf 'NEW ad-hoc Register/Lookup outside canonical registries:\n%s\n\n' \
    "$duplicate_registry_hits" >&2
  violations=$((violations + 1))
fi

if [[ "$violations" -gt 0 ]]; then
  printf '%d registry violation(s) -- see above\n' "$violations" >&2
  exit 1
fi

echo "check-registry: OK"
