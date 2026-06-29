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
#      referenced from the assembly package `DataServer/cmd/server/`
#      (excluding `*_test.go`). Phase-1 refactors split bootstrap.go
#      into multiple helpers (buildWorkers, buildAssets, ...); the
#      canonical factory MUST appear somewhere in this package, not
#      only in the entrypoint file. Test files are excluded so
#      package-level test setup doesn't satisfy Rule 1 on its own.
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
# Earlier v3 hard-coded bootstrap_rel=DataServer/cmd/server/bootstrap.go;
# the rule was widened to the assembly package directory to allow the
# wiring to spread across helpers (buildWorkers, buildAssets, ...) without
# re-introducing cargo-cult references into bootstrap.go itself.
set -euo pipefail
# shopt -s nullglob: harden any future glob expansion in this script.
# Without it, an empty glob (e.g. an accidentally-empty list of
# bootstrap*.go helper files) would expand to the literal pattern,
# which `git grep -- "$pattern"` would then treat as an invalid
# pathpec and produce a confusing error. With nullglob, missing
# globs expand to nothing — cleaner failure mode and zero risk of
# argument-quoting surprises.
shopt -s nullglob

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"
source "$(dirname "${BASH_SOURCE[0]}")/lib/diff-scope.sh"

fail() { printf 'REGISTRY ERROR: %s\n' "$*" >&2; exit 1; }
violations=0

# Canonical-assembly package directory. Phase-1 refactors split
# bootstrap.go into multiple helpers (buildWorkers, buildAssets, ...);
# the canonical factory MUST appear somewhere in this package, not
# only in the entrypoint file.
bootstrap_pkg_dir="DataServer/cmd/server"
# Kept for backwards-compat rename-protocol diagnostics (see below).
bootstrap_rel="${bootstrap_pkg_dir}/bootstrap.go"

# Rule 1: canonical registry factories MUST appear in the assembly package.
canonical_registries=(
  'NewRegistry'                 # outbox + deliveries
  'NewResolverRegistry'         # assets
  'workersreg\.New'             # workers — bare `New`, dot escaped so the
                                # sym match doesn't accept `workersregXNew`
                                # as a false-positive
)

# Hardening: drop lines whose content is a comment-only mention of a
# canonical factory symbol. git grep emits `path:lineno:content`. We
# rejoin $3..$NF to recover any colons-in-content, then trim leading
# whitespace. Lines starting with `//`, `/*`, or `*` are dropped —
# they would falsely satisfy Rule 1 even though the symbol isn't
# actually wired up. Real code lines — even those with a trailing
# `// ...` comment — are preserved.
bootstrap_symbols="$(
  git grep -nE \
    'NewRegistry|NewResolverRegistry|workersreg\.New' \
    -- "$bootstrap_pkg_dir" \
    ":!${bootstrap_pkg_dir}/*_test.go" \
    2>/dev/null |
  awk -F: '
    NF < 3 { print; next }
    {
      content = $3
      for (i = 4; i <= NF; i++) content = content ":" $i
      sub(/^[[:space:]]+/, "", content)
      if (content ~ /^(\/\/|\*|\/\*)/) next
      print
    }
  ' || true
)"
if [[ -z "$bootstrap_symbols" ]]; then
  if [[ -d "$bootstrap_pkg_dir" ]]; then
    printf 'No canonical registry factory referenced from %s\n' \
      "$bootstrap_pkg_dir" >&2
    violations=$((violations + 1))
  else
    printf 'bootstrap package %s missing -- rename protocol violated\n' \
      "$bootstrap_pkg_dir" >&2
    violations=$((violations + 1))
  fi
else
  for sym in "${canonical_registries[@]}"; do
    if ! grep -q "$sym" <<<"$bootstrap_symbols"; then
      printf 'Canonical registry factory %s missing from %s\n' \
        "$sym" "$bootstrap_pkg_dir" >&2
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
  ':!DataServer/cmd/server/**' \
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
