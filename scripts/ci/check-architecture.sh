#!/usr/bin/env bash
# scripts/ci/check-architecture.sh
#
# Structural invariants of the repository: this guards the SHAPE of the
# codebase, not its behaviour. Adding new rules here is encouraged; adding
# behaviour tests belongs elsewhere (Go test, ansible-lint, etc).
#
# Exit codes: 0 ok -- 1 violation detected.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

fail() {
  printf 'ARCHITECTURE ERROR: %s\n' "$*" >&2
  exit 1
}

# 1. The pre-restructure double-root must stay dead.
# If a contributor resurrects refactored/ (e.g. via
# `git mv DataServer refactored/DataServer`), every package import path
# drifts and builds break silently. Catch the resurrection at PR-time.
# We use `[ -e ]` directly so the failure path is unambiguous if someone
# hides refactored/ behind a symlink later.
if [[ -e refactored || -L refactored ]]; then
  ls -la refactored >&2 || true
  fail "refactored/ exists -- forbidden (single-root rule)"
fi

# 2. Exactly one VERSION.txt at project root.
version_count="$(
  find . \
    -path './.git' -prune -o \
    -name VERSION.txt -print | wc -l | tr -d ' '
)"
[[ "$version_count" == "1" ]] \
  || fail "expected exactly one VERSION.txt at root, found $version_count"

# 3. Exactly one shared/go.mod.
shared_count="$(
  find . \
    -path './.git' -prune -o \
    -path '*/shared/go.mod' -print | wc -l | tr -d ' '
)"
[[ "$shared_count" == "1" ]] \
  || fail "expected exactly one shared/go.mod, found $shared_count"

# 4. All GitHub workflow YAML lives under ./.github/workflows/.
# We anchor on `./.github/workflows/*` (NOT `*/workflows/*`) so that no
# stray `foo/workflows/stage.yml` from another tool slipped in.
found_off_root=0
while IFS= read -r workflow; do
  case "$workflow" in
    ./.github/workflows/*) ;;
    *) printf '  off-root workflow: %s\n' "$workflow" >&2; found_off_root=1 ;;
  esac
done < <(find . \
    -path './.git' -prune -o \
    -type f \( -name '*.yml' -o -name '*.yaml' \) \
    -path './.github/workflows/*' -print)
[[ "$found_off_root" -eq 0 ]] \
  || fail "workflow YAML files outside ./.github/workflows/ -- see above"

# 5. No *_legacy / *_old / *.deprecated files anywhere.
if find . \
     -path './.git' -prune -o \
     -type f \( \
       -iname '*.deprecated' -o \
       -iname '*_legacy.*'  -o \
       -iname '*_old.*' \
     \) -print -quit | grep -q .; then
  find . \
    -path './.git' -prune -o \
    -type f \( \
      -iname '*.deprecated' -o \
      -iname '*_legacy.*'  -o \
      -iname '*_old.*' \
    \) -print >&2
  fail "legacy/deprecated files are forbidden -- see above"
fi

echo "check-architecture: OK"
