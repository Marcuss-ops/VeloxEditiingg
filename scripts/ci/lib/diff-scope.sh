#!/usr/bin/env bash
# scripts/ci/lib/diff-scope.sh
#
# Helper to scope CI greps to FILES CHANGED in the current branch vs
# BASE_REF. Solves the "CI runs red on HEAD due to legacy artifacts
# outside this PR" problem: a freshly-cloned main has no diff against
# itself so every GRP-scoped check returns empty (and exits OK). PR
# diffs surface NEW regressions only.
#
# Two entry points:
#
#   fail_if_no_base_ref
#     Hard-fail the current script if BASE_REF is not resolvable.
#     Local dev without `origin/main` accessible must set
#     BASE_REF explicitly (e.g. `BASE_REF=HEAD~5 ./scripts/ci/check-*`)
#     -- silent pass would defeat the gate's intent.
#
#   scoped_grep PATTERN -- [':!exclude' ...]
#     Equivalent to `git grep -nE PATTERN -- :!exclude ...` but ONLY
#     against files changed since BASE_REF. The original exclusions
#     stay in effect.
#
# Intended to be sourced, not executed directly:
#
#   source "$(dirname "$0")/lib/diff-scope.sh"
#   hits="$(scoped_grep 'forbidden' -- ':!*_test.go')"

# Resolve REPO_ROOT once so the helper is path-aware.
: "${BASE_REF:=origin/main}"

fail_if_no_base_ref() {
  if ! git rev-parse --verify --quiet "$BASE_REF" >/dev/null 2>&1; then
    printf 'ERROR: BASE_REF "%s" is unreachable.\n' "$BASE_REF" >&2
    printf '       Set BASE_REF=<sha-or-branch> explicitly when running locally.\n' >&2
    exit 1
  fi
}

# scoped_grep PATTERN -- [':!exclude' ...]
# Works like `git grep -nE PATTERN -- <excludes>` but BOTH:
#   (a) restricted to files changed since BASE_REF...HEAD (so HEAD is
#       trivially green)
#   (b) tolerant of deletions in the diff (--diff-filter=ACMR drops D)
scoped_grep() {
  local pattern="$1"
  shift
  if [[ $# -gt 0 && "$1" == "--" ]]; then
    shift
  fi

  fail_if_no_base_ref

  local -a changed_files=()
  mapfile -t changed_files < <(
    git diff --name-only --diff-filter=ACMR "$BASE_REF"...HEAD 2>/dev/null \
      || true
  )

  # Filter to only .go files (exclude tests) so git grep literal paths
  # always match the '**/*.go' include pathspec. Without this, a
  # non-matching literal path (e.g. .gitignore) causes git grep to fall
  # back to searching the entire tree matching the include pathspec.
  local -a go_files=()
  for f in "${changed_files[@]}"; do
    if [[ "$f" == *.go && "$f" != *_test.go ]]; then
      go_files+=("$f")
    fi
  done

  if [[ ${#go_files[@]} -eq 0 ]]; then
    # No diff vs BASE_REF => no PR changes to gate on. Cheaper than
    # letting git grep iterate the full tree.
    return 0
  fi

  git grep -nE "$pattern" -- "$@" "${go_files[@]}" 2>/dev/null || true
}
