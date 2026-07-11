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

# Convert a git pathspec to a bash glob suitable for [[ $f == $glob ]].
# Git pathspec rules:
#   - Patterns without '/' match against the filename component only.
#     E.g. '*.go' matches 'foo.go' anywhere in the tree.
#   - Patterns with '/' match against the full path from repo root.
#     E.g. 'DataServer/**/*.go' matches files under DataServer/.
#   - '**' matches across directory boundaries.
#
# Bash glob with globstar:
#   - '**' matches across directory boundaries (same as git pathspec).
#   - '*.go' only matches in the current directory — NOT at any depth.
#     To match at any depth we prepend '**/'.
_pathspec_to_bash_glob() {
  local p="$1"
  if [[ "$p" != */* ]]; then
    # Bare filename pattern — match at any depth (git behaviour).
    printf '**/%s' "$p"
  else
    printf '%s' "$p"
  fi
}

# scoped_grep PATTERN -- [':!exclude' | include ...]
#
# Works like `git grep -nE PATTERN -- <pathspecs>` but ONLY against
# files changed since BASE_REF...HEAD.  Exclusion patterns are properly
# applied to the filtered file list — unlike the previous
# implementation which appended all diff files as explicit paths
# (bypassing git grep's pathspec exclude rules).
scoped_grep() {
  local pattern="$1"
  shift
  if [[ $# -gt 0 && "$1" == "--" ]]; then
    shift
  fi

  fail_if_no_base_ref

  local -a changed_files
  mapfile -t changed_files < <(
    git diff --name-only --diff-filter=ACMR "$BASE_REF"...HEAD 2>/dev/null \
      || true
  )

  if [[ ${#changed_files[@]} -eq 0 ]]; then
    return 0
  fi

  # Separate include pathspecs from exclude (':!...') pathspecs.
  # Then pre-convert ALL pathspecs to bash globs once (O(N+M)).
  local includes=() excludes=() inc_globs=() exc_globs=() filtered=()
  local a
  for a in "$@"; do
    if [[ "$a" == ':!'* ]]; then
      excludes+=("${a#:!}")
    else
      includes+=("$a")
    fi
  done
  local inc
  for inc in "${includes[@]}"; do
    inc_globs+=("$(_pathspec_to_bash_glob "$inc")")
  done
  local exc
  for exc in "${excludes[@]}"; do
    exc_globs+=("$(_pathspec_to_bash_glob "$exc")")
  done

  # Enable globstar so that ** in bash matches across directories
  # (same semantics as git pathspec's **).
  shopt -s globstar 2>/dev/null || true

  local f
  for f in "${changed_files[@]}"; do
    # If includes are specified, file must match at least one.
    if [[ ${#inc_globs[@]} -gt 0 ]]; then
      local in=0 g
      for g in "${inc_globs[@]}"; do
        [[ "$f" == $g ]] && { in=1; break; }
      done
      [[ $in -eq 0 ]] && continue
    fi
    # Drop if file matches any exclude.
    local skip=0
    for g in "${exc_globs[@]}"; do
      [[ "$f" == $g ]] && { skip=1; break; }
    done
    [[ $skip -eq 0 ]] && filtered+=("$f")
  done

  shopt -u globstar 2>/dev/null || true

  if [[ ${#filtered[@]} -eq 0 ]]; then
    return 0
  fi

  git grep -nE "$pattern" -- "${filtered[@]}" 2>/dev/null || true
}
