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
    ./frontend_standalone/.github/workflows/*) ;;
    *) printf '  off-root workflow: %s\n' "$workflow" >&2; found_off_root=1 ;;
  esac
done < <(find . \
    -path './.git' -prune -o \
    -type f \( -name '*.yml' -o -name '*.yaml' \) \
    -path '*/workflows/*' -print)
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

# 6. Removed queue package must stay dead.
# After PR "refactor(jobs): remove queue compatibility facade",
# internal/queue was deleted. Reintroducing it would resurrect the
# facade types (queue.Job, queue.QueueItem, queue.JobStatus,
# *queue.FileQueue) that were swept in that PR.
if [[ -d DataServer/internal/queue ]]; then
  fail "DataServer/internal/queue/ exists -- forbidden (queue facade was removed; use internal/jobs instead)"
fi

# 7. BUILD_INFO.json ↔ VERSION.txt SSOT drift guard.
#
# Single-source-of-truth rule (scope: VERSION-LEVEL integrity only).
# This rule catches the headline SSOT invariant: the `version` field in
# RemoteCodex/BUILD_INFO.json must mirror VERSION.txt (prefixed with `v`).
#
# NOT covered by this guard (out of scope, intentionally):
#   * engine_version drift    — `engine_version` is informational for the
#     remote worker; bump it independently when the C++ engine protocol
#     changes. Enforced separately by the worker-image cosign step which
#     tags with the resolved semver.
#   * source_hash drift       — versioned by sha256sum on VERSION.txt;
#     emerges naturally from VERSION.txt edits + ./scripts/generate-build-info.sh.
#   * git_commit drift        — informational only; HEAD at build time.
#   * built_at drift          — derived from SOURCE_DATE_EPOCH or wall clock.
#
# Deepening this guard into a full canonical-shape comparison is
# deliberately deferred: the BUILD_INFO.json file is owned by the worker
# release pipeline (worker-image.yml) and the master image never reads it
# directly, so the only drift class with end-to-end impact is the version
# field. Promote to full-shape check once a producer-side bug surfaces.
if [[ -f RemoteCodex/BUILD_INFO.json ]]; then
  build_info_version="$(python3 -c "import json,sys; print(json.load(open('RemoteCodex/BUILD_INFO.json')).get('version',''))" 2>/dev/null || echo "")"
  version_txt="$(tr -d '[:space:]' < VERSION.txt)"
  expected="v${version_txt}"
  if [[ "$build_info_version" != "$expected" ]]; then
    cat >&2 <<VIOLATION
BUILD_INFO.json version drift:
  RemoteCodex/BUILD_INFO.json   version=${build_info_version}
  VERSION.txt                   VERSION=${version_txt} (expected version=${expected})
Run ./scripts/generate-build-info.sh to regenerate BUILD_INFO.json from VERSION.txt.
VIOLATION
    exit 1
  fi
fi

echo "check-architecture: OK"
