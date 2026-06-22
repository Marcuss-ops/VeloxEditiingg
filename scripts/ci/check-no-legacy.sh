#!/usr/bin/env bash
# scripts/ci/check-no-legacy.sh
#
# Blacklist of CONCRETE forbidden symbols. These are not patterns to be
# loosely interpreted -- each entry is a removed-alias / removed-flag /
# removed-symbol from a completed restructure. Resurrecting any of
# them would reintroduce parallel-write or fallback drift.
#
# Issue #10: Most patterns are full-tree checks (git grep against the
# entire repository). Diff-scoped patterns catch method-call regressions
# that overlap with legitimate pre-existing uses (e.g. \.Create\().
# Historical references in CI workflows, deploy templates, and build
# configs are explicitly excluded — new regressions must not appear
# anywhere else.
#
# Excluded by default (still off-limits within the diff):
#   * docs/**                 -- historical documentation (intentional)
#   * frontend_standalone/**  -- sealed frontend bundle
#   * .github/**              -- CI workflows referencing old paths
#   * deploy/**               -- deploy templates with old env var names
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
  # PR #8: workflow package removed — write method calls must not reappear.
  # Matches workflow.Repository method invocations (workflow.CreateRun, etc.)
  'velox-server/internal/workflow'    # Removed package import (PR #8)
  'workflow\.CreateRun'               # workflow.Repository.CreateRun
  'workflow\.MarkStepRunning'         # workflow.Repository.MarkStepRunning
  'workflow\.CompleteStep'            # workflow.Repository.CompleteStep
  'workflow\.FailStep'                # workflow.Repository.FailStep
  'workflow\.CancelRun'               # workflow.Repository.CancelRun
  # WriteEnabled flag removed — must not reappear.
  'WriteEnabled'
  # Ownership markers — all issues must be resolved, not deferred.
  'TO-BE-OPENED'
  # PR #2: legacy no-op outbox handlers removed.
  'StepReadyHandler'
  'JobSucceededHandler'
  'ArtifactReadyHandler'
  'DeliveryCreatedHandler'
  # PR #6: _requirements removed from JSON blobs; must not reappear
  # as a JSON sub-object key in read/write paths.
  '"_requirements"'                  # JSON sub-object key (dedicated columns only)
  # PR #8: CreateJobParams removed — must not reappear in any form.
  # Canonical creation is now AtomicJobTaskCreator.CreateJobWithTask.
  'CreateJobParams'                   # Removed struct + usage (PR #8)
  # PR #9: toStoreParams helper removed — dead code after CreateJob dropped.
  'func toStoreParams'                # Removed function definition (PR #9)
  # PR #9: testSubmitQueue adapter removed — use AtomicJobTaskCreator + NewEnqueuer.
  'type testSubmitQueue'              # Removed adapter struct definition (PR #9)
  # De-legacy migration stubs — must never reappear in production code.
  'NewLegacy'                         # Stub from de-legacy migration
  'DeprecatedService'                 # Same
  'local-workers\.sh\.deprecated'     # Replaced by data/ansible
)

# Prohibited patterns checked only on the DIFF (forbidden in new/modified code)
diff_patterns=(
  # PR #8: .Create() method on job repository removed — new code must
  # not reintroduce it. os.Create is pre-existing and excluded by diff
  # scope (only new/modified lines trigger).
  '\.Create\('   # Removed repository method (PR #8)
)

violations=0

# 1. Full-tree checks
for pattern in "${full_tree_patterns[@]}"; do
  if matches="$(
         git grep -nE "$pattern" -- \
           ':!docs/**' \
           ':!frontend_standalone/**' \
           ':!.github/**' \
           ':!deploy/**' \
           ':!DataServer/data/ansible/**' \
           ':!RemoteCodex/native/video-engine-cpp/CMakeLists.txt' \
           ':!RemoteCodex/native/worker-agent-go/deploy/**' \
           ':!RemoteCodex/native/worker-agent-go/velox-worker-agent' \
           ':!scripts/ci/check-architecture.sh' \
           ':!scripts/ci/verify.sh' \
           ':!scripts/ci/check-no-legacy.sh' \
           ':!scripts/ci/lib/diff-scope.sh' \
           ':!DataServer/server.exe' 2>/dev/null || true
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
           ':!scripts/ci/lib/diff-scope.sh' \
           ':!DataServer/server.exe'
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
