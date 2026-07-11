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
#   * frontend/**             -- extracted to VeloxFrontend repo
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
  # Item 7 (worker state consolidation): removed dead maps + functions.
  'storePendingJob'                   # Removed: dead method (PR #7)
  'takePendingJob'                    # Removed: dead method (PR #7)
  'jobCancelFuncs'                    # Removed: replaced by ActiveTaskExecution.Cancel (PR #7)
  'registerJobCancel'                 # Removed: cancel embedded in ActiveTaskExecution (PR #7)
  'unregisterJobCancel'               # Removed: cancel embedded in ActiveTaskExecution (PR #7)
  # PR #1: dead orchestrator legacy adapter + RecoveryReport protocol removed.
  'recovery_report_v1'                # Removed: heartbeat.extra key (PR #1)
  'recovery_action_v1'                # Removed: ConfigurationUpdate key (PR #1)
  'orchestrator_legacy_adapter'       # Removed: COMPAT adapter (PR #1)
  'orchestratorv1'                    # Removed: legacy DTO package (PR #1)
  'UpsertJobResult'                   # Removed: legacy store mutation (PR #1)
  'OrchestratorJob'                   # Removed: legacy sqlite_queue struct (PR #1)
  'UpsertOrchestratorJob'             # Removed: legacy sqlite_queue method (PR #1)
  'ListOrchestratorJobs'              # Removed: legacy sqlite_queue method (PR #1)
  'GetOrchestratorJob'                # Removed: legacy sqlite_queue method (PR #1)
  'DeleteOrchestratorJob'             # Removed: legacy sqlite_queue method (PR #1)
  'RunPlaybook'                       # Removed: legacy ansible fake executor (PR #1)
  # Item 8 (renderer cleanup): removed legacy engine + fallback paths.
  'CompileLegacyRenderJobParams'      # Removed: legacy render-plan adapter (PR #8)
  'runNativeCxxEngine'                # Removed: --full-video engine launcher (PR #8)
  # Item 11 (duplicate roll-up): removed Handler-level Job helpers.
  'verifyJobOwnership'                # Removed: dead Job-era ownership check (PR #11)
  'lookupJobCASFields'                # Removed: dead Job-era CAS helper (PR #11)
  # Additional legacy removals.
  'submitLegacyJobResult'             # Removed: legacy JobResult submission path
  # Item 9: Job-era message constants removed — must not reappear.
  'MsgJobOffer'                       # Removed: superseded by MsgTaskOffer (PR #9)
  'MsgJobResult'                      # Removed: superseded by MsgTaskResult (PR #9)
  'MsgJobAccepted'                    # Removed: superseded by MsgTaskAccepted (PR #9)
  'MsgJobRejected'                    # Removed: superseded by MsgTaskRejected (PR #9)
  'MsgJobProgress'                    # Removed: dead constant (PR #9)
  'MsgJobLeaseGranted'                # Removed: superseded by MsgTaskLeaseGranted (PR #9)
  'MsgLeaseRenewal'                   # Removed: superseded by MsgTaskLeaseRenewal (PR #9)
)

# Prohibited patterns checked only on the DIFF (forbidden in new/modified code)
diff_patterns=(
  # PR #8: .Create() method on job repository removed — new code must
  # not reintroduce it. os.Create is pre-existing and excluded by diff
  # scope (only new/modified lines trigger).
  '\.Create\('   # Removed repository method (PR #8)
  # Item 7: worker state fields — replaced by activeTasks keyed by taskID.
  # Existing comments/docs still reference the old names; only NEW code
  # (modified/added lines) is flagged.
  '\.activeJobs\b'                    # Replaced by activeTasks (PR #7)
  '\bActiveJob\b'                     # Replaced by ActiveTaskExecution (PR #7)
  # Item 8: --full-video fallback removed — new code must only use --render.
  # C++ engine sources may still reference it (diff-scoped so only new
  # additions in modified files are flagged).
  '--full-video'                      # Legacy engine flag (use --render only)
)

violations=0

# 1. Full-tree checks
#
# Why scripts/ci/operator-history-scrub.sh is excluded from the
# legacy double-root pattern: that script deliberately documents
# the historical PR-5 scrub list, with the literal legacy paths-to-
# scrub embedded in its plan. The substring is load-bearing
# documentation -- operators need to see what to scrub before they
# force-push. Excluding it here keeps the lint honest (a real
# resurrection in any other location still fires) without forbidding
# the historical-context document.
for pattern in "${full_tree_patterns[@]}"; do
  if matches="$(
    git grep -nE "$pattern" -- \
      ':!docs/**' \
      ':!.github/**' \
      ':!deploy/**' \
      ':!DataServer/data/ansible/**' \
      ':!DataServer/internal/handlers/remote/ansible/**' \
      ':!DataServer/internal/store/migrations/**' \
      ':!RemoteCodex/native/video-engine-cpp/CMakeLists.txt' \
      ':!RemoteCodex/native/worker-agent-go/deploy/**' \
      ':!scripts/ci/check-architecture.sh' \
      ':!scripts/ci/verify.sh' \
      ':!scripts/ci/check-no-legacy.sh' \
      ':!scripts/ci/lib/diff-scope.sh' \
      ':!scripts/ci/operator-history-scrub.sh' 2>/dev/null || true
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
#
# NOTE: Do NOT use scoped_grep here — it appends all diff files after
# the provided path, defeating the intent of scoping to one file.
# Check ONLY the Makefile via git diff + git grep so that unrelated
# "dev" string literals in Go source files (config_release_channel_test,
# resource_sampler) are never flagged.
if git diff --name-only --diff-filter=ACMR "$BASE_REF"...HEAD 2>/dev/null \
      | grep -qx 'RemoteCodex/native/worker-agent-go/Makefile' 2>/dev/null; then
  if matches="$(git grep -nE \
       'git describe|\bdev\b["\047]|\bwildcard\s+VERSION' -- \
         RemoteCodex/native/worker-agent-go/Makefile 2>/dev/null \
       || true)"; [[ -n "$matches" ]]; then
    printf 'Worker version fallback is forbidden (new in this PR):\n%s\n\n' \
      "$matches" >&2
    violations=$((violations + 1))
  fi
fi

if [[ "$violations" -gt 0 ]]; then
  printf '%d forbidden legacy pattern(s) introduced in this PR\n' \
    "$violations" >&2
  exit 1
fi

echo "check-no-legacy: OK"
