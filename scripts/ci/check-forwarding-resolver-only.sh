#!/usr/bin/env bash
# scripts/ci/check-forwarding-resolver-only.sh
#
# Enforce the Area 3 architectural invariant: the ONLY path from a
# completed remote result to the Velox job queue is
# creatorflow.Resolver.Resolve(). This check prevents regressions where
# a contributor reintroduces:
#
#   1. Direct Enqueue() / EnqueueNow() calls from the forwarding runner
#      or pipeline handlers (bypassing the Resolver's idempotency +
#      deterministic job-id + atomic forwarding+enqueue).
#   2. Manual construction of jobs.Job{} or taskgraph.TaskSpec{} in the
#      forwarding or pipeline packages (the Resolver owns Job+Task
#      creation via store.AtomicForwardAndEnqueue / enqueuer.Enqueue).
#   3. Calls to enqueue.BuildPipelinePayload outside
#      creatorflow/resolver.go (the payload normalisation is owned by
#      the Resolver so every forwarded result passes through the same
#      canonical shape).
#   4. Non-canonical job-ID derivation functions (e.g. deriveJobID)
#      introduced in the forwarding or pipeline packages. The forwarding
#      path MUST use enqueue.DeriveForwardingJobID (via the Resolver).
#      The creatorflow/service.go deriveJobID function is for the
#      CreateJobWithPlan path (Fase 2) and is confined to creatorflow/.
#
# Approach: "forbidden zone" scanning via awk filtering. We search all
# changed files in the PR diff (no include pathspecs, which avoids the
# _pathspec_to_bash_glob directory-vs-file mismatch), then filter the
# hits with awk to only keep matches in the FORBIDDEN directories
# (forwarding/ and handlers/server/pipeline/). Every hit in a forbidden
# directory is a violation — no allowlist needed. This mirrors the
# check-single-writer.sh scan() pattern but in reverse: we keep hits IN
# the forbidden zones instead of excluding them.
#
# Scope: non-test Go files in the forwarding and pipeline handler
# packages. The check uses scoped_grep so only NEW regressions in the
# current PR are surfaced (main is trivially green).
#
# Exit codes: 0 ok -- 1 violation detected.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"
source "$(dirname "${BASH_SOURCE[0]}")/lib/diff-scope.sh"

fail() { printf 'FORWARDING-RESOLVER-ONLY ERROR: %s\n' "$*" >&2; exit 1; }
violations=0

# scan_forbidden PATTERN DIR1 [DIR2 ...]
#
# Search all PR-diff-changed files for PATTERN, then filter results to
# only keep hits whose file path starts with one of the DIR prefixes.
# Every remaining hit is a violation — the directories are "forbidden
# zones" for this pattern.
#
# This approach avoids the _pathspec_to_bash_glob directory-vs-file
# mismatch that would make include-based filtering a no-op: we search
# all changed files (no includes) and use awk to match file paths
# against directory prefixes, exactly like check-single-writer.sh's
# scan() function.
scan_forbidden() {
  local pattern="$1"
  shift

  local hits
  hits="$(scoped_grep "$pattern" -- \
            ':!*_test.go' \
            ':!scripts/ci/check-forwarding-resolver-only.sh')"
  [[ -z "$hits" ]] && return 0

  # Build an awk regex from the forbidden directory prefixes:
  #   ^(dir1|dir2)
  local dir_regex=""
  local first=1
  local d
  for d in "$@"; do
    if [[ $first -eq 1 ]]; then
      dir_regex="^${d}"
      first=0
    else
      dir_regex="${dir_regex}|^${d}"
    fi
  done

  local forbidden_hits
  forbidden_hits="$(printf '%s\n' "$hits" \
                      | awk -F: -v regex="$dir_regex" '$1 ~ regex { print }' \
                      || true)"
  if [[ -n "$forbidden_hits" ]]; then
    printf 'NEW pattern "%s" found in FORBIDDEN zone (forwarding/pipeline must use Resolver.Resolve only):\n%s\n\n' \
      "$pattern" "$forbidden_hits" >&2
    violations=$((violations + 1))
  fi
}

# Forbidden directories: the forwarding runner and the pipeline HTTP
# handlers. These packages must route every remote-result-to-worker
# handoff through creatorflow.Resolver.Resolve — never directly to
# Enqueue, Job/TaskSpec construction, or BuildPipelinePayload.
FWD_DIR="DataServer/internal/forwarding/"
PIPE_DIR="DataServer/internal/handlers/server/pipeline/"

# ── Rule 1: No direct Enqueue() / EnqueueNow() in forwarding/pipeline ───
# The forwarding runner and pipeline handlers must route through
# Resolver.Resolve, which internally calls enqueuer.Enqueue. A direct
# call bypasses idempotency pre-check, deterministic job-id derivation,
# forwarding-key injection, and the atomic forwarding+enqueue tx.
#
# NOTE: jobs/enqueue/ legitimately defines and internally calls Enqueue;
# store/ and creatorflow/ are also allowed. By filtering to only the
# forbidden directories, we avoid false positives from those packages.
scan_forbidden '\.Enqueue\('    "$FWD_DIR" "$PIPE_DIR"
scan_forbidden '\.EnqueueNow\(' "$FWD_DIR" "$PIPE_DIR"

# ── Rule 2: No manual Job/TaskSpec construction in forwarding/pipeline ──
# jobs.Job{} and taskgraph.TaskSpec{} literals must only appear in
# creatorflow/ (which owns CreateJobWithPlan) and store/ (which owns
# AtomicJobTaskCreator). The forwarding runner and pipeline handlers
# must NEVER construct these directly — doing so bypasses the atomic
# creator and the single-writer principle.
#
# NOTE: store/atomic_job_task.go and creatorflow/service.go legitimately
# construct these. By filtering to only the forbidden directories, we
# avoid false positives from those packages.
scan_forbidden 'jobs\.Job\{'           "$FWD_DIR" "$PIPE_DIR"
scan_forbidden 'taskgraph\.TaskSpec\{' "$FWD_DIR" "$PIPE_DIR"

# ── Rule 3: BuildPipelinePayload only in creatorflow/resolver.go ────────
# The payload normalisation (flatten result envelope, extract
# script/scenes/voiceover, inject delivery_plan) is owned by the
# Resolver so every forwarded result passes through the same canonical
# shape. Calling BuildPipelinePayload from the forwarding runner or
# pipeline handlers duplicates the normalisation and can drift.
#
# NOTE: jobs/enqueue/enqueue_pipeline.go defines BuildPipelinePayload;
# creatorflow/resolver.go calls it. Both are outside the forbidden
# directories, so no false positives.
scan_forbidden 'BuildPipelinePayload\(' "$FWD_DIR" "$PIPE_DIR"

# ── Rule 4: No non-canonical job-ID derivation in forwarding/pipeline ───
# The forwarding path MUST use enqueue.DeriveForwardingJobID (via the
# Resolver). The creatorflow/service.go deriveJobID function is a
# SEPARATE algorithm for the CreateJobWithPlan path (Fase 2) and must
# NOT leak into the forwarding or pipeline packages.
#
# We check for the deriveJobID function name (the non-forwarding
# algorithm) appearing in the forbidden directories. If someone copies
# it into the forwarding runner or pipeline handlers, that's a clear
# violation of the single-algorithm rule.
#
# NOTE: deriveJobID is a package-private function in creatorflow/, so
# it cannot be called from outside that package. But a contributor
# could copy-paste the function body, so we check for the name pattern.
scan_forbidden 'deriveJobID\(' "$FWD_DIR" "$PIPE_DIR"

if [[ "$violations" -gt 0 ]]; then
  printf '%d forwarding-resolver-only violation(s) -- see above\n' \
    "$violations" >&2
  printf '\nArchitectural rule (Area 3):\n' >&2
  printf '  The ONLY path from a completed remote result to the Velox\n' >&2
  printf '  job queue is creatorflow.Resolver.Resolve().\n' >&2
  printf '  Forbidden: direct Enqueue() from polling, manual Job/TaskSpec\n' >&2
  printf '  construction, duplicated payload normalisation, multiple\n' >&2
  printf '  job-ID derivation algorithms.\n' >&2
  exit 1
fi

echo "check-forwarding-resolver-only: OK"
