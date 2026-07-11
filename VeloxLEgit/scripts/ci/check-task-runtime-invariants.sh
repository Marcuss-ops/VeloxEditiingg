#!/usr/bin/env bash
# scripts/ci/check-task-runtime-invariants.sh
#
# Grep-based CI guards for the TASK runtime lifecycle. Each guard is a
# direct assertion of a single architectural invariant and fails CI on
# regression. See docs/architecture/OWNERSHIP.md for the canonical map
# and docs/architecture/legacy-cutover-followups/README.md for the PR
# trail (PR-01..PR-16) that established them.
#
# Invariants asserted:
#   (a) The ONLY writer of jobs.status='SUCCEEDED' is
#       artifacts/sqlite_finalization_repository.go (FinalizeVerified).
#       (PR-01 / Fase 3.5-a: FinalizationRepository is the single
#       SQL transaction across jobs + artifacts + outbox.)
#   (b) PENDING->AWAITING_ARTIFACT transition happens ONLY through
#       maybeTransitionJob -- the two canonical implementations
#       (internal/taskingestion/service.go + internal/grpcserver/handler_jobs.go).
#       (PR-02: pre-finalization gate.)
#   (c) handleTaskResult does NOT trust the wire-provided JobId as
#       authoritative; the worker_id + task_id + attempt_id triple is
#       used to look up the canonical job_id via taskattempts identity
#       validation, with tr.GetJobId() only as a FALLBACK. New callers
#       of GetJobId() outside the validated handler are forbidden.
#       (PR-02 / PR-04 §9.5: closes the desync surface in handleTaskResult.)
#   (d) Exactly ONE reaper is registered for task-lease expiry --
#       DataServer/cmd/server/bootstrap_tasks.go. The reaper struct
#       definition lives in internal/taskgraph/reaper.go but only the
#       bootstrap caller should instantiate it. (PR-05: TaskLeaseReaper
#       extracted as a named runner.)
#   (e) New task-pipeline migration SQL files use
#       STRFTIME('%Y-%m-%dT%H:%M:%SZ', 'now', ...) for any timestamp
#       default, NEVER datetime('now') or CURRENT_TIMESTAMP. (PR-09 /
#       PR-15: tasks / payload-V2 single source of truth -- timestamp
#       canonicalization for downstream ordering + dump-compat with the
#       workers.)
#
# NOTE: These guards are stricter and more task-runtime-specific than
# the existing check-single-writer.sh. They are intentionally a
# SECOND layer of defence, not a replacement. The single-writer check
# still flags job.status='SUCCEEDED' SQL writes via the SQL layer;
# this guard replicates it as a per-task-runtime invariant and adds
# four additional guards (b/c/d/e).
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"
source "$(dirname "${BASH_SOURCE[0]}")/lib/diff-scope.sh"

fail() { printf 'TASK-RUNTIME-INVARIANT ERROR: %s\n' "$*" >&2; exit 1; }
violations=0

# Shared pathspec excludes for all guards. Test files, declared enums +
# transition matrix files, and our own helper scripts are out of scope.
COMMON_EXCLUDES=(
  ':!*_test.go'
  ':!**/*status*.go'
  ':!**/transitions*.go'
  ':!docs/**'
  ':!scripts/ci/check-task-runtime-invariants.sh'
  ':!scripts/ci/lib/diff-scope.sh'
)

# ─────────────────────────────────────────────────────────────────────────────
# (a) SUCCEEDED writer is ONLY artifacts/sqlite_finalization_repository.go
# ─────────────────────────────────────────────────────────────────────────────
# Match the existing check-single-writer.sh style: SQL UPDATE/INSERT +
# method calls. The bare 'SUCCEEDED' string check is intentionally NOT
# used -- it would flag every enum declaration
# (StatusSucceeded = "SUCCEEDED") across the codebase, which is
# allowed. Writers are:
#   - SQL: UPDATE jobs ... / INSERT INTO jobs ... with SUCCEEDED as
#     status literal
#   - Go: .MarkSucceeded(...), jobs.UpdateJobStatus(... StatusSucceeded),
#     FinalizeVerified( -- whose sole non-test caller is the canonical repo
guard_a_succeeded_writer() {
  local writer_file='DataServer/internal/artifacts/sqlite_finalization_repository.go'
  # Match `UPDATE[ OR REPLACE][ INTO] jobs[ OR REPLACE] SET ... SUCCEEDED`
  #   and the INSERT equivalent. The OR REPLACE modifier is attached
  #   ONCE per clause (before INTO and before SET) to avoid the
  #   mis-nested trap where `UPDATE OR REPLACE INTO jobs SET ...`
  #   skips the second OR-clause.
  local pattern='UPDATE\s+(?:OR\s+REPLACE\s+)?(?:INTO\s+)?jobs\s+(?:OR\s+REPLACE\s+)?SET\s+[^;]*SUCCEEDED|INSERT\s+(?:OR\s+REPLACE\s+)?INTO\s+jobs\s+[^;]*SUCCEEDED|\.MarkSucceeded\s*\(|\.FinalizeVerified\s*\(|\.UpdateJobStatus\s*\([^)]*StatusSucceeded'
  local hits
  hits="$(scoped_grep "$pattern" -- "${COMMON_EXCLUDES[@]}" ':!*migrations/**')"
  [[ -z "$hits" ]] && return 0

  local disallowed
  disallowed="$(printf '%s\n' "$hits" \
                  | awk -F: -v writer="$writer_file" \
                    '$1 !~ "^"writer { print }' || true)"
  if [[ -n "$disallowed" ]]; then
    printf 'INVARIANT (a) -- SUCCEEDED writer found outside %s:\n%s\n\n' \
      "$writer_file" "$disallowed" >&2
    violations=$((violations + 1))
  fi
}

# ─────────────────────────────────────────────────────────────────────────────
# (b) PENDING->AWAITING_ARTIFACT transition ONLY via maybeTransitionJob
# ─────────────────────────────────────────────────────────────────────────────
# The two canonical implementations are
#   internal/taskingestion/service.go (TaskReportIngestionService.maybeTransitionJob)
#   internal/grpcserver/handler_jobs.go  (Handler.maybeTransitionJob, fire-and-forget goroutine)
# Enum declaration lives in internal/jobs/status.go and the transition
# matrix in internal/jobs/transitions.go -- both excluded from the grep
# by COMMON_EXCLUDES. A new WRITE of AWAITING_ARTIFACT in any other
# file is a regression.
guard_b_awaiting_artifact_writer() {
  local allowed_files='DataServer/internal/taskingestion/service.go|DataServer/internal/grpcserver/handler_jobs.go'
  # Same nesting discipline as guard (a): OR REPLACE once per clause.
  local pattern='UPDATE\s+(?:OR\s+REPLACE\s+)?(?:INTO\s+)?jobs\s+(?:OR\s+REPLACE\s+)?SET\s+[^;]*AWAITING_ARTIFACT|INSERT\s+(?:OR\s+REPLACE\s+)?INTO\s+jobs\s+[^;]*AWAITING_ARTIFACT|\.UpdateJobStatus\s*\([^)]*StatusAwaitingArtifact|\.Status\s*=\s*jobs\.StatusAwaitingArtifact'
  local hits
  hits="$(scoped_grep "$pattern" -- "${COMMON_EXCLUDES[@]}" ':!*migrations/**')"
  [[ -z "$hits" ]] && return 0

  local disallowed
  disallowed="$(printf '%s\n' "$hits" \
                  | awk -F: -v allowed="$allowed_files" \
                    '$1 !~ "^("allowed")" { print }' || true)"
  if [[ -n "$disallowed" ]]; then
    printf 'INVARIANT (b) -- AWAITING_ARTIFACT writer found outside the two maybeTransitionJob sites:\n%s\n\n' \
      "$disallowed" >&2
    violations=$((violations + 1))
  fi
}

# ─────────────────────────────────────────────────────────────────────────────
# (c) handleTaskResult does NOT trust wire JobId as authoritative
# ─────────────────────────────────────────────────────────────────────────────
# The ONLY place a TaskResult message's JobId is read is the validated
# handler at internal/grpcserver/handler_jobs.go (handleTaskResult body)
# -- and any new caller of tr.GetJobId() / tr.JobId must be inside that
# file or inside the validated wire-fallback path
# (internal/taskattempts/repository.go identity validation). A new
# writer that treats the wire JobId as authoritative without the
# worker + task + attempt triple lookup is a desync-bug regression.
guard_c_handleTaskResult_wireJobId() {
  local allowed_files='DataServer/internal/grpcserver/handler_jobs.go|DataServer/internal/taskattempts/repository.go'
  local pattern='\btr\.GetJobId\s*\(|\btr\.JobId\s'
  local hits
  # tr.JobId / tr.GetJobId() are NOT recognized in `status*.go` or
  # transitions tests -- use the bare COMMON_EXCLUDES set, no need for
  # extra filter on `status.go` here.
  hits="$(scoped_grep "$pattern" -- \
            ':!*_test.go' \
            ':!docs/**' \
            ':!scripts/ci/check-task-runtime-invariants.sh' \
            ':!scripts/ci/lib/diff-scope.sh')"
  [[ -z "$hits" ]] && return 0

  local disallowed
  disallowed="$(printf '%s\n' "$hits" \
                  | awk -F: -v allowed="$allowed_files" \
                    '$1 !~ "^("allowed")" { print }' || true)"
  if [[ -n "$disallowed" ]]; then
    printf 'INVARIANT (c) -- handleTaskResult wire JobId reference outside the validated handler/wire-fallback path:\n%s\n\n' \
      "$disallowed" >&2
    violations=$((violations + 1))
  fi
}

# ─────────────────────────────────────────────────────────────────────────────
# (d) EXACTLY ONE reaper registers for task-lease expiry
# ─────────────────────────────────────────────────────────────────────────────
# STRUCTURAL invariant -- not diff-scoped. The exact contract is "1
# constructor invocation in the entire repo, outside the definition
# itself". Diff-scoping this would silently accept a deleted-then-
# re-added registrant across PRs. Full-tree check.
guard_d_one_lease_reaper() {
  local hits
  hits="$(git grep -nE 'NewTaskLeaseReaper\s*\(' \
            -- \
            ':!*_test.go' \
            ':!docs/**' \
            ':!scripts/ci/check-task-runtime-invariants.sh' \
            ':!scripts/ci/lib/diff-scope.sh' \
            ':!DataServer/internal/taskgraph/reaper.go' \
            ':!DataServer/internal/taskgraph/reaper_test.go' \
            2>/dev/null || true)"
  local count
  count="$(printf '%s\n' "$hits" | awk '$0!=""' | wc -l | tr -d ' ')"

  # Allow exactly one (current: cmd/server/bootstrap_tasks.go).
  if [[ "$count" -ne 1 ]]; then
    printf 'INVARIANT (d) -- expected EXACTLY 1 NewTaskLeaseReaper() registration, found %d:\n%s\n\n' \
      "$count" "$hits" >&2
    violations=$((violations + 1))
  fi
}

# ─────────────────────────────────────────────────────────────────────────────
# (e) Task-pipeline migration SQL uses STRFTIME, not DATETIME
# ─────────────────────────────────────────────────────────────────────────────
# Task-pipeline migration files (anything under
# DataServer/internal/store/migrations/ matching *task* / *tasks*)
# must use STRFTIME('%Y-%m-%dT%H:%M:%SZ', 'now', ...) for any
# timestamp DEFAULT or expression. datetime('now') and CURRENT_TIMESTAMP
# leak timezone-naive strings + break SQLite dump compatibility with
# the worker's RFC3339 parser.
#
# PR-scoped: only NEW files introduced since BASE_REF are scanned, so
# baseline migrations grandfather. Existing files use TEXT NOT NULL
# without a default, which is also fine.
#
# Case-insensitive (real migrations use uppercase DATETIME).
guard_e_task_migration_strftime() {
  fail_if_no_base_ref
  local -a changed_files
  mapfile -t changed_files < <(
    git diff --name-only --diff-filter=ACMR "$BASE_REF"...HEAD 2>/dev/null \
      | grep -E 'DataServer/internal/store/migrations/.*tasks?\.(sql)$' \
      || true
  )
  [[ ${#changed_files[@]} -eq 0 ]] && return 0

  # Match either datetime/NOW/CURRENT_TIMESTAMP (case-insensitive).
  local sqlite_hits
  sqlite_hits="$(git grep -niE "datetime\s*\(\s*['\"]now['\"]\s*\)|CURRENT_TIMESTAMP" \
                  -- "${changed_files[@]}" 2>/dev/null || true)"
  if [[ -n "$sqlite_hits" ]]; then
    printf 'INVARIANT (e) -- new task-pipeline migration uses datetime()/CURRENT_TIMESTAMP; replace with STRFTIME(%%Y-%%m-%%dT%%H:%%M:%%SZ, '\''now'\'', ...):\n%s\n\n' \
      "$sqlite_hits" >&2
    violations=$((violations + 1))
  fi
}

# ─────────────────────────────────────────────────────────────────────────────
# Run all guards
# ─────────────────────────────────────────────────────────────────────────────
guard_a_succeeded_writer
guard_b_awaiting_artifact_writer
guard_c_handleTaskResult_wireJobId
guard_d_one_lease_reaper
guard_e_task_migration_strftime

if [[ "$violations" -gt 0 ]]; then
  printf '%d task-runtime-invariant violation(s) -- see above\n' \
    "$violations" >&2
  exit 1
fi

echo "check-task-runtime-invariants: OK"
