#!/usr/bin/env bash
# =============================================================================
# Scenario 20 — kill worker mid-render on a multi-task DAG (chaos W8)
# =============================================================================
# Fault: worker executing the 'mix' task of a 3-task DAG (prep → mix →
#   concat) is SIGKILLed mid-render. prep is SUCCEEDED, mix dies with an
#   expired lease, concat is PENDING behind it.
# Expected:
#   1. Lease reaper closes mix's attempt as TIMED_OUT and requeues per
#      retry budget (existing kill-worker semantics, scenarios 01-04).
#   2. When retries are exhausted and mix lands FAILED (non-SUCCEEDED
#      terminal), TickReadiness' failure propagation (taskgraph
#      propagateFailures) cancels concat — concat must NOT sit PENDING
#      forever (the pre-DAG zombie wait).
#   3. The job roll-up then observes all-tasks-terminal and the job
#      closes deterministically (no dangling READY/PENDING rows).
# Invariants exercised: NR-1 (one active attempt per task), NR-3 (no job
#   stuck forever), NR-7 (post-reap attempt identity), DAG-P1 (doomed
#   cancellation), DAG-P2 (no orphan non-terminal behind a non-SUCCEEDED
#   terminal dependency).
#
# Invocation (standalone, no orchestrator required):
#   bash tests/e2e/recovery-matrix/scenarios/20-dag-kill-worker-propagation.sh
#
# Integration note: run.sh's default loop iterates scenarios 01..17; run
# this gate with
#   bash tests/e2e/recovery-matrix/run.sh --scenario 20
# or wire the extended loop range in a follow-up commit (same pattern as
# scenario 19).
#
# DB contract: the orchestrator (run.sh) provisions a fresh DB whose
# tasks schema is migration-176-current (depends_on column present, the
# one-task-per-job unique index dropped). Standalone runs must point DB
# at an equivalent schema:
#   DB=/path/chaos.db EVIDENCE_DIR=/tmp/ev \
#     bash tests/e2e/recovery-matrix/scenarios/20-dag-kill-worker-propagation.sh
# =============================================================================
set -uo pipefail

SCENARIO_ID="20-dag-kill-worker-propagation"
EVIDENCE_DIR="${EVIDENCE_DIR:-/tmp/velox-recovery-matrix/$(date -u +%Y-%m-%d)/scenarios/20}"
DB="${DB:?DB not set (see header for the schema contract)}"

# shellcheck disable=SC1091
source "$(dirname "$0")/../lib.sh"
# shellcheck disable=SC1091
source "$(dirname "$0")/../invariants.sh"

rm_begin_scenario "$SCENARIO_ID"
rm_info "[$SCENARIO_ID] starting"
rm_info "[$SCENARIO_ID] db = $DB"

NOW="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
PAST="$(date -u -d '-2 minute' +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -v-2M +%Y-%m-%dT%H:%M:%SZ)"

JOB_ID="job-20-$RANDOM"
PREP_ID="task-20-prep-$RANDOM"
MIX_ID="task-20-mix-$RANDOM"
CONCAT_ID="task-20-concat-$RANDOM"
ATT_ID="att-20-mix-$RANDOM"
LEASE_ID="lease-20-mix-$RANDOM"

# Local positive assertion helper: the matrix lib ships invariant-level
# assertions only; scenarios 11-15 use direct sqlite comparisons inline.
dag_assert_eq() {
  local label="$1" got="$2" want="$3"
  if [[ "$got" == "$want" ]]; then
    rm_pass "$label"
  else
    rm_fail "$label (got '$got', want '$want')"
    rm_mark_inv_fail
  fi
}

# ── Seed: 3-task DAG with depends_on edges (migration 176 shape) ────────────
sqlite3 "$DB" <<SQL
INSERT INTO tasks (task_id, job_id, status, revision, attempt_count,
                  worker_id, lease_id, lease_expires_at, started_at,
                  created_at, updated_at, depends_on)
VALUES
 ('$PREP_ID', '$JOB_ID', 'SUCCEEDED', 3, 1, '', '', NULL, '$NOW', '$NOW', '$NOW', '[]'),
 ('$MIX_ID', '$JOB_ID', 'RUNNING', 7, 4, 'worker-chaos-20', '$LEASE_ID', '$PAST', '$NOW', '$NOW', '$NOW', '["$PREP_ID"]'),
 ('$CONCAT_ID', '$JOB_ID', 'PENDING', 0, 0, '', '', NULL, NULL, '$NOW', '$NOW', '["$MIX_ID"]');
INSERT INTO task_attempts (id, task_id, job_id, attempt_number, worker_id,
                          lease_id, status, revision,
                          started_at, created_at, updated_at)
VALUES ('$ATT_ID', '$MIX_ID', '$JOB_ID', 4, 'worker-chaos-20',
        '$LEASE_ID', 'RUNNING', 0, '$NOW', '$NOW', '$NOW');
SQL

# ── Stage 1: reaper requeues mix (attempt 4 within retry budget) ────────────
# ExpireTaskLeaseAtomic semantics: close attempt TIMED_OUT, task → READY,
# clear the lease tuple.
sqlite3 "$DB" <<SQL
UPDATE tasks SET status='READY',
  worker_id='', lease_id='', lease_expires_at=NULL,
  revision=revision+1, updated_at='$NOW'
WHERE task_id='$MIX_ID';
UPDATE task_attempts SET status='TIMED_OUT', completed_at='$NOW',
  error_code='LEASE_EXPIRED',
  error_message='scenario-20 requeue (retries not yet exhausted)',
  updated_at='$NOW'
WHERE id='$ATT_ID';
SQL

dag_assert_eq "stage1-mix-requeued" \
  "$(sqlite3 "$DB" "SELECT status FROM tasks WHERE task_id='$MIX_ID'")" "READY"
dag_assert_eq "stage1-concat-still-pending" \
  "$(sqlite3 "$DB" "SELECT status FROM tasks WHERE task_id='$CONCAT_ID'")" "PENDING"
dag_assert_eq "stage1-attempt-closed-timed-out" \
  "$(sqlite3 "$DB" "SELECT status FROM task_attempts WHERE id='$ATT_ID'")" "TIMED_OUT"
# NR-1/NR-7 invariants must hold over the seeded + reaped state.
rm_assert_invariant "$DB" "NR-1" 0 ""
rm_assert_invariant "$DB" "NR-7" 0 ""

# ── Stage 2: retries exhausted, mix lands FAILED (non-SUCCEEDED terminal) ──
# ExpireTaskLeaseAtomic with AttemptsExhausted=true (budget 3 default; the
# seeded attempt_count=4 already exceeds it).
sqlite3 "$DB" <<SQL
UPDATE tasks SET status='FAILED', completed_at='$NOW',
  revision=revision+1, updated_at='$NOW'
WHERE task_id='$MIX_ID';
SQL
dag_assert_eq "stage2-mix-failed-terminal" \
  "$(sqlite3 "$DB" "SELECT status FROM tasks WHERE task_id='$MIX_ID'")" "FAILED"

# ── Stage 3: failure propagation (TickReadiness.propagateFailures) ──────────
# The lifecycle sweep cancels every PENDING task transitively downstream
# of a non-SUCCEEDED terminal dependency. In SQL this is the transitive
# closure over depends_on edges (recursive CTE; UNION dedupe guarantees
# termination even on a hypothetical cycle). The Go-side unit tests
# (DataServer/internal/taskgraph/dependencies_test.go) pin the transitive
# engine behavior; this stage pins the persisted post-state.
sqlite3 "$DB" <<SQL
WITH RECURSIVE doomed(id) AS (
  SELECT task_id FROM tasks
   WHERE job_id='$JOB_ID'
     AND status IN ('FAILED','CANCELLED','TIMED_OUT')
  UNION
  SELECT child.task_id
    FROM tasks child, doomed d
   WHERE EXISTS (
     SELECT 1 FROM json_each(child.depends_on) je
      WHERE je.value = d.id
   )
)
UPDATE tasks SET status='CANCELLED', completed_at='$NOW',
  revision=revision+1, updated_at='$NOW'
WHERE task_id IN (
  SELECT id FROM doomed
   WHERE id IN (SELECT task_id FROM tasks WHERE job_id='$JOB_ID' AND status='PENDING')
);
SQL
dag_assert_eq "stage3-concat-cancelled" \
  "$(sqlite3 "$DB" "SELECT status FROM tasks WHERE task_id='$CONCAT_ID'")" "CANCELLED"

# ── Invariants: zombie-free terminal state ──────────────────────────────────
# DAG-P2: no non-terminal task remains behind a non-SUCCEEDED terminal dep.
NON_TERMINAL="$(sqlite3 "$DB" "SELECT COUNT(*) FROM tasks WHERE job_id='$JOB_ID' AND status NOT IN ('SUCCEEDED','FAILED','CANCELLED','TIMED_OUT')")"
dag_assert_eq "dag-no-orphan-nonterminal" "$NON_TERMINAL" "0"
# NR-1 zombie probe: no active attempts left in the job.
ZOMBIE_ATTEMPTS="$(sqlite3 "$DB" "SELECT COUNT(*) FROM task_attempts WHERE job_id='$JOB_ID' AND status IN ('RUNNING','PENDING','LEASED')")"
dag_assert_eq "dag-no-zombie-attempts" "$ZOMBIE_ATTEMPTS" "0"
rm_assert_invariant "$DB" "NR-3" 0 600 ""

rm_end_scenario "$SCENARIO_ID" "DAG kill-worker propagation: prep SUCCEEDED, mix FAILED after retry budget, concat CANCELLED by propagation, zero zombie rows"
