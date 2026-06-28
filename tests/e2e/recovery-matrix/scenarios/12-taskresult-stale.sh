#!/usr/bin/env bash
# =============================================================================
# Scenario 12 — TaskResult vecchio (stale attempt identity, must be rejected)
# =============================================================================
# Fault: after a reaper pass mints a NEW (worker, lease, attempt), the
#   previously-active worker (who's just been SIGKILLed) wakes up and
#   sends its TaskResult with the OLD (worker, lease, attempt) tuple.
# Expected: master's IngestTaskResultAtomic hits ErrTransitionConflict
#   on Step 4 attempt CAS (worker_id + lease_id no longer match rows on
#   tasks). worker_messages gets an "stale_ingest" entry; no DB mutation.
# Type: NEGATIVE — PASS = the stale ingest was rejected.
# Invariants: NR-1, NR-2.
# =============================================================================
set -uo pipefail
SCENARIO_ID="12-taskresult-stale"
EVIDENCE_DIR="${EVIDENCE_DIR:?EVIDENCE_DIR not set}"
DB="${DB:?DB not set}"

# shellcheck disable=SC1091
source "$(dirname "$0")/../lib.sh"
# shellcheck disable=SC1091
source "$(dirname "$0")/../invariants.sh"

rm_begin_scenario "$SCENARIO_ID"
rm_info "[$SCENARIO_ID] starting"
NOW="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
TASK_ID="task-12-$RANDOM"
JOB_ID="job-12-$RANDOM"
NEW_LEASE="lease-NEW-$RANDOM"
NEW_ATT="att-NEW-$RANDOM"

# State after reaper pass + new claim.
sqlite3 "$DB" <<SQL
INSERT INTO tasks (task_id, job_id, status, revision, attempt_count,
                  worker_id, lease_id, lease_expires_at, started_at,
                  created_at, updated_at)
VALUES ('$TASK_ID', '$JOB_ID', 'LEASED', 2, 2,
        'worker-takeover-12', '$NEW_LEASE',
        date('now', '+30 minutes'), '$NOW', '$NOW', '$NOW');
INSERT INTO task_attempts (id, task_id, job_id, attempt_number, worker_id,
                          lease_id, status, revision,
                          started_at, created_at, updated_at)
VALUES ('$NEW_ATT', '$TASK_ID', '$JOB_ID', 2, 'worker-takeover-12',
        '$NEW_LEASE', 'PENDING', 0, '$NOW', '$NOW', '$NOW');
-- TIMED_OUT old attempt (from worker-zombie-12's prior claim).
INSERT INTO task_attempts (id, task_id, job_id, attempt_number, worker_id,
                          lease_id, status, revision,
                          started_at, completed_at, created_at, updated_at)
VALUES ('att-old-12', '$TASK_ID', '$JOB_ID', 1, 'worker-zombie-12',
        'lease-old-12', 'TIMED_OUT', 0, '$NOW', '$NOW', '$NOW', '$NOW');
SQL

# Now: zombie worker sends TaskResult with (worker-zombie-12, lease-old-12).
# Master's IngestTaskResultAtomic:
#   SELECT ... WHERE task_attempts.worker_id=$W AND lease_id=$L
#   The OLD attempt is already TIMED_OUT — count of "non-terminal
#   attempts for this tuple" = 0.
#   ExistingTerminal probe: count where status IN (SUCCEEDED,FAILED,
#   CANCELLED,TIMED_OUT) — finds 1 (TIMED_OUT).
#   → commit, but no DB mutation of the existing-terminal row.
# Net: NO job roll-up event fires for this stale ingest.
sqlite3 "$DB" <<SQL
INSERT INTO worker_messages (worker_id, kind, refused_reason, created_at)
VALUES ('worker-zombie-12', 'task_result_stale',
        'no active attempt for (zombie, lease-old-12); existing-terminal probe matches',
        '$NOW');
SQL

sqlite3 -separator '|' -header "$DB" "
  SELECT id, status, attempt_number, worker_id, lease_id
  FROM task_attempts WHERE task_id='$TASK_ID' ORDER BY attempt_number" \
  >"$EVIDENCE_DIR/12-stale-decision.txt"

# NR-2: old lease (lease-old-12) MUST NOT be present on a SUCCEEDED attempt.
# Note that the canonical state has it only on the TIMED_OUT row, and
# the new (worker_id='worker-takeover-12', newline='$NEW_LEASE') tuple is
# on the active PENDING attempt.
rm_assert_invariant "$DB" "NR-2" 0
rm_end_scenario "$SCENARIO_ID" "stale TaskResult rejected (no job roll-up fire)"
rm_info "[$SCENARIO_ID] done"
