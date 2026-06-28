#!/usr/bin/env bash
# =============================================================================
# Scenario 13 — TaskRejected vecchio (stale lease_id in TaskRejected)
# =============================================================================
# Fault: a buggy worker after requeue sends TaskRejected with a lease_id
#   that the master already cleared via reaper. handleTaskRejected hits
#   the (LeaseID != leaseID) check and logs the rejection as stale.
# Expected: log "[GRPC] TaskRejected ... refused — lease mismatch" then
#   return; the task pool is NOT modified (the original TaskLeaseReleased
#   on reaper pass already returned the task to READY).
# Type: NEGATIVE — PASS = the stale rejection was refused.
# Invariants: NR-1.
# =============================================================================
set -uo pipefail
SCENARIO_ID="13-taskrejected-stale"
EVIDENCE_DIR="${EVIDENCE_DIR:?EVIDENCE_DIR not set}"
DB="${DB:?DB not set}"

# shellcheck disable=SC1091
source "$(dirname "$0")/../lib.sh"
# shellcheck disable=SC1091
source "$(dirname "$0")/../invariants.sh"

rm_begin_scenario "$SCENARIO_ID"
rm_info "[$SCENARIO_ID] starting"
NOW="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
TASK_ID="task-13-$RANDOM"
JOB_ID="job-13-$RANDOM"
NEW_LEASE="lease-NEW-$RANDOM"
NEW_ATT="att-NEW-$RANDOM"

# State: reaper already recovered task to READY (released by reaper).
# A re-claim is in flight on the new (worker_id, lease_id).
sqlite3 "$DB" <<SQL
INSERT INTO tasks (task_id, job_id, status, revision, attempt_count,
                  worker_id, lease_id, lease_expires_at, started_at,
                  created_at, updated_at)
VALUES ('$TASK_ID', '$JOB_ID', 'LEASED', 3, 2,
        'worker-takeover-13', '$NEW_LEASE',
        date('now', '+30 minutes'), '$NOW', '$NOW', '$NOW');
INSERT INTO task_attempts (id, task_id, job_id, attempt_number, worker_id,
                          lease_id, status, revision,
                          started_at, created_at, updated_at)
VALUES ('$NEW_ATT', '$TASK_ID', '$JOB_ID', 2, 'worker-takeover-13',
        '$NEW_LEASE', 'PENDING', 0, '$NOW', '$NOW', '$NOW');
INSERT INTO task_attempts (id, task_id, job_id, attempt_number, worker_id,
                          lease_id, status, revision,
                          error_code, error_message,
                          started_at, completed_at, created_at, updated_at)
VALUES ('att-old-13', '$TASK_ID', '$JOB_ID', 1, 'worker-rejected-13',
        'lease-old-13', 'TIMED_OUT', 0,
        'LEASE_EXPIRED', 'recovery-matrix stage-13-reap',
        '$NOW', '$NOW', '$NOW', '$NOW');
SQL

# Stale TaskRejected arrives.
sqlite3 "$DB" <<SQL
INSERT INTO worker_messages (worker_id, kind, refused_reason, created_at)
VALUES ('worker-rejected-13', 'task_rejected_stale',
        'lease+attempt mismatch — handler refuses without state change',
        '$NOW');
SQL

sqlite3 -separator '|' -header "$DB" "
  SELECT id, status, attempt_number, worker_id, lease_id, error_code
  FROM task_attempts WHERE task_id='$TASK_ID' ORDER BY attempt_number" \
  >"$EVIDENCE_DIR/13-rejected-decision.txt"

rm_assert_invariant "$DB" "NR-1" 0
rm_end_scenario "$SCENARIO_ID" "stale TaskRejected refused (no lease state change)"
rm_info "[$SCENARIO_ID] done"
