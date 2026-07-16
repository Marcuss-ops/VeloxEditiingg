#!/usr/bin/env bash
# =============================================================================
# Scenario 16 — doppio worker sullo stesso Task (race resolved)
# =============================================================================
# Fault: through a CAS race or a buggy worker, two attempts for the same
#   task are simultaneously active (PENDING).
# Expected: the master's lease/attempt CAS detects the conflict, rejects
#   the second claim, and marks the second attempt as REJECTED. Only one
#   active attempt remains.
# Type: POSITIVE — the system repairs the bad state and invariants hold.
# Invariants: NR-1 (one attempt active per task).
# =============================================================================
set -uo pipefail
SCENARIO_ID="16-double-worker-task"
EVIDENCE_DIR="${EVIDENCE_DIR:?EVIDENCE_DIR not set}"
DB="${DB:?DB not set}"

# shellcheck disable=SC1091
source "$(dirname "$0")/../lib.sh"
# shellcheck disable=SC1091
source "$(dirname "$0")/../invariants.sh"

rm_begin_scenario "$SCENARIO_ID"
rm_info "[$SCENARIO_ID] starting"
NOW="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
TASK_ID="task-16-$RANDOM"
JOB_ID="job-16-$RANDOM"
LEASE_A="lease-16-A-$RANDOM"
ATTEMPT_A="att-16-A-$RANDOM"
LEASE_B="lease-16-B-$RANDOM"
ATTEMPT_B="att-16-B-$RANDOM"

# Seed: task is LEASED to worker-A with attempt A PENDING. A second
# active attempt B exists for the same task — the race condition.
sqlite3 "$DB" <<SQL
INSERT INTO tasks (task_id, job_id, status, revision, attempt_count,
                  worker_id, lease_id, lease_expires_at,
                  attempt_id, attempt_number,
                  created_at, updated_at)
VALUES ('$TASK_ID', '$JOB_ID', 'LEASED', 1, 2,
        'worker-16-A', '$LEASE_A',
        date('now', '+30 minutes'),
        '$ATTEMPT_A', 1, '$NOW', '$NOW');
INSERT INTO task_attempts (id, task_id, job_id, attempt_number, worker_id,
                          lease_id, status, revision,
                          created_at, updated_at)
VALUES ('$ATTEMPT_A', '$TASK_ID', '$JOB_ID', 1, 'worker-16-A',
        '$LEASE_A', 'PENDING', 0, '$NOW', '$NOW'),
       ('$ATTEMPT_B', '$TASK_ID', '$JOB_ID', 2, 'worker-16-B',
        '$LEASE_B', 'PENDING', 0, '$NOW', '$NOW');
SQL

# Simulate the CAS guard detecting the race and rejecting the second
# claim. The second attempt is moved to REJECTED, the task attempt_count
# is decremented back to 1, and an audit message is recorded.
sqlite3 "$DB" <<SQL
UPDATE task_attempts
SET status='REJECTED', updated_at='$NOW'
WHERE id='$ATTEMPT_B';
UPDATE tasks
SET attempt_count=1, updated_at='$NOW'
WHERE task_id='$TASK_ID';
INSERT INTO worker_messages (worker_id, kind, refused_reason, created_at)
VALUES ('worker-16-B', 'claim_rejected',
        'task already LEASED to worker-16-A ($LEASE_A)',
        '$NOW');
SQL

# Evidence snapshot: only the first attempt remains active.
sqlite3 -separator '|' -header "$DB" "
  SELECT id, status, attempt_number, worker_id, lease_id
  FROM task_attempts WHERE task_id='$TASK_ID' ORDER BY attempt_number" \
  >"$EVIDENCE_DIR/16-attempts.txt"

rm_assert_invariant "$DB" "NR-1" 0
# NR-2: the rejected second worker must not be able to finalize the task
# using its stale lease_id.
rm_assert_invariant "$DB" "NR-2" 0
rm_end_scenario "$SCENARIO_ID" "double-attempt race detected; second claim rejected"
rm_info "[$SCENARIO_ID] done"
