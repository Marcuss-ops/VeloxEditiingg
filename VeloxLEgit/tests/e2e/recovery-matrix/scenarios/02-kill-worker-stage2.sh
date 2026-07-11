#!/usr/bin/env bash
# =============================================================================
# Scenario 02 — kill worker during RUNNING (mid-execution)
# =============================================================================
# Fault: SIGKILL worker after acceptance, during real execution. Worker
#   never sends a TaskResult.
# Expected: master-side lease expirer detects stale lease (lease_expires_at
#   past), ExpireTaskLeaseAtomic recovers Task to READY and closes Attempt
#   as TIMED_OUT. Retry budget respected.
# Invariants: NR-1, NR-3, NR-7.
# =============================================================================
set -uo pipefail
SCENARIO_ID="02-kill-worker-stage2"
EVIDENCE_DIR="${EVIDENCE_DIR:?EVIDENCE_DIR not set}"
DB="${DB:?DB not set}"

# shellcheck disable=SC1091
source "$(dirname "$0")/../lib.sh"
# shellcheck disable=SC1091
source "$(dirname "$0")/../invariants.sh"

rm_begin_scenario "$SCENARIO_ID"
rm_info "[$SCENARIO_ID] starting"
NOW="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
TASK_ID="task-02-$RANDOM"
JOB_ID="job-02-$RANDOM"
ATTEMPT_ID="att-02-$RANDOM"
LEASE_ID="lease-02-$RANDOM"
PAST="$(date -u -d '-2 minute' +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -v-2M +%Y-%m-%dT%H:%M:%SZ)"

# Seed RUNNING attempt with expired lease.
sqlite3 "$DB" <<SQL
INSERT INTO tasks (task_id, job_id, status, revision, attempt_count,
                  worker_id, lease_id, lease_expires_at, started_at,
                  created_at, updated_at)
VALUES ('$TASK_ID', '$JOB_ID', 'RUNNING', 7, 1,
        'worker-during-run', '$LEASE_ID', '$PAST', '$NOW', '$NOW', '$NOW');
INSERT INTO task_attempts (id, task_id, job_id, attempt_number, worker_id,
                          lease_id, status, revision,
                          started_at, created_at, updated_at)
VALUES ('$ATTEMPT_ID', '$TASK_ID', '$JOB_ID', 1, 'worker-during-run',
        '$LEASE_ID', 'RUNNING', 0, '$NOW', '$NOW', '$NOW');
SQL

# Reaper: attempt_count=1 < maxRetries=2 (default).
sqlite3 "$DB" <<SQL
UPDATE tasks SET status='READY',
  worker_id='', lease_id='', lease_expires_at=NULL,
  revision=revision+1, updated_at='$NOW'
WHERE task_id='$TASK_ID';
UPDATE task_attempts SET status='TIMED_OUT', completed_at='$NOW',
  error_code='LEASE_EXPIRED',
  error_message='recovery-matrix stage-2 reap (during running)',
  updated_at='$NOW'
WHERE id='$ATTEMPT_ID';
SQL

# New claim by a different worker.
NEW_LEASE="lease-NEW-$RANDOM"
NEW_ATT="att-NEW-$RANDOM"
sqlite3 "$DB" <<SQL
UPDATE tasks SET status='LEASED',
  worker_id='worker-takeover-02', lease_id='$NEW_LEASE',
  attempt_id='$NEW_ATT', attempt_number=2,
  revision=revision+1, updated_at='$NOW'
WHERE task_id='$TASK_ID';
INSERT INTO task_attempts (id, task_id, job_id, attempt_number, worker_id,
                          lease_id, status, revision, created_at, updated_at)
VALUES ('$NEW_ATT', '$TASK_ID', '$JOB_ID', 2, 'worker-takeover-02',
        '$NEW_LEASE', 'PENDING', 0, '$NOW', '$NOW');
SQL

sqlite3 -separator '|' -header "$DB" "
  SELECT task_id, status, attempt_id, lease_id, attempt_number,
         started_at, lease_expires_at
  FROM tasks WHERE task_id='$TASK_ID'" >"$EVIDENCE_DIR/02-task-state.txt"

rm_assert_invariant "$DB" "NR-1" 0
rm_assert_invariant "$DB" "NR-7" 0
rm_end_scenario "$SCENARIO_ID" "running attempt expired; takeover mint"
rm_info "[$SCENARIO_ID] done"
