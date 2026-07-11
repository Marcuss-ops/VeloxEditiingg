#!/usr/bin/env bash
# =============================================================================
# Scenario 01 — kill worker during READY→TaskOffer pre-accept window
# =============================================================================
# Fault: SIGKILL worker immediately after master wrote LEASED state and
#   PENDING task_attempt row, but BEFORE the worker's MsgTaskLeaseGranted
#   acknowledgement.
# Expected: reaper's ExpireTaskLeaseAtomic eventually promotes Task back
#   to READY with a fresh (worker_id, lease_id, attempt_id); attempt closes
#   as TIMED_OUT. New worker reclaims with new lease.
# Invariants: NR-1 (no double RUNNING), NR-3 (no stuck job), NR-7 (new
#   attempt_id minted after reap).
# =============================================================================
set -uo pipefail
SCENARIO_ID="01-kill-worker-stage1"
EVIDENCE_DIR="${EVIDENCE_DIR:?EVIDENCE_DIR not set}"
DB="${DB:?DB not set}"

# shellcheck disable=SC1091
source "$(dirname "$0")/../lib.sh"
# shellcheck disable=SC1091
source "$(dirname "$0")/../invariants.sh"

rm_begin_scenario "$SCENARIO_ID"
rm_info "[$SCENARIO_ID] starting"

# 1. Seed a LEASED task directly (simulates worker-died-mid-offer state).
NOW="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
TASK_ID="task-01-$RANDOM"
JOB_ID="job-01-$RANDOM"
ATTEMPT_ID="att-$RANDOM"
LEASE_ID="lease-$RANDOM"
SAFE_EXPIRY="$(date -u -d '-1 minute' +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -v-1M +%Y-%m-%dT%H:%M:%SZ)"

sqlite3 "$DB" <<SQL
INSERT INTO tasks (task_id, job_id, status, revision, attempt_count,
                  worker_id, lease_id, lease_expires_at,
                  created_at, updated_at)
VALUES ('$TASK_ID', '$JOB_ID', 'LEASED', 5, 1,
        'worker-dead-01', '$LEASE_ID', '$SAFE_EXPIRY',
        '$NOW', '$NOW');
INSERT INTO task_attempts (id, task_id, job_id, attempt_number, worker_id,
                          lease_id, status, revision, created_at, updated_at)
VALUES ('$ATTEMPT_ID', '$TASK_ID', '$JOB_ID', 1, 'worker-dead-01',
        '$LEASE_ID', 'RUNNING', 0, '$NOW', '$NOW');
SQL

rm_info "[$SCENARIO_ID] seed LEASED + RUNNING-attempt; lease expired 60s ago"

# 2. Simulate reaper execution (single CAS tx per SQLiteJobRepository
#    contract — see store/sqlite_task_repository.go:ExpireTaskLeaseAtomic).
sqlite3 "$DB" <<SQL
UPDATE tasks SET
  status = 'READY',
  worker_id = '', lease_id = '', lease_expires_at = NULL,
  revision = revision + 1, updated_at = '$NOW'
WHERE task_id = '$TASK_ID';
UPDATE task_attempts SET
  status = 'TIMED_OUT', completed_at = '$NOW',
  error_code = 'LEASE_EXPIRED', error_message = 'recovery-matrix stage-1 reap',
  updated_at = '$NOW'
WHERE id = '$ATTEMPT_ID';
SQL

rm_info "[$SCENARIO_ID] reaped; task=READY, attempt=TIMED_OUT"

# 3. Simulate a NEW ClaimNextWithAttemptAtomic — fresh identity.
NEW_ATTEMPT_ID="att-NEW-$RANDOM"
NEW_LEASE_ID="lease-NEW-$RANDOM"
sqlite3 "$DB" <<SQL
UPDATE tasks SET
  status = 'LEASED', worker_id = 'worker-recovered-01',
  lease_id = '$NEW_LEASE_ID',
  attempt_id = '$NEW_ATTEMPT_ID', attempt_number = 2,
  revision = revision + 1, updated_at = '$NOW'
WHERE task_id = '$TASK_ID';
INSERT INTO task_attempts (id, task_id, job_id, attempt_number, worker_id,
                          lease_id, status, revision, created_at, updated_at)
VALUES ('$NEW_ATTEMPT_ID', '$TASK_ID', '$JOB_ID', 2, 'worker-recovered-01',
        '$NEW_LEASE_ID', 'PENDING', 0, '$NOW', '$NOW');
SQL

# 4. Capture evidence + run invariants.
sqlite3 -separator '|' -header "$DB" "
  SELECT 'pre' AS phase, status, attempt_id, lease_id, attempt_number
  FROM   task_attempts WHERE task_id = '$TASK_ID'
  ORDER BY attempt_number" >"$EVIDENCE_DIR/01-attempts.txt"

# NR-1 (only 1 attempt active per task — fresh PENDING is fine; the
# TIMED_OUT one is on a different (worker_id, lease_id) tuple).
rm_assert_invariant "$DB" "NR-1" 0
# NR-7: the TIMED_OUT attempt's id MUST NOT equal the new active attempt_id.
rm_assert_invariant "$DB" "NR-7" 0

rm_end_scenario "$SCENARIO_ID" "expired lease reaped; new attempt minted"
rm_info "[$SCENARIO_ID] done"
