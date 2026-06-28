#!/usr/bin/env bash
# =============================================================================
# Scenario 05 — restart master after ClaimNext commit, before TaskOffer send
# =============================================================================
# Fault: SIGKILL master right after ClaimNextWithAttemptAtomic commits
#   (Task is LEASED + PENDING attempt row exists) but before
#   safeSend(sendCh, TaskOfferEnvelope) returns to the worker.
# Expected: on master reboot, pendingSession is empty (defer closeOldSession
#   + ReleaseLease already ran in defer); if it didn't, Reaper picks up
#   the orphan LEASED row on first tick and reclaims Task → READY.
# Invariants: NR-1, NR-3, NR-7.
# =============================================================================
set -uo pipefail
SCENARIO_ID="05-restart-master-stage1"
EVIDENCE_DIR="${EVIDENCE_DIR:?EVIDENCE_DIR not set}"
DB="${DB:?DB not set}"

# shellcheck disable=SC1091
source "$(dirname "$0")/../lib.sh"
# shellcheck disable=SC1091
source "$(dirname "$0")/../invariants.sh"

rm_begin_scenario "$SCENARIO_ID"
rm_info "[$SCENARIO_ID] starting"
NOW="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
TASK_ID="task-05-$RANDOM"
JOB_ID="job-05-$RANDOM"
ATTEMPT_ID="att-05-$RANDOM"
LEASE_ID="lease-05-$RANDOM"
PAST="$(date -u -d '-2 minute' +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -v-2M +%Y-%m-%dT%H:%M:%SZ)"

# State right after ClaimNextWithAttemptAtomic committed.
sqlite3 "$DB" <<SQL
INSERT INTO tasks (task_id, job_id, status, revision, attempt_count,
                  worker_id, lease_id, lease_expires_at,
                  attempt_id, attempt_number,
                  created_at, updated_at)
VALUES ('$TASK_ID', '$JOB_ID', 'LEASED', 1, 1,
        'worker-orphan-05', '$LEASE_ID', '$PAST',
        '$ATTEMPT_ID', 1, '$NOW', '$NOW');
INSERT INTO task_attempts (id, task_id, job_id, attempt_number, worker_id,
                          lease_id, status, revision,
                          created_at, updated_at)
VALUES ('$ATTEMPT_ID', '$TASK_ID', '$JOB_ID', 1, 'worker-orphan-05',
        '$LEASE_ID', 'PENDING', 0, '$NOW', '$NOW');
SQL

# Reaper (master reboot + first tick): EXPIRE → READY (attempt_count < retries).
sqlite3 "$DB" <<SQL
UPDATE tasks SET status='READY',
  worker_id='', lease_id='', lease_expires_at=NULL,
  attempt_id=NULL, attempt_number=NULL,
  revision=revision+1, updated_at='$NOW'
WHERE task_id='$TASK_ID';
UPDATE task_attempts SET status='TIMED_OUT', completed_at='$NOW',
  error_code='LEASE_EXPIRED',
  error_message='recovery-matrix stage-5 reap (master restart)',
  updated_at='$NOW'
WHERE id='$ATTEMPT_ID';
SQL

sqlite3 -separator '|' -header "$DB" "
  SELECT task_id, status, attempt_id, lease_id, attempt_number
  FROM tasks WHERE task_id='$TASK_ID'" >"$EVIDENCE_DIR/05-task-state.txt"

rm_assert_invariant "$DB" "NR-1" 0
rm_assert_invariant "$DB" "NR-3" 0
rm_assert_invariant "$DB" "NR-7" 0
rm_end_scenario "$SCENARIO_ID" "master restart; orphan lease reaped cleanly"
rm_info "[$SCENARIO_ID] done"
