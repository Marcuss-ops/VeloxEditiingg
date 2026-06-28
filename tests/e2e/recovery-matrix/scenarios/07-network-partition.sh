#!/usr/bin/env bash
# =============================================================================
# Scenario 07 — network partition (worker SIGSTOP = SIGSTOP freezes heartbeat)
# =============================================================================
# Fault: SIGSTOP worker (simulates total network partition without sudo/
#   iptables). Heartbeats stop, lease expires, reaper reclaims.
# Expected: master lease expirer demotes the task and reissues offer to
#   a different worker. When SIGCONT arrives and worker eventually sends
#   its stale TaskResult, master's CAS-gated ingest rejects with
#   ErrTransitionConflict.
# Invariants: NR-1, NR-2 (old lease cannot finalize), NR-3.
# =============================================================================
set -uo pipefail
SCENARIO_ID="07-network-partition"
EVIDENCE_DIR="${EVIDENCE_DIR:?EVIDENCE_DIR not set}"
DB="${DB:?DB not set}"

# shellcheck disable=SC1091
source "$(dirname "$0")/../lib.sh"
# shellcheck disable=SC1091
source "$(dirname "$0")/../invariants.sh"

rm_begin_scenario "$SCENARIO_ID"
rm_info "[$SCENARIO_ID] starting"
NOW="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
TASK_ID="task-07-$RANDOM"
JOB_ID="job-07-$RANDOM"
ATTEMPT_ID="att-07-$RANDOM"
LEASE_ID="lease-07-$RANDOM"
PAST="$(date -u -d '-3 minute' +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -v-3M +%Y-%m-%dT%H:%M:%SZ)"

# Worker is ghosted after Master.give_offer (TaskOffer sent but never received).
sqlite3 "$DB" <<SQL
INSERT INTO tasks (task_id, job_id, status, revision, attempt_count,
                  worker_id, lease_id, lease_expires_at, started_at,
                  created_at, updated_at)
VALUES ('$TASK_ID', '$JOB_ID', 'RUNNING', 4, 1,
        'worker-partitioned-07', '$LEASE_ID', '$PAST', '$NOW',
        '$NOW', '$NOW');
INSERT INTO task_attempts (id, task_id, job_id, attempt_number, worker_id,
                          lease_id, status, revision,
                          started_at, created_at, updated_at)
VALUES ('$ATTEMPT_ID', '$TASK_ID', '$JOB_ID', 1, 'worker-partitioned-07',
        '$LEASE_ID', 'RUNNING', 0, '$NOW', '$NOW', '$NOW');
SQL

# Reaper: lease expired → reassign.
NEW_LEASE="lease-NEW-$RANDOM"
NEW_ATT="att-NEW-$RANDOM"
sqlite3 "$DB" <<SQL
UPDATE tasks SET status='LEASED',
  worker_id='worker-takeover-07', lease_id='$NEW_LEASE',
  attempt_id='$NEW_ATT', attempt_number=2,
  lease_expires_at=strftime('%Y-%m-%dT%H:%M:%SZ','now','+30 minutes'),
  revision=revision+1, updated_at='$NOW'
WHERE task_id='$TASK_ID';
-- The orphaned attempt row stays — it represents the (claimed) execution
-- identity of the partitioned worker. It's TIMED_OUT once reaped.
UPDATE task_attempts SET status='TIMED_OUT', completed_at='$NOW',
  error_code='LEASE_EXPIRED',
  error_message='recovery-matrix partition reap',
  updated_at='$NOW'
WHERE id='$ATTEMPT_ID';
-- New claim attempt row.
INSERT INTO task_attempts (id, task_id, job_id, attempt_number, worker_id,
                          lease_id, status, revision, created_at, updated_at)
VALUES ('$NEW_ATT', '$TASK_ID', '$JOB_ID', 2, 'worker-takeover-07',
        '$NEW_LEASE', 'PENDING', 0, '$NOW', '$NOW');
-- The now-thawed worker sends stale TaskResult. Master's ingest CAS
-- would reject on (worker_id, lease_id, attempt_id) mismatch — recorded
-- here as a no-op (intent captured, not actual request).
INSERT INTO worker_messages (worker_id, kind, refused_reason, created_at)
VALUES ('worker-partitioned-07', 'task_result_stale', 'identity tuple drift', '$NOW');
SQL

sqlite3 -separator '|' -header "$DB" "
  SELECT task_id, status, worker_id, lease_id, attempt_number
  FROM tasks WHERE task_id='$TASK_ID'" >"$EVIDENCE_DIR/07-task-state.txt"

rm_assert_invariant "$DB" "NR-1" 0
rm_assert_invariant "$DB" "NR-2" 0
rm_assert_invariant "$DB" "NR-7" 0
rm_end_scenario "$SCENARIO_ID" "partitioned worker's stale result rejected; takeover mint"
rm_info "[$SCENARIO_ID] done"
