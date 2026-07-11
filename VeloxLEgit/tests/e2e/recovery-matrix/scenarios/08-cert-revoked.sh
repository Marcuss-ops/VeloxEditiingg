#!/usr/bin/env bash
# =============================================================================
# Scenario 08 — certificato revocato (session revocation mid-stream)
# =============================================================================
# Fault: worker_flags SET revoked=1 + worker_sessions SET revoked=1 while
#   worker is alive and heartbeating. Master's handleHeartbeat checks
#   ValidateSessionByID on every heartbeat — sees revoked=true, tears
#   the session down via writerErr + cancel().
# Expected: activeJobsCount cleared, claimed tasks return to READY via
#   reaper.
# Invariants: NR-1 (no double RUNNING), NR-7 (reaper mints new attempt).
# =============================================================================
set -uo pipefail
SCENARIO_ID="08-cert-revoked"
EVIDENCE_DIR="${EVIDENCE_DIR:?EVIDENCE_DIR not set}"
DB="${DB:?DB not set}"

# shellcheck disable=SC1091
source "$(dirname "$0")/../lib.sh"
# shellcheck disable=SC1091
source "$(dirname "$0")/../invariants.sh"

rm_begin_scenario "$SCENARIO_ID"
rm_info "[$SCENARIO_ID] starting"
NOW="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
TASK_ID="task-08-$RANDOM"
JOB_ID="job-08-$RANDOM"
ATTEMPT_ID="att-08-$RANDOM"
LEASE_ID="lease-08-$RANDOM"
SESSION_ID="sess-08-$RANDOM"
WORKER_ID="worker-revoked-08"

# Seed: TLS handshake succeeded, job is RUNNING.
sqlite3 "$DB" <<SQL
INSERT INTO worker_flags (worker_id, revoked, quarantined, raw_json, migrated_at)
VALUES ('$WORKER_ID', 0, 0,
        json_object('worker_id','$WORKER_ID','revoked',0,'updated_at','$NOW'),
        '$NOW');
INSERT INTO worker_sessions
  (session_id, worker_id, token_hash, ip_address, created_at,
   expires_at, last_seen, revoked)
VALUES ('$SESSION_ID', '$WORKER_ID', 'preimage-hash-08',
        '127.0.0.1', '$NOW',
        date('now', '+24 hours'), '$NOW', 0);
INSERT INTO jobs (job_id, status, revision, video_name, started_at,
                  created_at, updated_at)
VALUES ('$JOB_ID', 'RUNNING', 5, 'recovery-matrix-08', '$NOW', '$NOW', '$NOW');
INSERT INTO tasks (task_id, job_id, status, revision, attempt_count,
                  worker_id, lease_id,
                  lease_expires_at, started_at, created_at, updated_at)
VALUES ('$TASK_ID', '$JOB_ID', 'RUNNING', 5, 1,
        '$WORKER_ID', '$LEASE_ID',
        date('now', '+30 minutes'), '$NOW', '$NOW', '$NOW');
INSERT INTO task_attempts (id, task_id, job_id, attempt_number, worker_id,
                          lease_id, status, revision,
                          started_at, created_at, updated_at)
VALUES ('$ATTEMPT_ID', '$TASK_ID', '$JOB_ID', 1, '$WORKER_ID',
        '$LEASE_ID', 'RUNNING', 0, '$NOW', '$NOW', '$NOW');
SQL

# Operator action: revoke both worker_flags AND worker_sessions in DB.
sqlite3 "$DB" <<SQL
UPDATE worker_flags SET revoked=1,
  raw_json=json_object('worker_id','$WORKER_ID','revoked',1,'updated_at','$NOW')
WHERE worker_id='$WORKER_ID';
UPDATE worker_sessions SET revoked=1 WHERE session_id='$SESSION_ID';
SQL

# Master's next heartbeat handler will detect: writerErr <- "session revoked";
# cancel() forces all senders off. Reaper takes back the lease.
PAST="$(date -u -d '-31 minutes' +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -v-31M +%Y-%m-%dT%H:%M:%SZ)"
sqlite3 "$DB" <<SQL
UPDATE tasks SET lease_expires_at='$PAST' WHERE task_id='$TASK_ID';
SQL

# Simulate reaper pass.
NEW_LEASE="lease-NEW-$RANDOM"
NEW_ATT="att-NEW-$RANDOM"
sqlite3 "$DB" <<SQL
UPDATE tasks SET status='READY',
  worker_id='', lease_id='', lease_expires_at=NULL,
  attempt_id=NULL, attempt_number=NULL,
  revision=revision+1, updated_at='$NOW'
WHERE task_id='$TASK_ID';
UPDATE task_attempts SET status='TIMED_OUT', completed_at='$NOW',
  error_code='LEASE_EXPIRED',
  error_message='recovery-matrix cert-revoked reap',
  updated_at='$NOW'
WHERE id='$ATTEMPT_ID';
SQL

sqlite3 -separator '|' -header "$DB" "
  SELECT 'worker_flags' tbl, json_extract(raw_json,'\$.revoked') AS revoked
  FROM worker_flags WHERE worker_id='$WORKER_ID'
  UNION ALL SELECT 'worker_sessions', revoked
  FROM worker_sessions WHERE session_id='$SESSION_ID'
  UNION ALL SELECT 'tasks', status FROM tasks WHERE task_id='$TASK_ID'" \
  >"$EVIDENCE_DIR/08-revoke-state.txt"

rm_assert_invariant "$DB" "NR-1" 0
rm_assert_invariant "$DB" "NR-7" 0
rm_end_scenario "$SCENARIO_ID" "cert/session revoked; master-side heartbeat teardown"
rm_info "[$SCENARIO_ID] done"
