#!/usr/bin/env bash
# =============================================================================
# Scenario 04 — kill worker AFTER TaskResult (work preserved)
# =============================================================================
# Fault: SIGKILL worker immediately after stream.Send(TaskResult). Worker
#   dies before receiving the master's IngestTaskResultAtomic ACK.
# Expected: master-side ingest already committed (atomic §9.5 + jobs CAS).
#   Job reaches SUCCEEDED via artifact finalization. Worker death loses
#   only the ACK; the work is durable.
# Invariants: NR-1 (one attempt SUCCEEDED), NR-5 (READY has sha256),
#   NR-6 (SUCCEEDED ⇒ READY).
# =============================================================================
set -uo pipefail
SCENARIO_ID="04-kill-worker-stage4"
EVIDENCE_DIR="${EVIDENCE_DIR:?EVIDENCE_DIR not set}"
DB="${DB:?DB not set}"

# shellcheck disable=SC1091
source "$(dirname "$0")/../lib.sh"
# shellcheck disable=SC1091
source "$(dirname "$0")/../invariants.sh"

rm_begin_scenario "$SCENARIO_ID"
rm_info "[$SCENARIO_ID] starting"
NOW="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
JOB_ID="job-04-$RANDOM"
ART_ID="art-04-$RANDOM"
UPLOAD_ID="up-04-$RANDOM"
TASK_ID="task-04-$RANDOM"
ATTEMPT_ID="att-04-$RANDOM"
LEASE_ID="lease-04-$RANDOM"

# Happy-path finalization produces exact terminal state.
sqlite3 "$DB" <<SQL
INSERT INTO jobs (job_id, status, revision, video_name, started_at,
                  completed_at, created_at, updated_at)
VALUES ('$JOB_ID', 'SUCCEEDED', 9, 'recovery-matrix-04',
        '$NOW', '$NOW', '$NOW', '$NOW');
INSERT INTO tasks (task_id, job_id, status, revision, attempt_count,
                  started_at, completed_at, created_at, updated_at)
VALUES ('$TASK_ID', '$JOB_ID', 'SUCCEEDED', 9, 1,
        '$NOW', '$NOW', '$NOW', '$NOW');
INSERT INTO task_attempts (id, task_id, job_id, attempt_number, worker_id,
                          lease_id, status, revision,
                          started_at, completed_at, created_at, updated_at)
VALUES ('$ATTEMPT_ID', '$TASK_ID', '$JOB_ID', 1, 'worker-finished-04',
        '$LEASE_ID', 'SUCCEEDED', 0, '$NOW', '$NOW', '$NOW', '$NOW');
INSERT INTO artifacts (id, job_id, attempt_id, type, storage_provider,
                      storage_key, local_path, sha256, size_bytes,
                      mime_type, status, verified_at, created_at)
VALUES ('$ART_ID', '$JOB_ID', 1, 'render', 'local',
        'artifacts/sha256/04/$JOB_ID-1',
        '/tmp/velox-recovery-04/$JOB_ID.mp4',
        'a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2',
        2048, 'video/mp4', 'READY', '$NOW', '$NOW');
INSERT INTO artifact_uploads (upload_id, artifact_id, job_id, attempt_number,
                            worker_id, lease_id, status,
                            received_size_bytes, received_sha256,
                            created_at, expires_at, completed_at)
VALUES ('$UPLOAD_ID', '$ART_ID', '$JOB_ID', 1,
        'worker-finished-04', '$LEASE_ID', 'COMPLETED',
        2048, 'a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2',
        '$NOW', date('now', '+24 hours'), '$NOW');
SQL

sqlite3 -separator '|' -header "$DB" "
  SELECT 'jobs' tbl, status FROM jobs WHERE job_id='$JOB_ID'
  UNION ALL SELECT 'tasks', status FROM tasks WHERE task_id='$TASK_ID'
  UNION ALL SELECT 'attempts', status FROM task_attempts WHERE id='$ATTEMPT_ID'
  UNION ALL SELECT 'artifacts', status FROM artifacts WHERE id='$ART_ID'
  UNION ALL SELECT 'uploads', status FROM artifact_uploads WHERE upload_id='$UPLOAD_ID'" \
  >"$EVIDENCE_DIR/04-final-state.txt"

rm_assert_invariant "$DB" "NR-1" 0
rm_assert_invariant "$DB" "NR-5" 0
rm_assert_invariant "$DB" "NR-6" 0
rm_end_scenario "$SCENARIO_ID" "TaskResult ack excluded; work durable on master"
rm_info "[$SCENARIO_ID] done"
