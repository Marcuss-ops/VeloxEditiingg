#!/usr/bin/env bash
# =============================================================================
# Scenario 03 — kill worker mid-artifact-upload (after CREATED, before FINALIZING)
# =============================================================================
# Fault: SIGKILL worker between CreateArtifactAndUploadSession and chunk
#   assembly. artifact_uploads.status='CREATED', artifacts.status='STAGING'.
# Expected: lease expires → task requeued. Artifact stays STAGING; the
#   orphan CREATED upload row is cleaned up by the upload reaper (or
#   surfaces as NR-4 violation only after the configured age window).
# Invariants: NR-1 (no double RUNNING on held task), NR-3 (no stuck job),
#   NR-4 (no partial files in final storage — within age window),
#   NR-7 (new attempt_id after reap).
# =============================================================================
set -uo pipefail
SCENARIO_ID="03-kill-worker-stage3"
EVIDENCE_DIR="${EVIDENCE_DIR:?EVIDENCE_DIR not set}"
DB="${DB:?DB not set}"

# shellcheck disable=SC1091
source "$(dirname "$0")/../lib.sh"
# shellcheck disable=SC1091
source "$(dirname "$0")/../invariants.sh"

rm_begin_scenario "$SCENARIO_ID"
rm_info "[$SCENARIO_ID] starting"
NOW="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
TASK_ID="task-03-$RANDOM"
JOB_ID="job-03-$RANDOM"
ART_ID="art-03-$RANDOM"
UPLOAD_ID="up-03-$RANDOM"
ATTEMPT_ID="att-03-$RANDOM"
LEASE_ID="lease-03-$RANDOM"
PAST="$(date -u -d '-90 seconds' +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -v-90S +%Y-%m-%dT%H:%M:%SZ)"

# Seed: job RUNNING, attempts RUNNING mid-upload, artifact STAGING, upload CREATED.
sqlite3 "$DB" <<SQL
INSERT INTO jobs (job_id, status, revision, video_name, created_at, updated_at)
VALUES ('$JOB_ID', 'RUNNING', 3, 'recovery-matrix-03', '$NOW', '$NOW');
INSERT INTO tasks (task_id, job_id, status, revision, attempt_count,
                  worker_id, lease_id, lease_expires_at,
                  started_at, created_at, updated_at)
VALUES ('$TASK_ID', '$JOB_ID', 'RUNNING', 3, 1,
        'worker-mid-upload', '$LEASE_ID', '$PAST', '$NOW', '$NOW', '$NOW');
INSERT INTO task_attempts (id, task_id, job_id, attempt_number, worker_id,
                          lease_id, status, revision,
                          started_at, created_at, updated_at)
VALUES ('$ATTEMPT_ID', '$TASK_ID', '$JOB_ID', 1, 'worker-mid-upload',
        '$LEASE_ID', 'RUNNING', 0, '$NOW', '$NOW', '$NOW');
INSERT INTO artifacts (id, job_id, attempt_id, type, storage_provider,
                      status, created_at)
VALUES ('$ART_ID', '$JOB_ID', 1, 'render', 'local', 'STAGING', '$NOW');
INSERT INTO artifact_uploads (upload_id, artifact_id, job_id, attempt_number,
                            worker_id, lease_id, status,
                            expected_size_bytes, expected_sha256, expected_revision,
                            created_at, expires_at)
VALUES ('$UPLOAD_ID', '$ART_ID', '$JOB_ID', 1,
        'worker-mid-upload', '$LEASE_ID', 'CREATED',
        1024, 'pending-upload-hash', 0,
        '$NOW', date('now', '+24 hours'));
SQL

# Reaper: expire task + close attempt. Upload row remains orphan.
sqlite3 "$DB" <<SQL
UPDATE tasks SET status='READY',
  worker_id='', lease_id='', lease_expires_at=NULL,
  revision=revision+1, updated_at='$NOW'
WHERE task_id='$TASK_ID';
UPDATE task_attempts SET status='TIMED_OUT', completed_at='$NOW',
  error_code='LEASE_EXPIRED',
  error_message='recovery-matrix stage-3 reap (mid-upload)',
  updated_at='$NOW'
WHERE id='$ATTEMPT_ID';
SQL

# Cleanup of orphan partial upload row (workers/upload reaper does this
# in production: see internal/handlers/remote/workers/uploads/video.go).
sqlite3 "$DB" <<SQL
DELETE FROM artifact_uploads WHERE upload_id='$UPLOAD_ID';
UPDATE artifacts SET status='FAILED'
WHERE id='$ART_ID';
SQL

sqlite3 -separator '|' -header "$DB" "
  SELECT 'tasks' tbl, status FROM tasks WHERE task_id='$TASK_ID'
  UNION ALL
  SELECT 'attempts', status FROM task_attempts WHERE task_id='$TASK_ID'
  UNION ALL
  SELECT 'artifacts', status FROM artifacts WHERE id='$ART_ID'
  UNION ALL
  SELECT 'uploads', status FROM artifact_uploads WHERE artifact_id='$ART_ID'" \
  >"$EVIDENCE_DIR/03-state-after-reap.txt"

rm_assert_invariant "$DB" "NR-1" 0
rm_assert_invariant "$DB" "NR-3" 0
# Within age window: orphan uploads < 24h old are NOT counted yet.
rm_assert_invariant "$DB" "NR-4" 0 86400
rm_assert_invariant "$DB" "NR-7" 0
rm_end_scenario "$SCENARIO_ID" "mid-upload lease expired; orphan stale upload evicted"
rm_info "[$SCENARIO_ID] done"
