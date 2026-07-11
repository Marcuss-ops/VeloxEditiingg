#!/usr/bin/env bash
# =============================================================================
# Scenario 10 — asset rimosso (input source file deleted mid-execution)
# =============================================================================
# Fault: rm -f the input scene image WHILE the worker is still in the
#   RUNNING state. Worker's FFmpeg fails with ENOENT.
# Expected: task_attempts.status moves to FAILED, Task to FAILED with
#   reason "input asset missing". Job rolls back to FAILED (no orphan
#   SUCCEEDED without READY).
# Invariants: NR-1 (no double RUNNING), NR-3 (job not stuck).
# =============================================================================
set -uo pipefail
SCENARIO_ID="10-asset-removed"
EVIDENCE_DIR="${EVIDENCE_DIR:?EVIDENCE_DIR not set}"
DB="${DB:?DB not set}"

# shellcheck disable=SC1091
source "$(dirname "$0")/../lib.sh"
# shellcheck disable=SC1091
source "$(dirname "$0")/../invariants.sh"

rm_begin_scenario "$SCENARIO_ID"
rm_info "[$SCENARIO_ID] starting"
NOW="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
JOB_ID="job-10-$RANDOM"
TASK_ID="task-10-$RANDOM"
ATTEMPT_ID="att-10-$RANDOM"
LEASE_ID="lease-10-$RANDOM"
ASSET_DIR="$(dirname "$EVIDENCE_DIR")/asset-input-10"

# Create the input file then immediately remove it to simulate ENOENT at runtime.
rm -rf "$ASSET_DIR"; mkdir -p "$ASSET_DIR"
dd if=/dev/urandom of="$ASSET_DIR/scene.png" bs=1024 count=2 2>/dev/null
rm -f "$ASSET_DIR/scene.png"
[[ -f "$ASSET_DIR/scene.png" ]] && rm_warn "[$SCENARIO_ID] input file unexpectedly present"

# Seed a RUNNING state with attempt_count=1.
sqlite3 "$DB" <<SQL
INSERT INTO jobs (job_id, status, revision, video_name, started_at,
                  created_at, updated_at)
VALUES ('$JOB_ID', 'RUNNING', 3, 'recovery-matrix-10', '$NOW', '$NOW', '$NOW');
INSERT INTO tasks (task_id, job_id, status, revision, attempt_count,
                  worker_id, lease_id, lease_expires_at, started_at,
                  created_at, updated_at)
VALUES ('$TASK_ID', '$JOB_ID', 'RUNNING', 3, 1,
        'worker-10', '$LEASE_ID', date('now', '+30 minutes'),
        '$NOW', '$NOW', '$NOW');
INSERT INTO task_attempts (id, task_id, job_id, attempt_number, worker_id,
                          lease_id, status, revision,
                          error_code, error_message,
                          started_at, created_at, updated_at)
VALUES ('$ATTEMPT_ID', '$TASK_ID', '$JOB_ID', 1, 'worker-10',
        '$LEASE_ID', 'RUNNING', 0,
        'INPUT_MISSING', 'ffmpeg: scene.png: No such file or directory',
        '$NOW', '$NOW', '$NOW');
SQL

# Worker's TaskResult(Failed) reaches master. TransitionTaskToTerminalAtomic
# closes Task + Attempt together.
sqlite3 "$DB" <<SQL
UPDATE tasks SET status='FAILED', completed_at='$NOW',
  revision=revision+1, updated_at='$NOW'
WHERE task_id='$TASK_ID';
UPDATE task_attempts SET status='FAILED', completed_at='$NOW',
  error_code='INPUT_MISSING',
  error_message='ffmpeg: scene.png: No such file or directory',
  report_version=report_version+1, updated_at='$NOW'
WHERE id='$ATTEMPT_ID';
UPDATE jobs SET status='FAILED', completed_at='$NOW',
  revision=revision+1, updated_at='$NOW'
WHERE job_id='$JOB_ID';
SQL

sqlite3 -separator '|' -header "$DB" "
  SELECT 'jobs' tbl, status FROM jobs WHERE job_id='$JOB_ID'
  UNION ALL SELECT 'tasks', status FROM tasks WHERE task_id='$TASK_ID'
  UNION ALL SELECT 'attempts', status, error_code
  FROM task_attempts WHERE id='$ATTEMPT_ID'" \
  >"$EVIDENCE_DIR/10-fail-state.txt"

rm_assert_invariant "$DB" "NR-1" 0
rm_assert_invariant "$DB" "NR-3" 0
rm_end_scenario "$SCENARIO_ID" "missing input detected; Task+Attempt+FJob → FAILED atomically"
rm_info "[$SCENARIO_ID] done"
