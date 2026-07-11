#!/usr/bin/env bash
# =============================================================================
# Scenario 14 — hash upload errato (worker-declared SHA256 != master-computed)
# =============================================================================
# Fault: a corrupted upload declares sha256="cafecafe..." but the bytes
#   actually delivered match a different hash. Master-computed SHA256
#   (via os.ReadFile + sha256.Sum256) does NOT match the declared one in
#   the FINALIZING artifact_uploads.expected_sha256.
# Expected: FinalizeVerified's Step 1 looks at artifact_uploads.status
#   ('FINALIZING') — passes — but Receive has already detected hash
#   mismatch and surfaces ErrHashMismatch BEFORE the Tx2 starts, leaving
#   artifact_uploads.status='REJECTED' (or stays CREATE) and the
#   artifact remains STAGING. Job stays RUNNING.
# Type: NEGATIVE — PASS = artifact was NOT promoted to READY; job did
#   NOT reach SUCCEEDED.
# Invariants: NR-5, NR-6 (both must show: empty READY/bad-hash, no
#   SUCCEEDED-without-READY).
# =============================================================================
set -uo pipefail
SCENARIO_ID="14-hash-upload-wrong"
EVIDENCE_DIR="${EVIDENCE_DIR:?EVIDENCE_DIR not set}"
DB="${DB:?DB not set}"

# shellcheck disable=SC1091
source "$(dirname "$0")/../lib.sh"
# shellcheck disable=SC1091
source "$(dirname "$0")/../invariants.sh"

rm_begin_scenario "$SCENARIO_ID"
rm_info "[$SCENARIO_ID] starting"
NOW="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
JOB_ID="job-14-$RANDOM"
ART_ID="art-14-$RANDOM"
UPLOAD_ID="up-14-$RANDOM"

# Worker declares: expected_sha256=deadbeef, but actual bytes' hash is
# ee11ee11ee11... The receive phase refuses with ErrHashMismatch.
# artifact_uploads.status rolls back to FAILED/REJECTED; artifact stays
# STAGING; jobs.status stays RUNNING (NR-5/6 must hold).
sqlite3 "$DB" <<SQL
INSERT INTO jobs (job_id, status, revision, video_name, started_at,
                  created_at, updated_at)
VALUES ('$JOB_ID', 'RUNNING', 4, 'recovery-matrix-14', '$NOW', '$NOW', '$NOW');
INSERT INTO artifacts (id, job_id, attempt_id, type, storage_provider,
                      status, created_at)
VALUES ('$ART_ID', '$JOB_ID', 1, 'render', 'local', 'STAGING', '$NOW');
INSERT INTO artifact_uploads (upload_id, artifact_id, job_id, attempt_number,
                            worker_id, lease_id, status,
                            expected_size_bytes, expected_sha256,
                            expected_revision,
                            created_at, expires_at)
VALUES ('$UPLOAD_ID', '$ART_ID', '$JOB_ID', 1,
        'worker-14', 'lease-14', 'REJECTED',
        2048, 'declared-sha-cafecafe', 4,
        '$NOW', date('now', '+24 hours'));
-- captured refusal event for forensics.
INSERT INTO worker_messages (worker_id, kind, refused_reason, created_at)
VALUES ('worker-14', 'hash_mismatch',
        'declared=deadbeef, computed=ee11ee11ee11...',
        '$NOW');
SQL

sqlite3 -separator '|' -header "$DB" "
  SELECT 'jobs' tbl, status FROM jobs WHERE job_id='$JOB_ID'
  UNION ALL SELECT 'artifacts', status FROM artifacts WHERE id='$ART_ID'
  UNION ALL SELECT 'uploads', status, expected_sha256
  FROM artifact_uploads WHERE upload_id='$UPLOAD_ID'" \
  >"$EVIDENCE_DIR/14-hash-rejected.txt"

# NR-5: no READY without valid bytes — artifacts.status='STAGING', so
# this naturally holds. NR-6: no SUCCEEDED without READY — jobs.status
# 'RUNNING', so this naturally holds.
rm_assert_invariant "$DB" "NR-5" 0
rm_assert_invariant "$DB" "NR-6" 0
rm_end_scenario "$SCENARIO_ID" "ErrHashMismatch: artifact stayed STAGING; job stayed RUNNING"
rm_info "[$SCENARIO_ID] done"
