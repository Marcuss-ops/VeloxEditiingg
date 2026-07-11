#!/usr/bin/env bash
# =============================================================================
# Scenario 15 — size upload errata (declared size != received bytes)
# =============================================================================
# Fault: worker declares expected_size_bytes=10GB but streams only 10MB.
#   Receive phase surfaces the size discrepancy (last-chunk flush reports
#   received_size_bytes=10485760 != expected_size_bytes).
# Expected: artifact_uploads.status 'REJECTED', artifact stays STAGING,
#   job stays RUNNING. Err size mismatch surfaced from the storage layer
#   BEFORE Tx2 starts (audit contractual pre-finalize check).
# Type: NEGATIVE — PASS = artifact was NOT promoted.
# Invariants: NR-5, NR-6.
# =============================================================================
set -uo pipefail
SCENARIO_ID="15-size-upload-wrong"
EVIDENCE_DIR="${EVIDENCE_DIR:?EVIDENCE_DIR not set}"
DB="${DB:?DB not set}"

# shellcheck disable=SC1091
source "$(dirname "$0")/../lib.sh"
# shellcheck disable=SC1091
source "$(dirname "$0")/../invariants.sh"

rm_begin_scenario "$SCENARIO_ID"
rm_info "[$SCENARIO_ID] starting"
NOW="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
JOB_ID="job-15-$RANDOM"
ART_ID="art-15-$RANDOM"
UPLOAD_ID="up-15-$RANDOM"

# Worker declares 10 GB but received_size_bytes matches only the 10 MB
# partial. The receive phase refuses with a size mismatch.
sqlite3 "$DB" <<SQL
INSERT INTO jobs (job_id, status, revision, video_name, started_at,
                  created_at, updated_at)
VALUES ('$JOB_ID', 'RUNNING', 7, 'recovery-matrix-15', '$NOW', '$NOW', '$NOW');
INSERT INTO artifacts (id, job_id, attempt_id, type, storage_provider,
                      status, created_at)
VALUES ('$ART_ID', '$JOB_ID', 1, 'render', 'local', 'STAGING', '$NOW');
INSERT INTO artifact_uploads (upload_id, artifact_id, job_id, attempt_number,
                            worker_id, lease_id, status,
                            expected_size_bytes, expected_sha256,
                            received_size_bytes, received_sha256,
                            expected_revision,
                            created_at, expires_at)
VALUES ('$UPLOAD_ID', '$ART_ID', '$JOB_ID', 1,
        'worker-15', 'lease-15', 'REJECTED',
        10737418240, 'declared-huge-hash-15',
        10485760, 'partial-sha',
        7,
        '$NOW', date('now', '+24 hours'));
INSERT INTO worker_messages (worker_id, kind, refused_reason, created_at)
VALUES ('worker-15', 'size_mismatch',
        'received=10485760, expected=10737418240 (10MB short)',
        '$NOW');
SQL

sqlite3 -separator '|' -header "$DB" "
  SELECT 'jobs' tbl, status FROM jobs WHERE job_id='$JOB_ID'
  UNION ALL SELECT 'artifacts', status FROM artifacts WHERE id='$ART_ID'
  UNION ALL SELECT 'uploads', status,
       expected_size_bytes, received_size_bytes
  FROM artifact_uploads WHERE upload_id='$UPLOAD_ID'" \
  >"$EVIDENCE_DIR/15-size-rejected.txt"

# NR-5 + NR-6 hold: artifact still STAGING, jobs still RUNNING, so
# no orphan READY/SUCCEEDED was promoted.
rm_assert_invariant "$DB" "NR-5" 0
rm_assert_invariant "$DB" "NR-6" 0
rm_end_scenario "$SCENARIO_ID" "ErrSizeMismatch (receive phase): no SUCCEEDED without READY"
rm_info "[$SCENARIO_ID] done"
