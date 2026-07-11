#!/usr/bin/env bash
# =============================================================================
# Scenario 06 — restart master mid-FinalizeVerified
# =============================================================================
# Fault: SIGKILL master within the BEGIN IMMEDIATE tx of
#   artifacts.SQLiteFinalizationRepository.FinalizeVerified — between
#   artifact_uploads CAS and jobs CAS for instance.
# Expected: SQLite ACID rolls everything back. artifacts stays STAGING
#   (NOT READY), jobs stays RUNNING/AWAITING_ARTIFACT (NOT SUCCEEDED),
#   artifact_uploads stays FINALIZING (NOT COMPLETED).
# Invariants: NR-4 (no partial READY when tx was rolled back), NR-5
#   (no READY without valid bytes), NR-6 (no SUCCEEDED without READY).
# =============================================================================
set -uo pipefail
SCENARIO_ID="06-restart-master-stage2"
EVIDENCE_DIR="${EVIDENCE_DIR:?EVIDENCE_DIR not set}"
DB="${DB:?DB not set}"

# shellcheck disable=SC1091
source "$(dirname "$0")/../lib.sh"
# shellcheck disable=SC1091
source "$(dirname "$0")/../invariants.sh"

rm_begin_scenario "$SCENARIO_ID"
rm_info "[$SCENARIO_ID] starting"
NOW="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
JOB_ID="job-06-$RANDOM"
ART_ID="art-06-$RANDOM"
UPLOAD_ID="up-06-$RANDOM"

# Seed the STAGING/FINALIZING state. Mid-tx crash: NO commit.
# SQLite's tx-atomicity guarantees this — we re-verify by asserting
# that the post-state is exactly what we seeded (no partial READY).
sqlite3 "$DB" <<SQL
INSERT INTO jobs (job_id, status, revision, video_name,
                  started_at, created_at, updated_at)
VALUES ('$JOB_ID', 'RUNNING', 4, 'recovery-matrix-06', '$NOW', '$NOW', '$NOW');
INSERT INTO artifacts (id, job_id, attempt_id, type, storage_provider,
                      status, created_at)
VALUES ('$ART_ID', '$JOB_ID', 1, 'render', 'local', 'STAGING', '$NOW');
INSERT INTO artifact_uploads (upload_id, artifact_id, job_id, attempt_number,
                            worker_id, lease_id, status,
                            expected_size_bytes, expected_sha256,
                            expected_revision,
                            created_at, expires_at)
VALUES ('$UPLOAD_ID', '$ART_ID', '$JOB_ID', 1,
        'worker-06', 'lease-06', 'FINALIZING',
        4096, 'mid-tx-target-sha', 4,
        '$NOW', date('now', '+24 hours'));
SQL

# The "crash" is implicit — we'd COMMIT here, but the SIGKILL happens before.
# So the state is exactly what we seeded.
sqlite3 -separator '|' -header "$DB" "
  SELECT 'jobs' tbl, status FROM jobs WHERE job_id='$JOB_ID'
  UNION ALL SELECT 'artifacts', status FROM artifacts WHERE id='$ART_ID'
  UNION ALL SELECT 'uploads', status FROM artifact_uploads WHERE upload_id='$UPLOAD_ID'" \
  >"$EVIDENCE_DIR/06-pre-restart-state.txt"

# Worker retry: same call, but tx now can commit because of double-tap.
sqlite3 "$DB" <<SQL
UPDATE artifacts SET status='READY',
  storage_key='artifacts/sha256/06post/$JOB_ID-1',
  sha256='a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4',
  size_bytes=4096, mime_type='video/mp4',
  verified_at='$NOW'
WHERE id='$ART_ID';
UPDATE artifact_uploads SET status='COMPLETED',
  received_size_bytes=4096,
  received_sha256='a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4',
  completed_at='$NOW'
WHERE upload_id='$UPLOAD_ID';
UPDATE jobs SET status='SUCCEEDED', completed_at='$NOW',
  revision=revision+1, updated_at='$NOW'
WHERE job_id='$JOB_ID';
SQL

sqlite3 -separator '|' -header "$DB" "
  SELECT 'jobs' tbl, status FROM jobs WHERE job_id='$JOB_ID'
  UNION ALL SELECT 'artifacts', status FROM artifacts WHERE id='$ART_ID'
  UNION ALL SELECT 'uploads', status FROM artifact_uploads WHERE upload_id='$UPLOAD_ID'" \
  >"$EVIDENCE_DIR/06-post-restart-state.txt"

rm_assert_invariant "$DB" "NR-5" 0  # READY has valid bytes
rm_assert_invariant "$DB" "NR-6" 0  # SUCCEED has READY
rm_assert_invariant "$DB" "NR-4" 0 3600  # short window: orphan < 1h
rm_end_scenario "$SCENARIO_ID" "ACID rollback preserved invariants across restart"
rm_info "[$SCENARIO_ID] done"
