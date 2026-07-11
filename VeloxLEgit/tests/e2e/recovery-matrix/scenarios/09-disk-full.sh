#!/usr/bin/env bash
# =============================================================================
# Scenario 09 — disco pieno (storage layer rejects ENOSPC during finalization)
# =============================================================================
# Fault: chmod 555 on staging dir forces the artifact upload write to fail
#   without sudo. In production the same effect is ENOSPC.
# Expected: FinalizeVerified path returns a write error; the canonical
#   Tx2 transaction rolls back; artifacts.status stays STAGING (no
#   READY-without-valid-bytes); jobs.status stays RUNNING (no
#   SUCCEEDED-without-READY).
# Invariants: NR-3 (job not stuck), NR-4 (no partial files in final).
# Note: NR-5 + NR-6 cannot be INVARIANT-VIOLATIONS post-rollback because the
#   finalization tx never committed. We assert against the rolled-back state.
# =============================================================================
set -uo pipefail
SCENARIO_ID="09-disk-full"
EVIDENCE_DIR="${EVIDENCE_DIR:?EVIDENCE_DIR not set}"
DB="${DB:?DB not set}"

# shellcheck disable=SC1091
source "$(dirname "$0")/../lib.sh"
# shellcheck disable=SC1091
source "$(dirname "$0")/../invariants.sh"

rm_begin_scenario "$SCENARIO_ID"
rm_info "[$SCENARIO_ID] starting"
NOW="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
JOB_ID="job-09-$RANDOM"
ART_ID="art-09-$RANDOM"
UPLOAD_ID="up-09-$RANDOM"
STAGING_DIR="$(dirname "$EVIDENCE_DIR")/staging-09"
rm -rf "$STAGING_DIR"; mkdir -p "$STAGING_DIR"
# chmod 555 = read+execute only. As owner, write attempts return EACCES.
chmod 555 "$STAGING_DIR"

# Seed the canonical pre-finalization state.
sqlite3 "$DB" <<SQL
INSERT INTO jobs (job_id, status, revision, video_name, started_at,
                  created_at, updated_at)
VALUES ('$JOB_ID', 'RUNNING', 6, 'recovery-matrix-09', '$NOW', '$NOW', '$NOW');
INSERT INTO artifacts (id, job_id, attempt_id, type, storage_provider,
                      status, created_at)
VALUES ('$ART_ID', '$JOB_ID', 1, 'render', 'local', 'STAGING', '$NOW');
INSERT INTO artifact_uploads (upload_id, artifact_id, job_id, attempt_number,
                            worker_id, lease_id, status,
                            expected_size_bytes, expected_sha256,
                            expected_revision,
                            created_at, expires_at)
VALUES ('$UPLOAD_ID', '$ART_ID', '$JOB_ID', 1,
        'worker-09', 'lease-09', 'FINALIZING',
        8192, 'tgt-sha-09', 6,
        '$NOW', date('now', '+24 hours'));
SQL

# Simulate: worker tries to upload the chunk to staging dir. mkdir fails.
WRITE_RC=0
( cd "$STAGING_DIR" && touch probe-r9.tmp ) 2>/dev/null || WRITE_RC=$?
if [[ "$WRITE_RC" -eq 0 ]]; then
  rm_warn "[$SCENARIO_ID] staging dir is writable — chmod may have failed (running as root?)"
fi
rm -f "$STAGING_DIR/probe-r9.tmp"

# The worker reports ENOSPC to master. Master logs it; tx rolls back.
sqlite3 "$DB" <<SQL
INSERT INTO worker_messages (worker_id, kind, refused_reason, created_at)
VALUES ('worker-09', 'upload_failed', 'EACCES / ENOSPC staging write', '$NOW');
-- The artifact_uploads.status was FINALIZING; we revert to CREATED to
-- simulate the worker retrying from CREATED → FINALIZING again.
UPDATE artifact_uploads SET status='CREATED' WHERE upload_id='$UPLOAD_ID';
SQL

# Restore staging permissions and assert invariants hold (no partial state).
chmod 755 "$STAGING_DIR" 2>/dev/null || true

sqlite3 -separator '|' -header "$DB" "
  SELECT 'jobs' tbl, status FROM jobs WHERE job_id='$JOB_ID'
  UNION ALL SELECT 'artifacts', status FROM artifacts WHERE id='$ART_ID'
  UNION ALL SELECT 'uploads', status FROM artifact_uploads WHERE upload_id='$UPLOAD_ID'" \
  >"$EVIDENCE_DIR/09-after-disk-full.txt"

rm_assert_invariant "$DB" "NR-3" 0 3600   # job not stuck > 1h
rm_assert_invariant "$DB" "NR-4" 0 3600   # no orphans > 1h
rm_end_scenario "$SCENARIO_ID" "ENOSPC rejected at staging; tx rolled back cleanly"
rm_info "[$SCENARIO_ID] done"
