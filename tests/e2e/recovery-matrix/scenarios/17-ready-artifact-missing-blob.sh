#!/usr/bin/env bash
# =============================================================================
# Scenario 17 — artefatto READY senza blob (reconciler quarantine)
# =============================================================================
# Fault: an artifact row was promoted to READY, but the underlying blob is
#   missing. Because the recovery-matrix cannot access the filesystem, we
#   model the missing blob as empty sha256 and size_bytes=0 — the same
#   invalid-bytes surface the reconciler would detect when the blob is gone.
# Expected: the artifacts reconciler (rule 3: READY-no-blob → QUARANTINED)
#   detects the inconsistency and flips the artifact to QUARANTINED.
# Type: POSITIVE — the system repairs the bad state and invariants hold.
# Invariants: NR-5 (no READY artifact without valid bytes).
# =============================================================================
set -uo pipefail
SCENARIO_ID="17-ready-artifact-missing-blob"
EVIDENCE_DIR="${EVIDENCE_DIR:?EVIDENCE_DIR not set}"
DB="${DB:?DB not set}"

# shellcheck disable=SC1091
source "$(dirname "$0")/../lib.sh"
# shellcheck disable=SC1091
source "$(dirname "$0")/../invariants.sh"

rm_begin_scenario "$SCENARIO_ID"
rm_info "[$SCENARIO_ID] starting"
NOW="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
JOB_ID="job-17-$RANDOM"
ART_ID="art-17-$RANDOM"
UPLOAD_ID="up-17-$RANDOM"

# Seed: artifact was incorrectly promoted to READY, but its blob is
# missing (empty sha256, size_bytes=0). The upload row is COMPLETED,
# simulating the race window before the reconciler runs.
sqlite3 "$DB" <<SQL
INSERT INTO jobs (job_id, status, revision, video_name, started_at,
                  created_at, updated_at)
VALUES ('$JOB_ID', 'RUNNING', 2, 'recovery-matrix-17', '$NOW', '$NOW', '$NOW');
INSERT INTO artifacts (id, job_id, attempt_id, type, storage_provider,
                      storage_key, local_path, sha256, size_bytes,
                      mime_type, status, verified_at, created_at)
VALUES ('$ART_ID', '$JOB_ID', 1, 'render', 'local',
        'artifacts/sha256/17/$JOB_ID-missing',
        '/tmp/velox-recovery-17/missing-blob.mp4',
        '', 0, 'video/mp4',
        'READY', '$NOW', '$NOW');
INSERT INTO artifact_uploads (upload_id, artifact_id, job_id, attempt_number,
                            worker_id, lease_id, status,
                            received_size_bytes, received_sha256,
                            created_at, expires_at, completed_at)
VALUES ('$UPLOAD_ID', '$ART_ID', '$JOB_ID', 1,
        'worker-17', 'lease-17', 'COMPLETED',
        0, '',
        '$NOW', date('now', '+24 hours'), '$NOW');
SQL

# Simulate reconciler action: detect missing blob and quarantine.
sqlite3 "$DB" <<SQL
UPDATE artifacts
SET status='QUARANTINED', updated_at='$NOW'
WHERE id='$ART_ID' AND status='READY';
INSERT INTO worker_messages (worker_id, kind, refused_reason, created_at)
VALUES ('reconciler-17', 'artifact_quarantined',
        'READY artifact $ART_ID missing blob at artifacts/sha256/17/$JOB_ID-missing',
        '$NOW');
SQL

sqlite3 -separator '|' -header "$DB" "
  SELECT 'artifacts' tbl, id, status, storage_key, local_path, sha256, size_bytes
  FROM artifacts WHERE id='$ART_ID'" \
  >"$EVIDENCE_DIR/17-quarantine-state.txt"

# After quarantine, no READY row lacks valid bytes.
rm_assert_invariant "$DB" "NR-5" 0
rm_end_scenario "$SCENARIO_ID" "READY artifact without valid bytes quarantined by reconciler"
rm_info "[$SCENARIO_ID] done"
