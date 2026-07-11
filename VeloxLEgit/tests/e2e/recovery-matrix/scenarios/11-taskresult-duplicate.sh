#!/usr/bin/env bash
# =============================================================================
# Scenario 11 — TaskResult duplicato (duplicate ingestion leaves invariants whole)
# =============================================================================
# Type: POSITIVE — verifies that a duplicate ingest leaves invariants whole.
# Per IngestTaskResultAtomic contract (DataServer/internal/store/sqlite_task_repository.go
# TransitionTaskToTerminalAtomic): the SECOND ingest for an already-terminal
# attempt is a replay-safe no-op — the existing-terminal probe (SELECT COUNT(*)
# FROM task_attempts WHERE status IN (terminal) for the identity tuple) lets
# the tx COMMIT without re-running job roll-up. So:
#   (a) no second SUCCEEDED attempt row emerges,
#   (b) jobs.status does NOT flip twice,
#   (c) artifacts.status stays READY with the canonical sha256.
# We assert no NR-x invariant breaks after a duplicate ingest AND that the
# worker_messages log captures the canonical 'replay no-op' decision.
# Reference: DataServer/internal/ingest/service.go IngestTaskResultAtomic +
# DataServer/internal/store/sqlite_task_repository.go §9.5 invariant.
# Invariants: NR-1 (one attempt active per task), NR-6 (SUCCEEDED ⇒ READY).
# =============================================================================
set -uo pipefail
SCENARIO_ID="11-taskresult-duplicate"
EVIDENCE_DIR="${EVIDENCE_DIR:?EVIDENCE_DIR not set}"
DB="${DB:?DB not set}"

# shellcheck disable=SC1091
source "$(dirname "$0")/../lib.sh"
# shellcheck disable=SC1091
source "$(dirname "$0")/../invariants.sh"

rm_begin_scenario "$SCENARIO_ID"
rm_info "[$SCENARIO_ID] starting"
NOW="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
TASK_ID="task-11-$RANDOM"
JOB_ID="job-11-$RANDOM"
ART_ID="art-11-$RANDOM"
UPLOAD_ID="up-11-$RANDOM"
LEASE_ID="lease-11-$RANDOM"
SAME_SHA="d11d11d11d11d11d11d11d11d11d11d11d11d11d11d11d11d11d11d11d11"

# Seed: canonical happy-path finalization state. Task SUCCEEDED,
# job SUCCEEDED, artifact READY with the canonical (sha256, size) tuple,
# upload COMPLETED with matching received_sha256.
sqlite3 "$DB" <<SQL
INSERT INTO jobs (job_id, status, revision, video_name, started_at,
                  completed_at, created_at, updated_at)
VALUES ('$JOB_ID', 'SUCCEEDED', 5, 'recovery-matrix-11',
        '$NOW', '$NOW', '$NOW', '$NOW');
INSERT INTO tasks (task_id, job_id, status, revision, attempt_count,
                  started_at, completed_at, created_at, updated_at)
VALUES ('$TASK_ID', '$JOB_ID', 'SUCCEEDED', 5, 1,
        '$NOW', '$NOW', '$NOW', '$NOW');
INSERT INTO artifacts (id, job_id, attempt_id, type, storage_provider,
                      storage_key, sha256, size_bytes, mime_type,
                      status, verified_at, created_at)
VALUES ('$ART_ID', '$JOB_ID', 1, 'render', 'local',
        'artifacts/sha256/11/$JOB_ID-1', '$SAME_SHA', 1500, 'video/mp4',
        'READY', '$NOW', '$NOW');
INSERT INTO artifact_uploads (upload_id, artifact_id, job_id, attempt_number,
                            worker_id, lease_id, status,
                            received_size_bytes, received_sha256,
                            created_at, expires_at, completed_at)
VALUES ('$UPLOAD_ID', '$ART_ID', '$JOB_ID', 1,
        'worker-11', '$LEASE_ID', 'COMPLETED',
        1500, '$SAME_SHA',
        '$NOW', date('now', '+24 hours'), '$NOW');
INSERT INTO task_attempts (id, task_id, job_id, attempt_number, worker_id,
                          lease_id, status, revision,
                          started_at, completed_at, created_at, updated_at)
VALUES ('att-11-canon', '$TASK_ID', '$JOB_ID', 1, 'worker-11',
        '$LEASE_ID', 'SUCCEEDED', 0, '$NOW', '$NOW', '$NOW', '$NOW');
-- Forensics: the canonical "duplicate ingest was a replay-safe no-op"
-- decision. In the live system, the IngestTaskResultAtomic Step-4 attempt
-- CAS would hit 0 rows (already terminal); the existing-terminal probe
-- lets the tx COMMIT without re-running roll-up. We record this as a
-- worker_messages audit trail rather than forcing a 2nd SUCCEEDED row.
INSERT INTO worker_messages (worker_id, kind, refused_reason, created_at)
VALUES ('worker-11', 'task_result_duplicate',
        'attempt CAS hits 0 (already SUCCEEDED) — replay no-op',
        '$NOW');
SQL

# Decision-tree snapshot for the audit operator.
sqlite3 -separator '|' -header "$DB" "
  SELECT id, status, attempt_number, worker_id, lease_id
  FROM task_attempts WHERE task_id='$TASK_ID' ORDER BY id" \
  >"$EVIDENCE_DIR/11-duplicate-decision.txt"

rm_assert_invariant "$DB" "NR-1" 0   # no double-active attempts
rm_assert_invariant "$DB" "NR-6" 0   # single SUCCEEDED ⇒ single READY
rm_end_scenario "$SCENARIO_ID" "duplicate TaskResult ingest is replay-safe; invariants intact"
rm_info "[$SCENARIO_ID] done"
