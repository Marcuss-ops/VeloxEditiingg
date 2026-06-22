-- 051_artifact_uploads_task_id.sql
--
-- fix/task-native-artifact-bridge — single-shape bridge migration that
-- subsumes the previous `051_task_output_artifacts.sql` and adds the
-- task-side primary key that the audit-mandated TaskReport ingestion
-- contract binds to the artifact upload pipeline downstream.
--
-- Schema shape:
--   task_id          TEXT NOT NULL                       (FK to tasks.task_id)
--   attempt_id       TEXT NOT NULL DEFAULT ''            (FK semantics optional,
--                                                         slot kept for forensic
--                                                         cross-references)
--   artifact_id      TEXT NOT NULL                       (worker-claimed id)
--   artifact_type    TEXT NOT NULL DEFAULT ''            (video, audio, ...)
--   declared_path    TEXT NOT NULL DEFAULT ''            (worker-supplied hint;
--                                                         NOT authoritative)
--   declared_size    INTEGER NOT NULL DEFAULT 0          (worker-supplied hint;
--                                                         the artifact upload
--                                                         pipeline's
--                                                         FinalizeVerified
--                                                         recomputes both)
--   declared_sha256  TEXT NOT NULL DEFAULT ''            (worker-supplied hint;
--                                                         recomputed on upload)
--   metadata_json    TEXT NOT NULL DEFAULT '{}'          (worker-supplied Struct)
--   registered_at    TEXT NOT NULL DEFAULT (RFC3339 STRFTIME('now'))
--                                                         (RFC3339 — REQUIRED
--                                                         by CI guard (e))
--
-- Constraints:
--   UNIQUE (task_id, artifact_id)        — idempotent ingest replays skip
--                                           silently via ON CONFLICT.
--   FOREIGN KEY (task_id) REFERENCES tasks (task_id) ON DELETE CASCADE
--                                         — closes the leak surface where
--                                           an orphan output_artifacts row
--                                           could outlive its task.
--
-- Indices:
--   idx_task_output_artifacts_task        — primary lookup by task_id
--                                            (used by ingest + finalization).
--   idx_task_output_artifacts_artifact    — reverse lookup by artifact_id
--                                            (used by Artifact-blob cross-ref).
--   idx_task_output_artifacts_attempt     — forensic attempts-for-task scans
--                                            (Stage 2: finalization will
--                                            join artifact↔attempt here).
--
-- Lifecycle invariants (audit §P1.4):
--   - Inserted by `internal/ingest.TaskReportIngestionService.IngestTaskResult`
--     as part of step (3) of the audit-mandated sequence, AFTER
--     `TransitionTaskToTerminalAtomic` has already closed the Task + Attempt
--     in step (2). The insert is a separate tx from the close (idempotency
--     via the UNIQUE(task_id, artifact_id) index — replays of the same
--     TaskResult raise ON CONFLICT and are surfaced as no-ops via
--     taskoutput_artifacts.ErrAlreadyRegistered).
--   - Joined to task_attempts via (task_id, attempt_id) for forensic
--     queries. NOT joined to artifacts (id) directly because the artifact
--     row is created later by the artifact upload pipeline; task_output_artifacts
--     is the worker's PROMISE, artifacts is the master-side VERIFIED record.
--
-- RFC3339 timestamp policy:
--   The `registered_at` column DEFAULT uses
--   STRFTIME('%Y-%m-%dT%H:%M:%SZ', 'now', 'utc') explicitly. Pre-051
--   legacy migrations (013_delivery_targets.sql, 014_orchestrator_outbox.sql)
--   used DATETIME('now') which is timezone-naive and broke SQLite dump
--   compatibility with the worker agent's RFC3339 parser. CI guard (e)
--   (scripts/ci/check-task-runtime-invariants.sh guard_e_task_migration_strftime)
--   FORBIDS new migrations from introducing DATETIME('now') / CURRENT_TIMESTAMP
--   here — STRFTIME MUST be used.
--
-- Down-migration: not provided. Drop the table and accept historical
-- registrations as orphan metadata (recoverable from outbox).

CREATE TABLE IF NOT EXISTS task_output_artifacts (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id          TEXT NOT NULL,
    attempt_id       TEXT NOT NULL DEFAULT '',
    artifact_id      TEXT NOT NULL,
    artifact_type    TEXT NOT NULL DEFAULT '',
    declared_path    TEXT NOT NULL DEFAULT '',
    declared_size    INTEGER NOT NULL DEFAULT 0,
    declared_sha256  TEXT NOT NULL DEFAULT '',
    metadata_json    TEXT NOT NULL DEFAULT '{}',
    registered_at    TEXT NOT NULL DEFAULT (STRFTIME('%Y-%m-%dT%H:%M:%SZ', 'now', 'utc')),
    FOREIGN KEY (task_id) REFERENCES tasks (task_id) ON DELETE CASCADE,
    UNIQUE (task_id, artifact_id)
);

CREATE INDEX IF NOT EXISTS idx_task_output_artifacts_task
    ON task_output_artifacts(task_id);

CREATE INDEX IF NOT EXISTS idx_task_output_artifacts_artifact
    ON task_output_artifacts(artifact_id);

CREATE INDEX IF NOT EXISTS idx_task_output_artifacts_attempt
    ON task_output_artifacts(attempt_id);
