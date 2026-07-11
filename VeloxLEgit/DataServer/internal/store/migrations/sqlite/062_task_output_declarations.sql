-- migrations/sqlite/062_task_output_declarations.sql
--
-- Artifact Commit Protocol (Phase 1.2) — worker declarations.
--
-- Each row is a worker-supplied manifest entry: opaque
-- worker_spool_key, expected size, expected sha256, mime. The master
-- promotes the declaration to a corresponding artifacts row at
-- RECEIVE time only after the master-side SHA256 recompute matches
-- the worker's expected_sha256.
--
-- Coexistence with task_output_artifacts:
-- Phase 1 keeps task_output_artifacts in the schema unchanged; Phase
-- 2 will demote task_output_artifacts to a non-authoritative mirror
-- once the master stops ingesting TaskResult ⇒ artifact rows. Until
-- then the two tables coexist. task_output_declarations IS the
-- declared-tracker-of-record from this point forward; it feeds every
-- upload plan, identity check and commit-gate.
--
-- Companion table: attempt_commits (migration 061). UNIQUE on
-- (task_id, attempt_id, output_kind, logical_name) ensures a single
-- Attempt cannot declare two artifacts with the same identity.

CREATE TABLE IF NOT EXISTS task_output_declarations (
    declaration_id TEXT PRIMARY KEY,

    commit_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    attempt_id TEXT NOT NULL,

    output_kind TEXT NOT NULL,
    logical_name TEXT NOT NULL,
    mime_type TEXT NOT NULL,

    expected_size_bytes INTEGER NOT NULL,
    expected_sha256 TEXT NOT NULL,

    worker_spool_key TEXT,
    status TEXT NOT NULL,

    upload_id TEXT,
    artifact_id TEXT,

    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,

    UNIQUE(task_id, attempt_id, output_kind, logical_name)
);

CREATE INDEX IF NOT EXISTS idx_task_output_declarations_commit
    ON task_output_declarations(commit_id);
