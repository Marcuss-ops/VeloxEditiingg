-- migrations/sqlite/061_attempt_commits.sql
--
-- Artifact Commit Protocol (Phase 1.1) — durable tracker for the
-- attempt-commit lifecycle. One row per Attempt that produces
-- required outputs. The master creates the row on DECLARED when the
-- worker first sends TaskOutputDeclared and gates every terminal
-- state change (tasks/tasks_attempts/jobs ⇒ SUCCEEDED) on this row
-- reaching COMMITTED.
--
-- Companion tables: tasks, task_attempts, task_output_declarations
-- (migration 062). The tuple (task_id, attempt_id) is UNIQUE — at
-- most one AttemptCommit per Attempt.
--
-- The fence tuple (task_id, attempt_id, worker_id, lease_id,
-- task_revision) is enforced by callers (see the completion package,
-- Phase 2); this DDL only locks the row identity. commit_token_hash
-- stores the SHA256 hex of the opaque commit_token the master hands
-- to the worker in ArtifactUploadPlan. The token itself is opaquely
-- transmitted over the wire and never persisted on the master
-- beyond the moment it is first returned.

CREATE TABLE IF NOT EXISTS attempt_commits (
    commit_id TEXT PRIMARY KEY,

    task_id TEXT NOT NULL,
    attempt_id TEXT NOT NULL,
    job_id TEXT NOT NULL,
    worker_id TEXT NOT NULL,
    lease_id TEXT NOT NULL,
    task_revision INTEGER NOT NULL,

    status TEXT NOT NULL,
    required_output_count INTEGER NOT NULL,
    ready_output_count INTEGER NOT NULL DEFAULT 0,

    commit_token_hash TEXT NOT NULL,
    commit_deadline_at TEXT NOT NULL,
    last_progress_at TEXT NOT NULL,

    committed_at TEXT,
    rejected_code TEXT,
    rejected_message TEXT,

    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,

    UNIQUE(task_id, attempt_id)
);

CREATE INDEX IF NOT EXISTS idx_attempt_commits_status
    ON attempt_commits(status);

CREATE INDEX IF NOT EXISTS idx_attempt_commits_deadline
    ON attempt_commits(commit_deadline_at);
