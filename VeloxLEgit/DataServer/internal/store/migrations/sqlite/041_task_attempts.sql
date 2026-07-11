-- 041_task_attempts.sql
--
-- Stores per-execution attempt records for tasks.
-- Uniqueness: (task_id, attempt_number) is unique.
-- At most one active (non-terminal) attempt per task at any time.

CREATE TABLE IF NOT EXISTS task_attempts (
    id              TEXT PRIMARY KEY,
    task_id         TEXT NOT NULL,
    attempt_number  INTEGER NOT NULL DEFAULT 0,
    worker_id       TEXT NOT NULL DEFAULT '',
    lease_id        TEXT NOT NULL DEFAULT '',
    status          TEXT NOT NULL DEFAULT 'PENDING',
    started_at      TEXT,
    completed_at    TEXT,
    error_code      TEXT NOT NULL DEFAULT '',
    error_message   TEXT NOT NULL DEFAULT '',
    report_version  INTEGER NOT NULL DEFAULT 0,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_task_attempts_task_number
    ON task_attempts(task_id, attempt_number);
CREATE INDEX IF NOT EXISTS idx_task_attempts_task_id ON task_attempts(task_id);
CREATE INDEX IF NOT EXISTS idx_task_attempts_worker_id ON task_attempts(worker_id);
CREATE INDEX IF NOT EXISTS idx_task_attempts_status ON task_attempts(status);
