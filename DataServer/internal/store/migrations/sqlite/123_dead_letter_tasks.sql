CREATE TABLE IF NOT EXISTS dead_letter_tasks (
    id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    last_attempt_id TEXT NOT NULL UNIQUE,
    failure_class TEXT NOT NULL,
    error_code TEXT NOT NULL DEFAULT '',
    retryable INTEGER NOT NULL DEFAULT 0,
    payload_snapshot_json TEXT NOT NULL DEFAULT '{}',
    first_failed_at TEXT NOT NULL,
    last_failed_at TEXT NOT NULL,
    replay_count INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'OPEN'
        CHECK (status IN ('OPEN','REPLAY_PENDING','RESOLVED','CANCELLED'))
);
CREATE INDEX IF NOT EXISTS idx_dead_letter_tasks_status_time
    ON dead_letter_tasks(status, last_failed_at);
CREATE INDEX IF NOT EXISTS idx_dead_letter_tasks_task
    ON dead_letter_tasks(task_id);
