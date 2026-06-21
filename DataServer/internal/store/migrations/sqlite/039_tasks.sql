-- 039_tasks.sql
--
-- Creates the canonical tasks table for distributed rendering.
-- Each render Job owns exactly one Task.

CREATE TABLE IF NOT EXISTS tasks (
    task_id        TEXT PRIMARY KEY,
    job_id         TEXT NOT NULL,
    project_id     TEXT NOT NULL DEFAULT '',
    render_plan_id TEXT NOT NULL DEFAULT '',
    executor_id    TEXT NOT NULL DEFAULT '',
    executor_version INTEGER NOT NULL DEFAULT 0,
    status         TEXT NOT NULL DEFAULT 'PENDING',
    priority       INTEGER NOT NULL DEFAULT 0,
    revision       INTEGER NOT NULL DEFAULT 0,
    attempt_count  INTEGER NOT NULL DEFAULT 0,
    worker_id      TEXT NOT NULL DEFAULT '',
    lease_id       TEXT NOT NULL DEFAULT '',
    ready_at       TEXT,
    started_at     TEXT,
    completed_at   TEXT,
    created_at     TEXT NOT NULL,
    updated_at     TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_tasks_job_id ON tasks(job_id);
CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
CREATE INDEX IF NOT EXISTS idx_tasks_executor_id ON tasks(executor_id);
CREATE INDEX IF NOT EXISTS idx_tasks_worker_id ON tasks(worker_id);
CREATE INDEX IF NOT EXISTS idx_tasks_lease_id ON tasks(lease_id);
CREATE INDEX IF NOT EXISTS idx_tasks_ready_at ON tasks(ready_at);
CREATE UNIQUE INDEX IF NOT EXISTS idx_tasks_job_id_unique ON tasks(job_id);
