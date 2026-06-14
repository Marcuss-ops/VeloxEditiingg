-- Migration 007: Queue persistence - orchestrator and DLQ
--
-- Makes SQLite the source of truth for multi-step jobs and dead letter queue.
-- JSON files become legacy backups only.

-- ============================================================
-- orchestrator_jobs: multi-step job state
-- ============================================================
CREATE TABLE IF NOT EXISTS orchestrator_jobs (
    job_id        TEXT PRIMARY KEY,
    status        TEXT NOT NULL DEFAULT 'PENDING',
    total_steps   INTEGER NOT NULL DEFAULT 0,
    current_step  INTEGER NOT NULL DEFAULT 0,
    pipeline_type TEXT NOT NULL DEFAULT '',
    started_at    TEXT,
    completed_at  TEXT,
    created_at    TEXT NOT NULL,
    updated_at    TEXT NOT NULL,
    raw_json      TEXT NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_orch_jobs_status ON orchestrator_jobs(status);
CREATE INDEX IF NOT EXISTS idx_orch_jobs_updated ON orchestrator_jobs(updated_at);

-- ============================================================
-- dlq_jobs: dead letter queue
-- ============================================================
CREATE TABLE IF NOT EXISTS dlq_jobs (
    job_id        TEXT PRIMARY KEY,
    dead_at       TEXT NOT NULL,
    dead_reason   TEXT NOT NULL DEFAULT '',
    fail_reason   TEXT NOT NULL DEFAULT '',
    fail_count    INTEGER NOT NULL DEFAULT 0,
    replayable    INTEGER NOT NULL DEFAULT 0,
    created_at    TEXT NOT NULL,
    raw_json      TEXT NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_dlq_jobs_dead_at ON dlq_jobs(dead_at);
CREATE INDEX IF NOT EXISTS idx_dlq_jobs_replayable ON dlq_jobs(replayable);

-- ============================================================
-- job_events: event log in SQLite
-- ============================================================
CREATE TABLE IF NOT EXISTS job_events (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    job_id    TEXT NOT NULL,
    event     TEXT NOT NULL DEFAULT '',
    raw_json  TEXT NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_job_events_job_id ON job_events(job_id);
CREATE INDEX IF NOT EXISTS idx_job_events_timestamp ON job_events(timestamp);
