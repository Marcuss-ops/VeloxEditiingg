-- Migration 002: jobs (Postgres dialect)
--
-- Mirrors the cumulative shape of the SQLite `jobs` table after
-- migrations 001_initial + 015_revision + 017_normalization +
-- 023_columns minus the columns DROPPED in
-- 033_drop_legacy_job_columns / 038_drop_jobs_raw_json / 048_drop_jobs_runtime_columns.
-- Result matches store_jobs_query.go's `jobProjectionColumns` projection.
--
-- PR #9: assigned_to, claimed_by, lease_id, lease_expiry, retry_count dropped.
-- Runtime state (worker assignment, lease identity, attempt count) lives on
-- tasks + job_attempts now.
--
-- Cross-domain FK strategy: deliberately omitted. jobs is referenced
-- by artifacts.job_id, job_attempts.job_id, job_events.job_id, and
-- outbox_events.aggregate_id — none of those tables ship in this
-- file. A follow-up migration will ADD CONSTRAINT … FOREIGN KEY back
-- once both sides are PG-resident.
--
-- Index plan mirrors the SQLite indices that the runner installed the
-- same columns for, plus a partial index for the ClaimNext scan path
-- so `WHERE status = 'PENDING' ORDER BY updated_at` stays cheap as
-- the terminal-state backlog grows.

CREATE TABLE IF NOT EXISTS jobs (
    job_id                       TEXT PRIMARY KEY,
    status                       TEXT,
    video_name                   TEXT,
    project_id                   TEXT,
    created_at                   TEXT NOT NULL,
    updated_at                   TEXT NOT NULL,
    started_at                   TEXT,
    completed_at                 TEXT,
    assigned_at                  TEXT,
    worker_name                  TEXT,
    claimed_at                   TEXT,
    attempt                      INTEGER NOT NULL DEFAULT 0,
    max_retries                  INTEGER NOT NULL DEFAULT 3,
    revision                     INTEGER NOT NULL DEFAULT 0,
    last_error                   TEXT,
    last_error_at                TEXT,
    error_message                TEXT,
    failed_at                    TEXT,
    failed_by                    TEXT,
    processing_at                TEXT,
    last_upload_attempt_at       TEXT,
    last_drive_upload_result     TEXT,
    remote_status                TEXT,
    job_fingerprint              TEXT,
    submitted_via                TEXT,
    last_activity                TEXT,
    run_id                       TEXT,
    job_run_id                   TEXT,
    logs_updated_at              TEXT,
    slot_data                    TEXT,
    request_json                 TEXT NOT NULL DEFAULT '',
    result_json                  TEXT NOT NULL DEFAULT '',
    last_upload_result           TEXT,
    migrated_at                  TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_pg_jobs_status ON jobs(status);
CREATE INDEX IF NOT EXISTS idx_pg_jobs_updated ON jobs(updated_at);
CREATE INDEX IF NOT EXISTS idx_pg_jobs_last_error ON jobs(last_error);
CREATE INDEX IF NOT EXISTS idx_pg_jobs_completed_at ON jobs(completed_at);
-- Partial index for ClaimNext: keeps `WHERE UPPER(status)='PENDING'
-- ORDER BY updated_at` cheap on large backlogs.
CREATE INDEX IF NOT EXISTS idx_pg_jobs_pending_first ON jobs(updated_at)
    WHERE UPPER(COALESCE(status, '')) = 'PENDING';
