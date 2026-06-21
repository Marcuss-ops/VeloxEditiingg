-- Migration 007: deliveries (Postgres dialect)
--
-- Delivery target registry + attempt-level lease/attempt bookkeeping.
-- Mirrors SQLite migrations 013_delivery_targets.sql,
-- 022_split_deliveries.sql, 031_delivery_leases.sql,
-- 032_delivery_attempts_nullable_target.sql, and
-- 037_job_delivery_plans.sql (consolidated here).
--
-- Cross-domain FKs (to jobs, artifacts) intentionally omitted so each
-- file ships cleanly. A late migration adds the FKs.

CREATE TABLE IF NOT EXISTS delivery_targets (
    target_id           TEXT PRIMARY KEY,
    name                TEXT NOT NULL,
    kind                TEXT NOT NULL,
    config_json         TEXT NOT NULL DEFAULT '{}',
    enabled             INTEGER NOT NULL DEFAULT 1,
    created_at          TEXT NOT NULL,
    updated_at          TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS delivery_leases (
    lease_id            TEXT PRIMARY KEY,
    target_id           TEXT NOT NULL,
    job_id              TEXT NOT NULL,
    worker_id           TEXT NOT NULL,
    locked_until        TEXT NOT NULL,
    status              TEXT NOT NULL DEFAULT 'ACTIVE',
    created_at          TEXT NOT NULL,
    updated_at          TEXT NOT NULL,
    UNIQUE(target_id, job_id)
);

CREATE INDEX IF NOT EXISTS idx_pg_delivery_leases_locked_until ON delivery_leases(locked_until)
    WHERE status = 'ACTIVE';
CREATE INDEX IF NOT EXISTS idx_pg_delivery_leases_worker ON delivery_leases(worker_id);

CREATE TABLE IF NOT EXISTS delivery_attempts (
    attempt_id          TEXT PRIMARY KEY,
    job_id              TEXT NOT NULL,
    target_id           TEXT,
    lease_id            TEXT,
    started_at          TEXT NOT NULL,
    completed_at        TEXT,
    status              TEXT NOT NULL DEFAULT 'PENDING',
    error               TEXT,
    response_json       TEXT
);

CREATE INDEX IF NOT EXISTS idx_pg_delivery_attempts_job ON delivery_attempts(job_id);
CREATE INDEX IF NOT EXISTS idx_pg_delivery_attempts_target ON delivery_attempts(target_id);
