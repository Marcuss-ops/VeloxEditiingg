-- Migration 003: workers (Postgres dialect)
--
-- Worker registry + heartbeat state. Mirrors SQLite migrations
-- 001_initial.sql (workers base), 002_legacy_imports.sql (display_name
-- + ip_address + first_seen), and 020_worker_control_plane.sql
-- (code_version / bundle_version / bundle_hash / capabilities).
--
-- No FK to jobs(assigned_to) — Postgres-native cross-domain FKs land in
-- a later migration once both sides ship.

CREATE TABLE IF NOT EXISTS workers (
    worker_id            TEXT PRIMARY KEY,
    worker_name          TEXT,                 -- human-readable fleet label
    display_name         TEXT,
    ip_address           TEXT,
    first_seen           TEXT,
    last_heartbeat       TEXT,
    last_job             TEXT,
    current_job          TEXT,
    code_version         TEXT,
    bundle_version       TEXT,
    bundle_hash          TEXT,
    protocol_version     TEXT,
    engine_version       TEXT,
    capabilities         TEXT,                 -- JSON
    approved             INTEGER NOT NULL DEFAULT 0,
    enabled              INTEGER NOT NULL DEFAULT 1,
    is_active            INTEGER NOT NULL DEFAULT 0,
    registered_at        TEXT NOT NULL,
    updated_at           TEXT NOT NULL,
    last_revision        BIGINT NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_pg_workers_active ON workers(is_active) WHERE is_active = 1;
CREATE INDEX IF NOT EXISTS idx_pg_workers_last_heartbeat ON workers(last_heartbeat);
