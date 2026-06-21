-- Migration 001: core (Postgres dialect)
--
-- Cross-cutting tables that aren't owned by any single domain. Today
-- this is just velox_meta, a KV-table for operator-set knobs (default
-- timezone, default sqlite→pg migration flag, etc.). Future
-- migrations in this slot add app-wide observability or feature flags.
--
-- Mirrors no specific SQLite migration; the SQLite cumulative path
-- spread equivalent knobs across queues/jobs/workflows tables. Postgres
-- gets a clean home for them.

CREATE TABLE IF NOT EXISTS velox_meta (
    key          TEXT PRIMARY KEY,
    value        TEXT NOT NULL,
    updated_at   TEXT NOT NULL
);
