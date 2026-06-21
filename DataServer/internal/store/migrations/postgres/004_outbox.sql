-- Migration 004: outbox (Postgres dialect)
--
-- Transactional outbox: producers INSERT into outbox_events inside
-- the same DB transaction as their state-change writes so dispatchers
-- can reliably forward events later. Mirrors SQLite migrations
-- 014_orchestrator_outbox.sql (initial shape) + 026_outbox_v2.sql
-- (event_id is string PK, attempt_count + locked_* added).
--
-- No FK to jobs.aggregate_id — outbox covers many aggregate types, and
-- reserving FKs to every one would entangle later migrations.

CREATE TABLE IF NOT EXISTS outbox_events (
    event_id         TEXT PRIMARY KEY,
    aggregate_type   TEXT NOT NULL,
    aggregate_id     TEXT NOT NULL,
    event_type       TEXT NOT NULL,
    payload_json     TEXT NOT NULL DEFAULT '{}',
    status           TEXT NOT NULL DEFAULT 'PENDING',
    available_at     TEXT NOT NULL,
    created_at       TEXT NOT NULL,
    processed_at     TEXT,
    locked_by        TEXT,
    locked_until     TEXT,
    attempt_count    INTEGER NOT NULL DEFAULT 0,
    last_error       TEXT
);

-- Claim scan path: dispatcher finds ready events with `status='PENDING'
-- AND available_at <= now() ORDER BY created_at`. Keeping the index
-- narrow avoids bloating with terminal-state rows.
CREATE INDEX IF NOT EXISTS idx_pg_outbox_pending ON outbox_events(available_at)
    WHERE status = 'PENDING';
-- Recovery scan path: PROCESSING rows whose locked_until has passed
-- need to be available for re-claim.
CREATE INDEX IF NOT EXISTS idx_pg_outbox_locked_until ON outbox_events(locked_until)
    WHERE status = 'PROCESSING';
-- Aggregate lookup for dispatcher that filters by aggregate_type (rare
-- but used by replay tooling).
CREATE INDEX IF NOT EXISTS idx_pg_outbox_aggregate ON outbox_events(aggregate_type, aggregate_id);
