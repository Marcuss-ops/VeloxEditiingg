-- Migration 026 — Generic transactional outbox (PR 8).
--
-- Replaces `orchestrator_outbox` (migration 014) with a generic outbox table
-- whose event_type is NOT constrained to a closed enum: new handler types
-- register at runtime via the outbox.Registry, no SQL change needed.
--
-- State machine (application-enforced; no SQL CHECK):
--   PENDING     — newly written, not yet claimed
--   PROCESSING  — claimed by a dispatcher (with locked_by + locked_until)
--   PROCESSED   — handler completed successfully
--   FAILED      — handler exhausted retries
--
-- Atomic claim is performed by `Claim` (see internal/outbox/store.go):
--   UPDATE outbox_events
--   SET status='PROCESSING', locked_by=?, locked_until=?, attempt_count=attempt_count+1
--   WHERE event_id IN (SELECT event_id FROM outbox_events
--                      WHERE status IN ('PENDING','PROCESSING')
--                        AND available_at <= ?
--                        AND (status='PENDING' OR locked_until < ?)
--                      ORDER BY created_at LIMIT ?)

CREATE TABLE IF NOT EXISTS outbox_events (
    event_id        TEXT PRIMARY KEY,
    aggregate_type  TEXT NOT NULL,
    aggregate_id    TEXT NOT NULL,
    event_type      TEXT NOT NULL,
    payload_json    TEXT NOT NULL DEFAULT '{}',

    status          TEXT NOT NULL DEFAULT 'PENDING',
    available_at    TEXT NOT NULL,

    attempt_count   INTEGER NOT NULL DEFAULT 0,
    locked_by       TEXT,
    locked_until    TEXT,

    processed_at    TEXT,
    last_error      TEXT,
    created_at      TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_outbox_ready
    ON outbox_events(status, available_at, created_at)
    WHERE status IN ('PENDING', 'PROCESSING');

CREATE INDEX IF NOT EXISTS idx_outbox_locked
    ON outbox_events(locked_until)
    WHERE status = 'PROCESSING';

CREATE INDEX IF NOT EXISTS idx_outbox_aggregate
    ON outbox_events(aggregate_type, aggregate_id);
