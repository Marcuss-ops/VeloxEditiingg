-- Migration 120 — outbox claim fencing.
--
-- Every Claim stamps a fresh token. Terminal and lease-extension writes
-- must include that token so a stale handler cannot mutate a row after
-- another dispatcher has reclaimed it.

ALTER TABLE outbox_events
    ADD COLUMN fence_token TEXT NOT NULL DEFAULT '';

CREATE INDEX IF NOT EXISTS idx_outbox_fence_token
    ON outbox_events(event_id, fence_token)
    WHERE status = 'PROCESSING';
