-- Migration 120 — outbox claim fencing test fixture.
ALTER TABLE outbox_events
    ADD COLUMN fence_token TEXT NOT NULL DEFAULT '';

CREATE INDEX IF NOT EXISTS idx_outbox_fence_token
    ON outbox_events(event_id, fence_token)
    WHERE status = 'PROCESSING';
