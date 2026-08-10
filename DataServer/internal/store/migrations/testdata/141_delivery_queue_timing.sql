-- Test fixture mirror of sqlite/141_delivery_queue_timing.sql.
CREATE TABLE IF NOT EXISTS job_deliveries (
    delivery_id TEXT PRIMARY KEY,
    artifact_id TEXT NOT NULL DEFAULT '',
    destination_id TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'PENDING',
    idempotency_key TEXT,
    created_at TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL DEFAULT '',
    next_attempt_at TEXT
);

ALTER TABLE job_deliveries ADD COLUMN queued_at TEXT;

UPDATE job_deliveries
SET queued_at = COALESCE(next_attempt_at, created_at)
WHERE queued_at IS NULL;
