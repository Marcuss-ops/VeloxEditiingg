-- 031_delivery_leases.sql
-- Adds durable lease + retry columns to job_deliveries.
--
-- Status set changes from:
--   PENDING → CLAIMED → SUCCEEDED | FAILED
-- to:
--   PENDING → RUNNING → SUCCEEDED
--                ↓
--           RETRY_WAIT → PENDING (retry)
--                ↓
--           FAILED
--                ↓
--           BLOCKED_AUTH
--                ↓
--           CANCELLED
--
-- CLAIMED is eliminated; RUNNING replaces it.

-- ── New columns ──────────────────────────────────────────────────────────────

ALTER TABLE job_deliveries ADD COLUMN locked_by TEXT;
ALTER TABLE job_deliveries ADD COLUMN lease_id TEXT;
ALTER TABLE job_deliveries ADD COLUMN lease_expires_at TEXT;
ALTER TABLE job_deliveries ADD COLUMN next_attempt_at TEXT;
ALTER TABLE job_deliveries ADD COLUMN attempt_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE job_deliveries ADD COLUMN max_attempts INTEGER NOT NULL DEFAULT 5;
ALTER TABLE job_deliveries ADD COLUMN last_error_code TEXT;
ALTER TABLE job_deliveries ADD COLUMN last_error_message TEXT;
ALTER TABLE job_deliveries ADD COLUMN completed_at TEXT;

-- ── Indexes ──────────────────────────────────────────────────────────────────

-- Composite index for the claim query: find PENDING/RETRY_WAIT rows that are
-- claimable, plus expired RUNNING leases for reclaim.
CREATE INDEX IF NOT EXISTS idx_job_deliveries_claimable
ON job_deliveries(
    status,
    next_attempt_at,
    lease_expires_at,
    created_at
);

-- Uniqueness guard: one delivery per (artifact, destination) pair.
CREATE UNIQUE INDEX IF NOT EXISTS idx_job_delivery_artifact_destination
ON job_deliveries(artifact_id, destination_id);

-- ── Backfill existing CLAIMED rows to RUNNING ────────────────────────────────

-- Any row still in CLAIMED status from the old runner gets moved to RUNNING
-- so the new claim query doesn't skip them.
UPDATE job_deliveries SET status = 'RUNNING' WHERE status = 'CLAIMED';
