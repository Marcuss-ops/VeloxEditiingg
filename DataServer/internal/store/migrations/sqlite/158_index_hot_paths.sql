-- 158_index_hot_paths.sql
--
-- Hot-path indexes for delivery reconciliation and job listing.
-- Scans in reconciliation_delivery_pending.go and
-- reconciliation_awaiting_artifact.go do ORDER BY COALESCE(updated_at,
-- created_at); a composite on (status, updated_at) and (status,
-- created_at) lets the planner use an index for both the filter and the
-- sort. IF NOT EXISTS keeps the migration idempotent on downgrade/replay.

CREATE INDEX IF NOT EXISTS idx_jobs_status_updated_at
    ON jobs(status, updated_at);
CREATE INDEX IF NOT EXISTS idx_jobs_status_created_at
    ON jobs(status, created_at);
CREATE INDEX IF NOT EXISTS idx_job_deliveries_status_updated_at
    ON job_deliveries(status, updated_at);
CREATE INDEX IF NOT EXISTS idx_job_deliveries_status_next_attempt
    ON job_deliveries(status, next_attempt_at);
