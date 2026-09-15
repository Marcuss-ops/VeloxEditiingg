-- 174_attempt_concurrency_metrics.sql
-- Persist the worker-wide concurrency observed when an attempt starts.

ALTER TABLE task_attempt_metrics
ADD COLUMN jobs_concurrent_at_start INTEGER NOT NULL DEFAULT 0;
