-- 074_queue_metrics.sql
--
-- Step 11 / Scorecard v2: queue and wait-time metrics on
-- task_attempt_metrics. Captures the scheduling latency so
-- operators can distinguish render-time from queue-time.
--
-- Columns:
--   queue_ms                 — time spent in READY queue before claim
--   lease_wait_ms            — time between claim and worker acceptance
--   time_to_first_worker_ms  — end-to-end scheduling latency
--   pending_tasks_at_start   — queue depth when this task was claimed
--   active_workers_at_start  — active workers when this task was claimed
--
-- All DEFAULT 0 so older workers that don't emit these fields
-- continue to function without a code change.

ALTER TABLE task_attempt_metrics
    ADD COLUMN queue_ms                  INTEGER NOT NULL DEFAULT 0;
ALTER TABLE task_attempt_metrics
    ADD COLUMN lease_wait_ms             INTEGER NOT NULL DEFAULT 0;
ALTER TABLE task_attempt_metrics
    ADD COLUMN time_to_first_worker_ms   INTEGER NOT NULL DEFAULT 0;
ALTER TABLE task_attempt_metrics
    ADD COLUMN pending_tasks_at_start    INTEGER NOT NULL DEFAULT 0;
ALTER TABLE task_attempt_metrics
    ADD COLUMN active_workers_at_start   INTEGER NOT NULL DEFAULT 0;
