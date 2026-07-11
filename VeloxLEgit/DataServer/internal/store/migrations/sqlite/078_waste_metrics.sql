-- 078_waste_metrics.sql
--
-- Step 17 / Scorecard v2: waste and cost metrics on task_attempt_metrics.
-- Captures retry/waste data so operators can compute the cost of
-- failed attempts and attribute wasted compute to root causes.
--
-- Columns:
--   retry_count          — how many times this task was retried (0 = first attempt)
--   wasted_cpu_ms        — CPU time consumed by failed attempts for this task
--   wasted_download_bytes — bytes downloaded for failed/downloaded-then-retried attempts
--   wasted_cost_estimate — estimated EUR cost of wasted resources (cpu + storage + network)
--
-- All DEFAULT 0 so older workers that don't emit these fields
-- continue to function without a code change.

ALTER TABLE task_attempt_metrics
    ADD COLUMN retry_count            INTEGER NOT NULL DEFAULT 0;
ALTER TABLE task_attempt_metrics
    ADD COLUMN wasted_cpu_ms          INTEGER NOT NULL DEFAULT 0;
ALTER TABLE task_attempt_metrics
    ADD COLUMN wasted_download_bytes  INTEGER NOT NULL DEFAULT 0;
ALTER TABLE task_attempt_metrics
    ADD COLUMN wasted_cost_estimate   REAL    NOT NULL DEFAULT 0.0;
