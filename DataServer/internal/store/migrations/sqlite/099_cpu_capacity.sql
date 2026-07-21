-- 099: Per-attempt CPU capacity telemetry.
--
-- The worker now reports the host CPU environment for each attempt so the
-- master can compute accurate oversubscription ratios and stop using
-- active_workers_at_start as a proxy for logical CPU count.

ALTER TABLE task_attempt_metrics
ADD COLUMN logical_cpu_count INTEGER NOT NULL DEFAULT 0;

ALTER TABLE task_attempt_metrics
ADD COLUMN cpu_quota REAL NOT NULL DEFAULT 0;

ALTER TABLE task_attempt_metrics
ADD COLUMN effective_cpu_count INTEGER NOT NULL DEFAULT 0;
