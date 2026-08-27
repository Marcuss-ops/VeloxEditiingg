-- 160_per_job_resource_attribution.sql
--
-- Per-job resource attribution columns for the capacity scorecard.
-- These attributes resource consumption to the individual job, enabling
-- the master to answer: "How much does one job cost on this worker?"

ALTER TABLE task_attempt_metrics
ADD COLUMN job_peak_rss_delta_bytes INTEGER NOT NULL DEFAULT 0;

ALTER TABLE task_attempt_metrics
ADD COLUMN job_cpu_core_seconds REAL NOT NULL DEFAULT 0.0;

ALTER TABLE task_attempt_metrics
ADD COLUMN job_asset_cache_bytes_used INTEGER NOT NULL DEFAULT 0;

ALTER TABLE task_attempt_metrics
ADD COLUMN job_prefetch_bytes INTEGER NOT NULL DEFAULT 0;

ALTER TABLE task_attempt_metrics
ADD COLUMN job_upload_buffer_peak_bytes INTEGER NOT NULL DEFAULT 0;

ALTER TABLE task_attempt_metrics
ADD COLUMN job_render_wall_ms INTEGER NOT NULL DEFAULT 0;

ALTER TABLE task_attempt_metrics
ADD COLUMN job_asset_wall_ms INTEGER NOT NULL DEFAULT 0;

ALTER TABLE task_attempt_metrics
ADD COLUMN job_publish_wall_ms INTEGER NOT NULL DEFAULT 0;
