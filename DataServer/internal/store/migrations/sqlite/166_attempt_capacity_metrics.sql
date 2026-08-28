-- 166_attempt_capacity_metrics.sql
-- Preserve worker-reported per-job capacity facts at terminal ingest.

ALTER TABLE task_attempt_metrics ADD COLUMN job_publish_bytes INTEGER NOT NULL DEFAULT 0;
ALTER TABLE task_attempt_metrics ADD COLUMN job_page_faults INTEGER NOT NULL DEFAULT 0;
ALTER TABLE task_attempt_metrics ADD COLUMN job_scratch_peak_bytes INTEGER NOT NULL DEFAULT 0;
