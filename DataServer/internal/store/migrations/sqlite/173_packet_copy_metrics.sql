-- 173_packet_copy_metrics.sql
-- Persist the packet-copy breakdown received/derived during TaskResult ingest.
-- These columns are appended because applied SQLite migrations are immutable.

ALTER TABLE task_attempt_metrics ADD COLUMN segments_total INTEGER NOT NULL DEFAULT 0;
ALTER TABLE task_attempt_metrics ADD COLUMN segments_packet_copy INTEGER NOT NULL DEFAULT 0;
ALTER TABLE task_attempt_metrics ADD COLUMN segments_reencoded INTEGER NOT NULL DEFAULT 0;
ALTER TABLE task_attempt_metrics ADD COLUMN packet_copy_ratio REAL NOT NULL DEFAULT 0.0;
ALTER TABLE task_attempt_metrics ADD COLUMN packet_copy_bytes INTEGER NOT NULL DEFAULT 0;
ALTER TABLE task_attempt_metrics ADD COLUMN reencoded_bytes INTEGER NOT NULL DEFAULT 0;
