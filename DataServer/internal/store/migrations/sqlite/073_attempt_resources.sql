-- 073_attempt_resources.sql
--
-- Step 10 / Scorecard v2: per-attempt resource snapshot columns on
-- task_attempt_metrics. Captures the peak resource consumption
-- observed during the attempt so operators can correlate slowdowns
-- with resource pressure without querying the worker heartbeat
-- time-series.
--
-- Columns:
--   cpu_percent_peak    — peak CPU utilization % during the attempt
--   rss_peak_bytes      — peak resident set size (memory) in bytes
--   disk_read_bytes     — total bytes read from disk during attempt
--   disk_write_bytes    — total bytes written to disk during attempt
--   network_rx_bytes    — total bytes received over network
--   network_tx_bytes    — total bytes transmitted over network
--   iowait_ms           — total CPU iowait time in milliseconds
--   open_fds_peak       — peak open file descriptor count
--
-- All DEFAULT 0 so older workers that don't emit these fields
-- continue to function without a code change.

ALTER TABLE task_attempt_metrics
    ADD COLUMN cpu_percent_peak    REAL    NOT NULL DEFAULT 0.0;
ALTER TABLE task_attempt_metrics
    ADD COLUMN rss_peak_bytes      INTEGER NOT NULL DEFAULT 0;
ALTER TABLE task_attempt_metrics
    ADD COLUMN disk_read_bytes     INTEGER NOT NULL DEFAULT 0;
ALTER TABLE task_attempt_metrics
    ADD COLUMN disk_write_bytes    INTEGER NOT NULL DEFAULT 0;
ALTER TABLE task_attempt_metrics
    ADD COLUMN network_rx_bytes    INTEGER NOT NULL DEFAULT 0;
ALTER TABLE task_attempt_metrics
    ADD COLUMN network_tx_bytes    INTEGER NOT NULL DEFAULT 0;
ALTER TABLE task_attempt_metrics
    ADD COLUMN iowait_ms           INTEGER NOT NULL DEFAULT 0;
ALTER TABLE task_attempt_metrics
    ADD COLUMN open_fds_peak       INTEGER NOT NULL DEFAULT 0;
