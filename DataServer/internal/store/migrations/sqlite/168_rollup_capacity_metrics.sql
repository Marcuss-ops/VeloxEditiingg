-- 168_rollup_capacity_metrics.sql
-- Extend worker_resource_rollups with the capacity columns added to
-- worker_resource_samples in migration 165, so hourly rollups are
-- lossless for capacity certification and historical analysis.

-- Aggregation rationale:
--   MAX  — peaks (scratch_peak, process_rss_peak, open_fd, max_fd, disk_io_wait, disk_free_min, network_retransmits)
--   AVG  — ratios / throughput / rates (fd_util, disk_read/write_mbps, download/upload_mbps, task_slots, active counts)
--   SUM  — cumulative counters that reset per session (swap_used, page_cache, temp_bytes_written, temp_files_open)
--   AVG  — point-in-time snapshots (memory_available, swap, page_cache)

-- Legacy-upgrade fixtures may stop before migration 117 and therefore have no
-- rollup table. Keep this migration total for those fixtures too.
CREATE TABLE IF NOT EXISTS worker_resource_rollups (
    worker_id TEXT NOT NULL,
    session_id TEXT NOT NULL DEFAULT '',
    hour_bucket TEXT NOT NULL,
    sample_count INTEGER NOT NULL DEFAULT 0,
    cpu_percent_avg REAL NOT NULL DEFAULT 0,
    cpu_iowait_percent_avg REAL NOT NULL DEFAULT 0,
    cpu_steal_percent_avg REAL NOT NULL DEFAULT 0,
    load1_avg REAL NOT NULL DEFAULT 0,
    run_queue_avg REAL NOT NULL DEFAULT 0,
    rss_bytes_avg REAL NOT NULL DEFAULT 0,
    memory_used_bytes_avg REAL NOT NULL DEFAULT 0,
    disk_read_bytes_avg REAL NOT NULL DEFAULT 0,
    disk_write_bytes_avg REAL NOT NULL DEFAULT 0,
    network_rx_bytes_avg REAL NOT NULL DEFAULT 0,
    network_tx_bytes_avg REAL NOT NULL DEFAULT 0,
    active_tasks_avg REAL NOT NULL DEFAULT 0,
    ffmpeg_processes_avg REAL NOT NULL DEFAULT 0,
    calculated_at TEXT NOT NULL,
    PRIMARY KEY(worker_id, session_id, hour_bucket)
);

ALTER TABLE worker_resource_rollups ADD COLUMN effective_cpu_cores_avg REAL NOT NULL DEFAULT 0;
ALTER TABLE worker_resource_rollups ADD COLUMN process_rss_peak_bytes_max INTEGER NOT NULL DEFAULT 0;
ALTER TABLE worker_resource_rollups ADD COLUMN memory_available_bytes_avg REAL NOT NULL DEFAULT 0;
ALTER TABLE worker_resource_rollups ADD COLUMN swap_used_bytes_avg REAL NOT NULL DEFAULT 0;
ALTER TABLE worker_resource_rollups ADD COLUMN page_cache_bytes_avg REAL NOT NULL DEFAULT 0;
ALTER TABLE worker_resource_rollups ADD COLUMN temp_bytes_written_max INTEGER NOT NULL DEFAULT 0;
ALTER TABLE worker_resource_rollups ADD COLUMN temp_files_open_max INTEGER NOT NULL DEFAULT 0;
ALTER TABLE worker_resource_rollups ADD COLUMN scratch_current_bytes_avg REAL NOT NULL DEFAULT 0;
ALTER TABLE worker_resource_rollups ADD COLUMN scratch_peak_bytes_max INTEGER NOT NULL DEFAULT 0;
ALTER TABLE worker_resource_rollups ADD COLUMN disk_read_mbps_avg REAL NOT NULL DEFAULT 0;
ALTER TABLE worker_resource_rollups ADD COLUMN disk_write_mbps_avg REAL NOT NULL DEFAULT 0;
ALTER TABLE worker_resource_rollups ADD COLUMN disk_io_wait_ms_max INTEGER NOT NULL DEFAULT 0;
ALTER TABLE worker_resource_rollups ADD COLUMN disk_free_bytes_min INTEGER NOT NULL DEFAULT 0;
ALTER TABLE worker_resource_rollups ADD COLUMN network_retransmits_max INTEGER NOT NULL DEFAULT 0;
ALTER TABLE worker_resource_rollups ADD COLUMN download_mbps_avg REAL NOT NULL DEFAULT 0;
ALTER TABLE worker_resource_rollups ADD COLUMN upload_mbps_avg REAL NOT NULL DEFAULT 0;
ALTER TABLE worker_resource_rollups ADD COLUMN task_slots_avg REAL NOT NULL DEFAULT 0;
ALTER TABLE worker_resource_rollups ADD COLUMN render_jobs_active_avg REAL NOT NULL DEFAULT 0;
ALTER TABLE worker_resource_rollups ADD COLUMN prefetch_jobs_active_avg REAL NOT NULL DEFAULT 0;
ALTER TABLE worker_resource_rollups ADD COLUMN publisher_jobs_active_avg REAL NOT NULL DEFAULT 0;
ALTER TABLE worker_resource_rollups ADD COLUMN open_file_descriptors_max INTEGER NOT NULL DEFAULT 0;
ALTER TABLE worker_resource_rollups ADD COLUMN max_file_descriptors_max INTEGER NOT NULL DEFAULT 0;
ALTER TABLE worker_resource_rollups ADD COLUMN fd_utilization_ratio_avg REAL NOT NULL DEFAULT 0;
