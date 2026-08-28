-- 165_worker_resource_capacity_metrics.sql
-- Make the Master SQL history lossless for capacity certification.

-- Some legacy-upgrade fixtures intentionally stop before migration 117 and
-- do not exercise worker history. Keep this migration total for those stores.
CREATE TABLE IF NOT EXISTS worker_resource_samples (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    worker_id TEXT NOT NULL,
    session_id TEXT NOT NULL DEFAULT '',
    sampled_at TEXT NOT NULL,
    ingested_at TEXT NOT NULL,
    cpu_percent REAL NOT NULL DEFAULT 0,
    cpu_iowait_percent REAL NOT NULL DEFAULT 0,
    cpu_steal_percent REAL NOT NULL DEFAULT 0,
    load1 REAL NOT NULL DEFAULT 0,
    run_queue INTEGER NOT NULL DEFAULT 0,
    rss_bytes INTEGER NOT NULL DEFAULT 0,
    memory_used_bytes INTEGER NOT NULL DEFAULT 0,
    major_page_faults INTEGER NOT NULL DEFAULT 0,
    disk_read_bytes INTEGER NOT NULL DEFAULT 0,
    disk_write_bytes INTEGER NOT NULL DEFAULT 0,
    disk_free_bytes INTEGER NOT NULL DEFAULT 0,
    network_rx_bytes INTEGER NOT NULL DEFAULT 0,
    network_tx_bytes INTEGER NOT NULL DEFAULT 0,
    active_tasks INTEGER NOT NULL DEFAULT 0,
    ffmpeg_processes INTEGER NOT NULL DEFAULT 0,
    UNIQUE(worker_id, session_id, sampled_at)
);

ALTER TABLE worker_resource_samples ADD COLUMN effective_cpu_cores INTEGER NOT NULL DEFAULT 0;
ALTER TABLE worker_resource_samples ADD COLUMN process_rss_peak_bytes INTEGER NOT NULL DEFAULT 0;
ALTER TABLE worker_resource_samples ADD COLUMN memory_available_bytes INTEGER NOT NULL DEFAULT 0;
ALTER TABLE worker_resource_samples ADD COLUMN swap_used_bytes INTEGER NOT NULL DEFAULT 0;
ALTER TABLE worker_resource_samples ADD COLUMN page_cache_bytes INTEGER NOT NULL DEFAULT 0;
ALTER TABLE worker_resource_samples ADD COLUMN temp_bytes_written INTEGER NOT NULL DEFAULT 0;
ALTER TABLE worker_resource_samples ADD COLUMN temp_files_open INTEGER NOT NULL DEFAULT 0;
ALTER TABLE worker_resource_samples ADD COLUMN scratch_current_bytes INTEGER NOT NULL DEFAULT 0;
ALTER TABLE worker_resource_samples ADD COLUMN scratch_peak_bytes INTEGER NOT NULL DEFAULT 0;
ALTER TABLE worker_resource_samples ADD COLUMN disk_read_mbps REAL NOT NULL DEFAULT 0;
ALTER TABLE worker_resource_samples ADD COLUMN disk_write_mbps REAL NOT NULL DEFAULT 0;
ALTER TABLE worker_resource_samples ADD COLUMN disk_io_wait_ms INTEGER NOT NULL DEFAULT 0;
ALTER TABLE worker_resource_samples ADD COLUMN network_retransmits INTEGER NOT NULL DEFAULT 0;
ALTER TABLE worker_resource_samples ADD COLUMN download_mbps REAL NOT NULL DEFAULT 0;
ALTER TABLE worker_resource_samples ADD COLUMN upload_mbps REAL NOT NULL DEFAULT 0;
ALTER TABLE worker_resource_samples ADD COLUMN task_slots INTEGER NOT NULL DEFAULT 0;
ALTER TABLE worker_resource_samples ADD COLUMN render_jobs_active INTEGER NOT NULL DEFAULT 0;
ALTER TABLE worker_resource_samples ADD COLUMN prefetch_jobs_active INTEGER NOT NULL DEFAULT 0;
ALTER TABLE worker_resource_samples ADD COLUMN publisher_jobs_active INTEGER NOT NULL DEFAULT 0;
ALTER TABLE worker_resource_samples ADD COLUMN open_file_descriptors INTEGER NOT NULL DEFAULT 0;
ALTER TABLE worker_resource_samples ADD COLUMN max_file_descriptors INTEGER NOT NULL DEFAULT 0;
ALTER TABLE worker_resource_samples ADD COLUMN fd_utilization_ratio REAL NOT NULL DEFAULT 0;
