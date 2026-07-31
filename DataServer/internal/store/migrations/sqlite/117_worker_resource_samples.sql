-- 117_worker_resource_samples.sql
--
-- Worker resource history. The worker samples host resources locally and
-- sends the latest typed snapshot in Heartbeat.resources. The master stores
-- the observed sample timestamp separately from ingested_at so clock skew is
-- visible and retention is based on a trusted master timestamp.
--
-- Raw samples: retain for the configured operational window (default 90d).
-- Hourly rollups: retain for the configured historical window (default 365d).
-- The composite uniqueness key makes a replay of the same worker/session/
-- sampled_at observation a no-op without mixing worker restarts.

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

CREATE INDEX IF NOT EXISTS idx_worker_resource_samples_worker_time
    ON worker_resource_samples(worker_id, sampled_at DESC);

CREATE INDEX IF NOT EXISTS idx_worker_resource_samples_ingested
    ON worker_resource_samples(ingested_at);

CREATE INDEX IF NOT EXISTS idx_worker_resource_samples_session_time
    ON worker_resource_samples(worker_id, session_id, sampled_at DESC);

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

CREATE INDEX IF NOT EXISTS idx_worker_resource_rollups_worker_time
    ON worker_resource_rollups(worker_id, hour_bucket DESC);

CREATE INDEX IF NOT EXISTS idx_worker_resource_rollups_hour
    ON worker_resource_rollups(hour_bucket);
