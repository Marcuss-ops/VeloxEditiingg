-- 162_worker_capacity_scorecard.sql
-- Persists the CapacityScorecard computation per worker so per-phase slot
-- limits survive Master restarts and are available during registry hydration.

CREATE TABLE IF NOT EXISTS worker_capacity_scorecards (
    worker_id           TEXT PRIMARY KEY,
    render_slots        INTEGER NOT NULL DEFAULT 0,
    prefetch_slots      INTEGER NOT NULL DEFAULT 0,
    publisher_slots     INTEGER NOT NULL DEFAULT 0,
    ram_slots           INTEGER NOT NULL DEFAULT 0,
    cpu_slots           INTEGER NOT NULL DEFAULT 0,
    disk_slots          INTEGER NOT NULL DEFAULT 0,
    network_slots       INTEGER NOT NULL DEFAULT 0,
    limiting_resource   TEXT    NOT NULL DEFAULT '',
    total_ram_bytes     INTEGER NOT NULL DEFAULT 0,
    available_ram_bytes INTEGER NOT NULL DEFAULT 0,
    effective_cpu_cores INTEGER NOT NULL DEFAULT 0,
    disk_read_mbps      REAL    NOT NULL DEFAULT 0,
    disk_write_mbps     REAL    NOT NULL DEFAULT 0,
    download_mbps       REAL    NOT NULL DEFAULT 0,
    upload_mbps         REAL    NOT NULL DEFAULT 0,
    ram_per_job_bytes   INTEGER NOT NULL DEFAULT 0,
    cpu_cores_per_job   REAL    NOT NULL DEFAULT 0,
    disk_mbps_per_job   REAL    NOT NULL DEFAULT 0,
    network_mbps_per_job REAL   NOT NULL DEFAULT 0,
    render_wall_ms_per_job   INTEGER NOT NULL DEFAULT 0,
    prefetch_wall_ms_per_job INTEGER NOT NULL DEFAULT 0,
    publish_wall_ms_per_job  INTEGER NOT NULL DEFAULT 0,
    computed_at         TEXT    NOT NULL
);
