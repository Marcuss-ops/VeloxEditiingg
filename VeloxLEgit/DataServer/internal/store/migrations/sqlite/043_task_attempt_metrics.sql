-- 043_task_attempt_metrics.sql
--
-- Stores typed resource counters per attempt.
-- GPU fields remain optional/zero on CPU-only workers.
-- A JSON blob may be retained only as supplementary/debug data.

CREATE TABLE IF NOT EXISTS task_attempt_metrics (
    attempt_id              TEXT PRIMARY KEY,
    input_bytes             INTEGER NOT NULL DEFAULT 0,
    output_bytes            INTEGER NOT NULL DEFAULT 0,
    bytes_from_drive        INTEGER NOT NULL DEFAULT 0,
    bytes_from_blobstore    INTEGER NOT NULL DEFAULT 0,
    bytes_from_local_cache  INTEGER NOT NULL DEFAULT 0,
    cpu_time_ms             INTEGER NOT NULL DEFAULT 0,
    gpu_time_ms             INTEGER NOT NULL DEFAULT 0,
    peak_rss_bytes          INTEGER NOT NULL DEFAULT 0,
    peak_vram_bytes         INTEGER NOT NULL DEFAULT 0,
    debug_json              TEXT NOT NULL DEFAULT '{}'
);
