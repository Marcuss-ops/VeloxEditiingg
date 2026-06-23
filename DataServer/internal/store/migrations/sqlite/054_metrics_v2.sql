-- 054_metrics_v2.sql
--
-- Scorecard v1 / PR-5 typed metrics.
-- Adds the typed columns the worker emits via TaskExecutionMetrics (proto v3)
-- plus derived columns the master computes on ingest for the Project
-- Performance Scorecard (the 12 green/yellow/red ratios). Helper
-- table task_attempt_cache_stats carries the cache hit/miss/eviction
-- counters the worker surfaces as dotted-key entries; we hoist them
-- into a typed table so percentiles are easy to compute without
-- walking a JSON blob.
--
-- SQLite constraint reminder: each ALTER TABLE ADD COLUMN is its own
-- statement. DEFAULT must be constant (0 / '' / 0.0). CURRENT_TIMESTAMP
-- is NOT a constant here — we use TEXT DEFAULT '' and the application
-- layer stamps RFC3339 at write-time.

ALTER TABLE task_attempt_metrics
    ADD COLUMN frames_decoded      INTEGER NOT NULL DEFAULT 0;
ALTER TABLE task_attempt_metrics
    ADD COLUMN frames_composited   INTEGER NOT NULL DEFAULT 0;
ALTER TABLE task_attempt_metrics
    ADD COLUMN frames_encoded      INTEGER NOT NULL DEFAULT 0;
ALTER TABLE task_attempt_metrics
    ADD COLUMN ffmpeg_speed_ratio  REAL    NOT NULL DEFAULT 0.0;
ALTER TABLE task_attempt_metrics
    ADD COLUMN encode_passes       INTEGER NOT NULL DEFAULT 0;
ALTER TABLE task_attempt_metrics
    ADD COLUMN final_concat_stream_copy INTEGER NOT NULL DEFAULT 0;
ALTER TABLE task_attempt_metrics
    ADD COLUMN concat_mode         TEXT    NOT NULL DEFAULT 'n/a';
ALTER TABLE task_attempt_metrics
    ADD COLUMN temp_bytes_written  INTEGER NOT NULL DEFAULT 0;
ALTER TABLE task_attempt_metrics
    ADD COLUMN duplicate_download_bytes INTEGER NOT NULL DEFAULT 0;
ALTER TABLE task_attempt_metrics
    ADD COLUMN media_duration_seconds REAL NOT NULL DEFAULT 0.0;
ALTER TABLE task_attempt_metrics
    ADD COLUMN wall_clock_seconds REAL NOT NULL DEFAULT 0.0;

-- Per-attempt cache stats snapshot at attempt-close time. The worker
-- emits this as cache.hits / cache.misses / cache.evictions /
-- cache.bytes in report.Metrics; we extract + persist here so the
-- byte_hit_ratio can be computed in SQL rather than re-walking a
-- JSON document. Index by attempt_id is implicit (PRIMARY KEY).
CREATE TABLE IF NOT EXISTS task_attempt_cache_stats (
    attempt_id          TEXT PRIMARY KEY,
    cache_hits          INTEGER NOT NULL DEFAULT 0,
    cache_misses        INTEGER NOT NULL DEFAULT 0,
    cache_evictions     INTEGER NOT NULL DEFAULT 0,
    cache_corruptions   INTEGER NOT NULL DEFAULT 0,
    cache_bytes_used    INTEGER NOT NULL DEFAULT 0,
    cache_entries       INTEGER NOT NULL DEFAULT 0
);

-- Cost basis snapshot (worker-emitted; see TaskExecutionMetrics fields
-- 15-17). Saved as a single row per attempt so cost_per_output_minute
-- is a 1:1 read at the API layer.
CREATE TABLE IF NOT EXISTS task_attempt_cost_basis (
    attempt_id              TEXT PRIMARY KEY,
    cpu_price_per_second    REAL NOT NULL DEFAULT 0.0,
    storage_price_per_gb    REAL NOT NULL DEFAULT 0.0,
    network_price_per_gb    REAL NOT NULL DEFAULT 0.0,
    cpu_time_seconds_total  REAL NOT NULL DEFAULT 0.0,
    storage_gb_written      REAL NOT NULL DEFAULT 0.0,
    network_gb_egressed     REAL NOT NULL DEFAULT 0.0,
    output_minutes_total    REAL NOT NULL DEFAULT 0.0
);

CREATE INDEX IF NOT EXISTS idx_task_attempt_metrics_concat_mode
    ON task_attempt_metrics(concat_mode);
