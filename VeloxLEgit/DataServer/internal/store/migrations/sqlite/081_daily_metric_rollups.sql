-- 081_daily_metric_rollups.sql
--
-- Step 2 / Velox Metrics Center: daily metric rollups table.
-- Aggregates per-day metric statistics so historical dashboards
-- can query a single compact table rather than scanning thousands
-- of raw task_attempt_metrics rows.
--
-- Retention strategy:
--   raw task_attempt_metrics: 30-90 days (configurable)
--   daily_metric_rollups:     forever (compacted view)
--
-- Columns:
--   day              — UTC date in YYYY-MM-DD format
--   metric_name      — dotted canonical metric name (from MetricCatalog)
--   executor_id      — executor that processed the attempts ('' = all)
--   worker_id        — worker that ran the attempts ('' = all)
--   avg_value        — arithmetic mean
--   p50_value        — 50th percentile (median)
--   p95_value        — 95th percentile
--   p99_value        — 99th percentile
--   min_value        — minimum observed value
--   max_value        — maximum observed value
--   sample_count     — number of observations in this rollup
--   created_at       — when this rollup row was inserted

CREATE TABLE IF NOT EXISTS daily_metric_rollups (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    day          TEXT    NOT NULL,
    metric_name  TEXT    NOT NULL,
    executor_id  TEXT    NOT NULL DEFAULT '',
    worker_id    TEXT    NOT NULL DEFAULT '',
    avg_value    REAL    NOT NULL DEFAULT 0.0,
    p50_value    REAL    NOT NULL DEFAULT 0.0,
    p95_value    REAL    NOT NULL DEFAULT 0.0,
    p99_value    REAL    NOT NULL DEFAULT 0.0,
    min_value    REAL    NOT NULL DEFAULT 0.0,
    max_value    REAL    NOT NULL DEFAULT 0.0,
    sample_count INTEGER NOT NULL DEFAULT 0,
    created_at   TEXT    NOT NULL DEFAULT (datetime('now'))
);

-- One rollup row per (day, metric_name, executor_id, worker_id).
-- INSERT OR REPLACE so re-running rollups for the same day is idempotent.
CREATE UNIQUE INDEX IF NOT EXISTS idx_daily_metric_rollups_unique
    ON daily_metric_rollups (day, metric_name, executor_id, worker_id);

-- Fast lookup by day + metric for dashboard trend queries.
CREATE INDEX IF NOT EXISTS idx_daily_metric_rollups_day_metric
    ON daily_metric_rollups (day, metric_name);
