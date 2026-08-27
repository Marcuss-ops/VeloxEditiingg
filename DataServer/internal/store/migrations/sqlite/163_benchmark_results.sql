-- 163_benchmark_results.sql
-- Persists concurrent benchmark results per worker for scorecard validation
-- and threshold tuning. Each row is one complete benchmark run.

CREATE TABLE IF NOT EXISTS worker_benchmark_results (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    worker_id           TEXT    NOT NULL,
    benchmark_run_id    TEXT    NOT NULL UNIQUE,
    fixture_id          TEXT    NOT NULL,
    max_concurrency     INTEGER NOT NULL,
    runs_per_level      INTEGER NOT NULL,
    cache_mode          TEXT    NOT NULL DEFAULT 'cold_cache',
    sweet_spot          INTEGER NOT NULL DEFAULT 1,
    limiting_factor     TEXT    NOT NULL DEFAULT '',
    -- Per-level results stored as JSON for flexibility.
    levels_json         TEXT    NOT NULL DEFAULT '[]',
    gains_json          TEXT    NOT NULL DEFAULT '[]',
    summary             TEXT    NOT NULL DEFAULT '',
    -- Resource snapshot at benchmark time (for correlation).
    snapshot_ram_bytes      INTEGER NOT NULL DEFAULT 0,
    snapshot_cpu_cores      INTEGER NOT NULL DEFAULT 0,
    snapshot_disk_read_mbps REAL    NOT NULL DEFAULT 0,
    snapshot_upload_mbps    REAL    NOT NULL DEFAULT 0,
    -- Scorecard comparison (populated after validation).
    predicted_render_slots  INTEGER,
    predicted_sweet_spot    INTEGER,
    prediction_accuracy     TEXT,
    -- Threshold tuning suggestions (populated after analysis).
    suggested_ram_safety     REAL,
    suggested_disk_safety    REAL,
    suggested_network_safety REAL,
    tuning_rationale         TEXT,
    -- Timestamps.
    started_at          TEXT    NOT NULL,
    completed_at        TEXT    NOT NULL,
    validated_at        TEXT
);

CREATE INDEX IF NOT EXISTS idx_benchmark_worker ON worker_benchmark_results(worker_id);
CREATE INDEX IF NOT EXISTS idx_benchmark_completed ON worker_benchmark_results(completed_at);
