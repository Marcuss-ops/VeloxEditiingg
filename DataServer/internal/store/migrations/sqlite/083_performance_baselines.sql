-- 083_performance_baselines.sql
--
-- Scorecard v2 / Metrics Center: stores historical performance baselines
-- for comparable workloads. A baseline is keyed by the combination of
-- workload class, git SHA, config hash and worker class so that current
-- attempts can be compared against previous versions.
--
-- baseline_id          — canonical baseline identity (UUID)
-- workload_key         — opaque key describing a comparable workload class
-- git_sha              — git commit SHA that produced this baseline
-- config_hash          — hash of the configuration used for the sample set
-- worker_class         — class/category of the worker hardware
-- sample_count         — number of attempts used to compute the percentiles
-- p50_wall_ms          — median wall-clock time in milliseconds
-- p95_wall_ms          — 95th percentile wall-clock time in milliseconds
-- p50_render_factor    — median render factor (worker_execution_seconds / output_duration_seconds)
-- p95_render_factor    — 95th percentile render factor
-- error_rate           — fraction of failed attempts in the sample set
-- created_at           — RFC3339 timestamp when the baseline was computed

CREATE TABLE IF NOT EXISTS performance_baselines (
    baseline_id        TEXT PRIMARY KEY,
    workload_key       TEXT NOT NULL,
    git_sha            TEXT NOT NULL,
    config_hash        TEXT NOT NULL,
    worker_class       TEXT NOT NULL,
    sample_count       INTEGER NOT NULL,
    p50_wall_ms        REAL NOT NULL,
    p95_wall_ms        REAL NOT NULL,
    p50_render_factor  REAL NOT NULL,
    p95_render_factor  REAL NOT NULL,
    error_rate         REAL NOT NULL,
    created_at         TEXT NOT NULL,
    UNIQUE(workload_key, git_sha, config_hash, worker_class)
);

-- Fast lookup of all baselines for a workload class.
CREATE INDEX IF NOT EXISTS idx_performance_baselines_workload_key
    ON performance_baselines(workload_key);

-- Fast lookup of all baselines for a specific git SHA / config pair.
CREATE INDEX IF NOT EXISTS idx_performance_baselines_git_sha_config
    ON performance_baselines(git_sha, config_hash);
