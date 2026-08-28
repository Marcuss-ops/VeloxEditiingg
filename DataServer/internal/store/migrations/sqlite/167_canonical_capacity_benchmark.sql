-- 167_canonical_capacity_benchmark.sql
-- Replace the legacy JSON benchmark payload with normalized SQL facts.

ALTER TABLE worker_benchmark_results RENAME TO capacity_benchmark_runs;
ALTER TABLE capacity_benchmark_runs DROP COLUMN levels_json;
ALTER TABLE capacity_benchmark_runs DROP COLUMN gains_json;

CREATE TABLE capacity_benchmark_levels (
    benchmark_run_id TEXT NOT NULL REFERENCES capacity_benchmark_runs(benchmark_run_id) ON DELETE CASCADE,
    level INTEGER NOT NULL,
    total_runs INTEGER NOT NULL DEFAULT 0,
    successful_runs INTEGER NOT NULL DEFAULT 0,
    failed_runs INTEGER NOT NULL DEFAULT 0,
    total_wall_ms INTEGER NOT NULL DEFAULT 0,
    avg_wall_ms REAL NOT NULL DEFAULT 0,
    p50_wall_ms REAL NOT NULL DEFAULT 0,
    p95_wall_ms REAL NOT NULL DEFAULT 0,
    throughput REAL NOT NULL DEFAULT 0,
    peak_ram_bytes INTEGER NOT NULL DEFAULT 0,
    avg_ram_bytes INTEGER NOT NULL DEFAULT 0,
    render_wall_ms REAL NOT NULL DEFAULT 0,
    upload_wall_ms REAL NOT NULL DEFAULT 0,
    render_per_job_ms REAL NOT NULL DEFAULT 0,
    upload_per_job_ms REAL NOT NULL DEFAULT 0,
    render_jobs_active INTEGER NOT NULL DEFAULT 0,
    prefetch_jobs_active INTEGER NOT NULL DEFAULT 0,
    publisher_active INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (benchmark_run_id, level)
);

CREATE TABLE capacity_benchmark_gains (
    benchmark_run_id TEXT NOT NULL REFERENCES capacity_benchmark_runs(benchmark_run_id) ON DELETE CASCADE,
    from_level INTEGER NOT NULL,
    to_level INTEGER NOT NULL,
    gain_percent REAL NOT NULL DEFAULT 0,
    absolute_gain REAL NOT NULL DEFAULT 0,
    is_efficient INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (benchmark_run_id, from_level, to_level)
);

CREATE INDEX idx_capacity_levels_run ON capacity_benchmark_levels(benchmark_run_id, level);
CREATE INDEX idx_capacity_gains_run ON capacity_benchmark_gains(benchmark_run_id, to_level);
