-- 119_performance_benchmark_runs.sql
--
-- Immutable benchmark evidence submitted by perf_matrix.sh.  run_id is the
-- replay identity; payload_hash lets the master accept an identical replay
-- without duplicating the row while rejecting divergent evidence.

CREATE TABLE IF NOT EXISTS performance_benchmark_runs (
    run_id TEXT PRIMARY KEY,
    payload_hash TEXT NOT NULL,
    benchmark_case_id TEXT NOT NULL,
    job_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    attempt_id TEXT NOT NULL,

    worker_id TEXT NOT NULL,
    worker_snapshot_id TEXT NOT NULL DEFAULT '',
    cache_mode TEXT NOT NULL,

    git_sha TEXT NOT NULL DEFAULT '',
    engine_version TEXT NOT NULL DEFAULT '',
    ffmpeg_version TEXT NOT NULL DEFAULT '',
    config_hash TEXT NOT NULL DEFAULT '',
    docker_image_digest TEXT NOT NULL DEFAULT '',

    status TEXT NOT NULL,
    render_factor REAL NOT NULL DEFAULT 0,
    wall_ms REAL NOT NULL DEFAULT 0,
    output_duration_ms REAL NOT NULL DEFAULT 0,
    output_sha256 TEXT NOT NULL DEFAULT '',

    created_at TEXT NOT NULL,

    CHECK (benchmark_case_id <> ''),
    CHECK (cache_mode IN ('cold_cache', 'warm_cache'))
);

CREATE INDEX IF NOT EXISTS idx_benchmark_runs_case_cache
    ON performance_benchmark_runs(benchmark_case_id, cache_mode, created_at);

CREATE INDEX IF NOT EXISTS idx_benchmark_runs_attempt
    ON performance_benchmark_runs(attempt_id);

CREATE INDEX IF NOT EXISTS idx_benchmark_runs_worker
    ON performance_benchmark_runs(worker_id, created_at);
