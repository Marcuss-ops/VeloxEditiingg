-- 118_render_performance_daily.sql
--
-- Historical render performance by equivalent workload cohort and phase.
-- Raw attempt/phase rows remain the source of truth; this table is an
-- idempotent compact daily view used for regression and recoverable-time
-- dashboards.
--
-- cohort_key includes the executor version. cohort_base_key intentionally
-- excludes executor version so a version can be compared with its peers.
-- baseline_p25_ms is calculated from earlier healthy attempts in the same
-- cohort_base_key/worker_class/phase, and recoverable_ms_total is the sum of
-- max(0, observed_phase_ms - baseline_p25_ms) for the day's observations.

CREATE TABLE IF NOT EXISTS render_performance_daily (
    day TEXT NOT NULL,
    cohort_key TEXT NOT NULL,
    cohort_base_key TEXT NOT NULL,
    phase TEXT NOT NULL,

    executor_id TEXT NOT NULL DEFAULT '',
    executor_version INTEGER NOT NULL DEFAULT 0,
    worker_id TEXT NOT NULL DEFAULT '',
    worker_class TEXT NOT NULL DEFAULT '',

    git_sha TEXT NOT NULL DEFAULT '',
    engine_version TEXT NOT NULL DEFAULT '',
    ffmpeg_version TEXT NOT NULL DEFAULT '',
    docker_image_digest TEXT NOT NULL DEFAULT '',
    config_hash TEXT NOT NULL DEFAULT '',

    attempts INTEGER NOT NULL DEFAULT 0,
    succeeded INTEGER NOT NULL DEFAULT 0,
    failed INTEGER NOT NULL DEFAULT 0,

    phase_ms_total REAL NOT NULL DEFAULT 0,
    phase_ms_avg REAL NOT NULL DEFAULT 0,
    phase_ms_p25 REAL NOT NULL DEFAULT 0,
    phase_ms_p50 REAL NOT NULL DEFAULT 0,
    phase_ms_p95 REAL NOT NULL DEFAULT 0,
    phase_ms_p99 REAL NOT NULL DEFAULT 0,
    baseline_p25_ms REAL NOT NULL DEFAULT 0,
    recoverable_ms_total REAL NOT NULL DEFAULT 0,

    output_seconds REAL NOT NULL DEFAULT 0,
    wall_ms_total REAL NOT NULL DEFAULT 0,
    cpu_ms_total REAL NOT NULL DEFAULT 0,
    download_ms_total REAL NOT NULL DEFAULT 0,
    decode_ms_total REAL NOT NULL DEFAULT 0,
    composite_ms_total REAL NOT NULL DEFAULT 0,
    encode_ms_total REAL NOT NULL DEFAULT 0,
    upload_ms_total REAL NOT NULL DEFAULT 0,
    output_bytes_total INTEGER NOT NULL DEFAULT 0,
    temp_bytes_total INTEGER NOT NULL DEFAULT 0,
    wasted_cpu_ms_total INTEGER NOT NULL DEFAULT 0,
    wasted_download_bytes_total INTEGER NOT NULL DEFAULT 0,
    render_factor_avg REAL NOT NULL DEFAULT 0,

    calculated_at TEXT NOT NULL,

    PRIMARY KEY (day, cohort_key, phase, worker_id, config_hash)
);

CREATE INDEX IF NOT EXISTS idx_render_performance_daily_day
    ON render_performance_daily(day, phase);

CREATE INDEX IF NOT EXISTS idx_render_performance_daily_cohort
    ON render_performance_daily(cohort_base_key, phase, day);

CREATE INDEX IF NOT EXISTS idx_render_performance_daily_version
    ON render_performance_daily(cohort_base_key, phase, executor_version, day);

CREATE INDEX IF NOT EXISTS idx_render_performance_daily_recoverable
    ON render_performance_daily(day, recoverable_ms_total DESC);
