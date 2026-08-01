-- observability-dashboards.sql
--
-- Operator-facing SQL for the eight observability dashboards. Run against
-- the master SQLite database in a read-only session. Prometheus panels use
-- only bounded labels, this file intentionally contains job_id/task_id/
-- attempt_id and immutable version identifiers for drill-down analysis.
--
-- Time parameters use SQLite date expressions. Replace :from_day and
-- :to_day with ISO dates when using a client that supports parameters.

-- ========================================================================
-- 1. ATTEMPT EXPLORER
-- ========================================================================
-- Complete event Gantt for one attempt. The event table is append-only and
-- supports repeated engine.encode events without overwriting them.
SELECT
    e.attempt_id,
    e.job_id,
    e.task_id,
    e.event_index,
    e.origin,
    e.scope,
    e.component,
    e.action,
    e.phase,
    e.status,
    e.started_at,
    e.completed_at,
    e.duration_ms,
    e.bytes_in,
    e.bytes_out,
    e.frames,
    e.error_code,
    e.worker_id,
    e.worker_session_id,
    e.worker_snapshot_id,
    e.executor_id,
    e.executor_version,
    e.metadata_json
FROM task_execution_events e
WHERE e.attempt_id = :attempt_id
ORDER BY e.event_index ASC, e.started_at ASC;

-- Attempt summary with the immutable runtime identity used for rendering.
SELECT
    a.id AS attempt_id,
    a.task_id,
    a.attempt_number,
    a.status,
    a.started_at,
    a.completed_at,
    a.worker_id,
    a.worker_session_id,
    a.worker_snapshot_id,
    a.git_sha,
    a.worker_version,
    a.engine_version,
    a.ffmpeg_version,
    a.config_hash,
    a.docker_image_digest,
    s.hostname,
    s.worker_class,
    s.cpu_model,
    s.logical_cpu_count,
    s.effective_cpu_count,
    s.cpu_quota,
    s.total_memory_bytes,
    s.gpu_model,
    s.kernel_version,
    s.storage_class
FROM task_attempts a
LEFT JOIN worker_runtime_snapshots s
  ON s.snapshot_id = a.worker_snapshot_id
WHERE a.id = :attempt_id;

-- ========================================================================
-- 2. WORKER RANKING
-- ========================================================================
-- Rank workers by render factor, success rate, cache ratio and phase time.
-- The worker_id is deliberately visible here because SQL is the detail
-- plane, do not copy these dimensions into Prometheus labels.
WITH attempts AS (
    SELECT
        a.worker_id,
        COUNT(*) AS attempts,
        SUM(CASE WHEN a.status = 'SUCCEEDED' THEN 1 ELSE 0 END) AS succeeded,
        AVG(m.wall_clock_seconds / NULLIF(m.media_duration_seconds, 0)) AS avg_render_factor,
        AVG(m.cpu_time_ms / NULLIF(m.media_duration_seconds, 0)) AS avg_cpu_ms_per_output_second,
        AVG(m.engine_asset_download_ms) AS avg_download_ms,
        AVG(m.engine_segment_build_ms) AS avg_encode_ms,
        AVG(m.retry_count) AS avg_retry_count
    FROM task_attempts a
    JOIN task_attempt_metrics m ON m.attempt_id = a.id
    WHERE COALESCE(NULLIF(a.completed_at, ''), a.updated_at) >= datetime('now', '-7 days')
    GROUP BY a.worker_id
), cache AS (
    SELECT
        a.worker_id,
        SUM(COALESCE(c.cache_hits, 0)) AS cache_hits,
        SUM(COALESCE(c.cache_misses, 0)) AS cache_misses
    FROM task_attempts a
    LEFT JOIN task_attempt_cache_stats c ON c.attempt_id = a.id
    WHERE COALESCE(NULLIF(a.completed_at, ''), a.updated_at) >= datetime('now', '-7 days')
    GROUP BY a.worker_id
)
SELECT
    x.worker_id,
    x.attempts,
    x.succeeded,
    ROUND(100.0 * x.succeeded / NULLIF(x.attempts, 0), 2) AS success_pct,
    ROUND(x.avg_render_factor, 4) AS avg_render_factor,
    ROUND(x.avg_cpu_ms_per_output_second, 1) AS avg_cpu_ms_per_output_second,
    ROUND(x.avg_download_ms, 1) AS avg_download_ms,
    ROUND(x.avg_encode_ms, 1) AS avg_encode_ms,
    ROUND(x.avg_retry_count, 2) AS avg_retry_count,
    COALESCE(c.cache_hits, 0) AS cache_hits,
    COALESCE(c.cache_misses, 0) AS cache_misses,
    ROUND(1.0 * COALESCE(c.cache_hits, 0) /
        NULLIF(COALESCE(c.cache_hits, 0) + COALESCE(c.cache_misses, 0), 0), 4) AS cache_hit_ratio
FROM attempts x
LEFT JOIN cache c ON c.worker_id = x.worker_id
WHERE x.attempts >= 10
ORDER BY x.avg_render_factor ASC, success_pct DESC;

-- ========================================================================
-- 3. VERSION REGRESSION
-- ========================================================================
-- Compare equivalent workload cohorts across executor/runtime versions.
SELECT
    d.cohort_base_key,
    d.phase,
    d.executor_id,
    d.executor_version,
    d.worker_class,
    d.git_sha,
    d.engine_version,
    d.ffmpeg_version,
    d.docker_image_digest,
    d.config_hash,
    SUM(d.attempts) AS attempts,
    SUM(d.succeeded) AS succeeded,
    ROUND(SUM(d.phase_ms_total) / NULLIF(SUM(d.attempts), 0), 1) AS avg_phase_ms,
    ROUND(AVG(NULLIF(d.phase_ms_p95, 0)), 1) AS p95_phase_ms,
    ROUND(AVG(NULLIF(d.baseline_p25_ms, 0)), 1) AS baseline_p25_ms,
    ROUND(SUM(d.recoverable_ms_total), 1) AS recoverable_ms_total
FROM render_performance_daily d
WHERE d.day >= date('now', '-30 days')
GROUP BY d.cohort_base_key, d.phase, d.executor_id, d.executor_version,
         d.worker_class, d.git_sha, d.engine_version, d.ffmpeg_version,
         d.docker_image_digest, d.config_hash
ORDER BY d.cohort_base_key, d.phase, avg_phase_ms DESC;

-- ========================================================================
-- 4. RECOVERABLE TIME
-- ========================================================================
-- Daily phase view: observed p25, healthy baseline p25 and seconds that
-- could be recovered if the cohort returned to its healthy baseline.
SELECT
    d.day,
    d.cohort_base_key,
    d.phase,
    d.worker_class,
    d.executor_id,
    d.executor_version,
    d.attempts,
    ROUND(d.phase_ms_p25, 1) AS observed_p25_ms,
    ROUND(d.baseline_p25_ms, 1) AS healthy_baseline_p25_ms,
    ROUND(MAX(0, d.phase_ms_p25 - d.baseline_p25_ms), 1) AS recoverable_p25_ms,
    ROUND(d.recoverable_ms_total, 1) AS recoverable_ms_total,
    ROUND(d.wall_ms_total, 1) AS wall_ms_total,
    d.git_sha,
    d.engine_version,
    d.ffmpeg_version,
    d.docker_image_digest,
    d.config_hash
FROM render_performance_daily d
WHERE d.day BETWEEN COALESCE(NULLIF(:from_day, ''), date('now', '-30 days'))
                AND COALESCE(NULLIF(:to_day, ''), date('now'))
ORDER BY d.recoverable_ms_total DESC, d.day DESC;

-- ========================================================================
-- 5. COLD VS WARM CACHE
-- ========================================================================
-- Permanent benchmark evidence. Keep benchmark_case_id and output hash in
-- SQL so a warm run is compared only with the matching deterministic case.
SELECT
    benchmark_case_id,
    cache_mode,
    COUNT(*) AS runs,
    SUM(CASE WHEN status = 'SUCCEEDED' THEN 1 ELSE 0 END) AS succeeded,
    ROUND(AVG(render_factor), 4) AS avg_render_factor,
    ROUND(MIN(render_factor), 4) AS best_render_factor,
    ROUND(AVG(wall_ms), 1) AS avg_wall_ms,
    ROUND(AVG(output_duration_ms), 1) AS output_duration_ms,
    COUNT(DISTINCT output_sha256) AS distinct_output_hashes,
    MIN(created_at) AS first_run_at,
    MAX(created_at) AS last_run_at
FROM performance_benchmark_runs
WHERE benchmark_case_id = 'gervais-final-v1'
GROUP BY benchmark_case_id, cache_mode
ORDER BY cache_mode;

SELECT
    run_id,
    benchmark_case_id,
    cache_mode,
    job_id,
    task_id,
    attempt_id,
    worker_id,
    worker_snapshot_id,
    git_sha,
    engine_version,
    ffmpeg_version,
    config_hash,
    docker_image_digest,
    status,
    render_factor,
    wall_ms,
    output_duration_ms,
    output_sha256,
    created_at
FROM performance_benchmark_runs
WHERE benchmark_case_id = 'gervais-final-v1'
ORDER BY created_at DESC;

-- ========================================================================
-- 6. PARALLELISM EFFICIENCY
-- ========================================================================
SELECT
    a.id AS attempt_id,
    a.task_id,
    a.worker_id,
    a.worker_snapshot_id,
    a.engine_version,
    p.configured_segment_workers,
    p.ffmpeg_threads_per_segment,
    p.logical_cpu_count,
    p.cpu_budget,
    ROUND(p.serial_work_ms, 1) AS serial_work_ms,
    ROUND(p.render_window_ms, 1) AS render_window_ms,
    ROUND(p.overlap_ms, 1) AS overlap_ms,
    ROUND(p.idle_gap_ms, 1) AS idle_gap_ms,
    p.peak_concurrency,
    ROUND(p.average_concurrency, 3) AS average_concurrency,
    ROUND(p.speedup_vs_serial, 3) AS speedup_vs_serial,
    ROUND(p.parallel_efficiency_ratio, 3) AS parallel_efficiency_ratio,
    ROUND(p.cpu_oversubscription_ratio, 3) AS cpu_oversubscription_ratio,
    p.bottleneck_phase,
    p.parallel_strategy
FROM task_attempt_parallelism p
JOIN task_attempts a ON a.id = p.attempt_id
WHERE COALESCE(NULLIF(a.completed_at, ''), a.updated_at) >= datetime('now', '-7 days')
ORDER BY p.parallel_efficiency_ratio ASC, p.cpu_oversubscription_ratio DESC;

-- Segment-level timeline to explain idle gaps and stragglers.
SELECT
    s.attempt_id,
    s.worker_id,
    s.segment_index,
    s.parallel_group,
    s.worker_slot,
    s.cpu_threads,
    s.started_offset_ms,
    s.finished_offset_ms,
    s.duration_ms,
    s.status,
    s.source_type
FROM task_attempt_segment_timings s
WHERE s.attempt_id = :attempt_id
ORDER BY s.started_offset_ms, s.segment_index;

-- ========================================================================
-- 7. QUALITY VS SPEED
-- ========================================================================
SELECT
    a.id AS attempt_id,
    a.task_id,
    a.worker_id,
    a.worker_snapshot_id,
    a.git_sha,
    a.engine_version,
    a.ffmpeg_version,
    a.config_hash,
    a.status,
    ROUND(m.media_duration_seconds, 3) AS media_duration_seconds,
    ROUND(m.wall_clock_seconds, 3) AS wall_clock_seconds,
    ROUND(m.media_duration_seconds / NULLIF(m.wall_clock_seconds, 0), 3) AS render_speed_ratio,
    m.ffprobe_valid,
    ROUND(m.duration_diff_sec, 3) AS duration_diff_sec,
    m.has_video_stream,
    m.has_audio_stream,
    m.output_file_size,
    ROUND(m.black_frame_ratio, 5) AS black_frame_ratio,
    m.audio_sync_offset_ms,
    m.output_sha256
FROM task_attempts a
JOIN task_attempt_metrics m ON m.attempt_id = a.id
WHERE COALESCE(NULLIF(a.completed_at, ''), a.updated_at) >= datetime('now', '-7 days')
ORDER BY render_speed_ratio DESC;

-- Quality outliers among otherwise successful attempts.
SELECT
    a.id AS attempt_id,
    a.worker_id,
    a.engine_version,
    m.media_duration_seconds / NULLIF(m.wall_clock_seconds, 0) AS render_speed_ratio,
    m.black_frame_ratio,
    m.audio_sync_offset_ms,
    m.duration_diff_sec,
    m.ffprobe_valid
FROM task_attempts a
JOIN task_attempt_metrics m ON m.attempt_id = a.id
WHERE a.status = 'SUCCEEDED'
  AND (m.ffprobe_valid = 0
       OR m.black_frame_ratio > 0.01
       OR ABS(m.audio_sync_offset_ms) > 100
       OR ABS(m.duration_diff_sec) > 0.25)
ORDER BY render_speed_ratio DESC;

-- ========================================================================
-- 8. WASTE
-- ========================================================================
SELECT
    a.id AS attempt_id,
    a.task_id,
    a.attempt_number,
    a.worker_id,
    a.worker_snapshot_id,
    a.status,
    a.error_code,
    a.git_sha,
    a.engine_version,
    a.ffmpeg_version,
    m.retry_count,
    m.wasted_cpu_ms,
    m.wasted_download_bytes,
    m.wasted_cost_estimate,
    m.completed_segments,
    ROUND(m.wasted_cpu_ms / 1000.0, 3) AS wasted_cpu_seconds,
    ROUND(m.wasted_cost_estimate, 6) AS wasted_eur
FROM task_attempts a
JOIN task_attempt_metrics m ON m.attempt_id = a.id
WHERE COALESCE(NULLIF(a.completed_at, ''), a.updated_at) >= datetime('now', '-30 days')
  AND (m.retry_count > 0 OR m.wasted_cpu_ms > 0
       OR m.wasted_download_bytes > 0 OR m.wasted_cost_estimate > 0)
ORDER BY m.wasted_cost_estimate DESC, m.wasted_cpu_ms DESC;

SELECT
    a.worker_id,
    a.engine_version,
    COUNT(*) AS attempts_with_waste,
    SUM(m.retry_count) AS retries,
    SUM(m.wasted_cpu_ms) AS wasted_cpu_ms,
    SUM(m.wasted_download_bytes) AS wasted_download_bytes,
    SUM(m.wasted_cost_estimate) AS wasted_cost_estimate
FROM task_attempts a
JOIN task_attempt_metrics m ON m.attempt_id = a.id
WHERE COALESCE(NULLIF(a.completed_at, ''), a.updated_at) >= datetime('now', '-30 days')
GROUP BY a.worker_id, a.engine_version
HAVING retries > 0 OR wasted_cpu_ms > 0 OR wasted_download_bytes > 0
ORDER BY wasted_cost_estimate DESC, wasted_cpu_ms DESC;
