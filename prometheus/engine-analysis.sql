-- engine-analysis.sql
--
-- Scorecard v2 / Step 7: analytical SQL queries for the engine
-- phase + segment timing tables. Run against the Velox master's
-- SQLite database (velox.db) to surface historical trends, slowest
-- workers, and straggler candidates without joining the Prometheus
-- TSDB. All queries target the migration 070 schema.
--
-- These queries are designed to be copy-pasted directly into a
-- read-only SQLite session or run through the sqlite3 CLI pointed
-- at the production database.  All time-filters use the ISO-8601
-- date range pattern for direct substitution.

-- ═══════════════════════════════════════════════════════════════
-- 1. HISTORICAL PHASE TRENDS
-- ═══════════════════════════════════════════════════════════════

-- 1a. Rolling p50/p95/p99 of engine-aggregate phase durations
--     for the last 7 days, bucketed by hour. Replace the date
--     range placeholders when running.
SELECT
    strftime('%Y-%m-%dT%H:00:00', a.updated_at) AS hour,
    COUNT(*)                                                AS attempts,
    -- Pipeline phases
    ROUND(AVG(m.pipeline_total_ms), 1)                      AS avg_pipeline_total_ms,
    ROUND(AVG(m.engine_segment_build_ms), 1)                AS avg_segment_build_ms,
    ROUND(AVG(m.engine_concat_ms), 1)                       AS avg_concat_ms,
    ROUND(AVG(m.engine_mux_audio_ms), 1)                    AS avg_mux_audio_ms,
    -- Total engine wall time (sum of engine_* columns)
    ROUND(AVG(m.engine_asset_download_ms
           + m.engine_segment_build_ms
           + m.engine_concat_ms
           + m.engine_audio_download_ms
           + m.engine_mux_audio_ms
           + m.engine_copy_final_ms), 1)                    AS avg_engine_total_ms
FROM task_attempt_metrics m
JOIN task_attempts a ON a.id = m.attempt_id
WHERE a.updated_at >= '2026-06-29T00:00:00Z'
  AND a.updated_at  < '2026-07-06T00:00:00Z'
  AND a.status = 'SUCCEEDED'
GROUP BY hour
ORDER BY hour ASC;

-- 1b. Daily p50/p95/p99 percentiles for engine phases over the
--     last 30 days. Useful for capacity planning and detecting
--     gradual degradation (e.g. growing asset_download_ms).
--     Each metric gets its own ranking so percentiles are correct.
WITH seg_ranked AS (
    SELECT
        date(a.updated_at)                                                AS day,
        m.engine_segment_build_ms                                         AS value,
        ROW_NUMBER() OVER (PARTITION BY date(a.updated_at) ORDER BY m.engine_segment_build_ms) AS rn,
        COUNT(*)      OVER (PARTITION BY date(a.updated_at))              AS cnt
    FROM task_attempt_metrics m
    JOIN task_attempts a ON a.id = m.attempt_id
    WHERE a.updated_at >= date('now', '-30 days')
      AND a.status = 'SUCCEEDED'
      AND m.engine_segment_build_ms > 0
),
concat_ranked AS (
    SELECT
        date(a.updated_at)                                                AS day,
        m.engine_concat_ms                                                AS value,
        ROW_NUMBER() OVER (PARTITION BY date(a.updated_at) ORDER BY m.engine_concat_ms) AS rn,
        COUNT(*)      OVER (PARTITION BY date(a.updated_at))              AS cnt
    FROM task_attempt_metrics m
    JOIN task_attempts a ON a.id = m.attempt_id
    WHERE a.updated_at >= date('now', '-30 days')
      AND a.status = 'SUCCEEDED'
      AND m.engine_concat_ms > 0
),
seg_pct AS (
    SELECT day, MAX(cnt) AS attempts,
        MAX(CASE WHEN rn = CAST(cnt * 0.50 AS INT) THEN value END) AS p50,
        MAX(CASE WHEN rn = CAST(cnt * 0.95 AS INT) THEN value END) AS p95,
        MAX(CASE WHEN rn = CAST(cnt * 0.99 AS INT) THEN value END) AS p99
    FROM seg_ranked GROUP BY day
),
concat_pct AS (
    SELECT day,
        MAX(CASE WHEN rn = CAST(cnt * 0.50 AS INT) THEN value END) AS p50,
        MAX(CASE WHEN rn = CAST(cnt * 0.95 AS INT) THEN value END) AS p95,
        MAX(CASE WHEN rn = CAST(cnt * 0.99 AS INT) THEN value END) AS p99
    FROM concat_ranked GROUP BY day
)
SELECT
    COALESCE(s.day, c.day)                                     AS day,
    COALESCE(s.attempts, 0)                                    AS attempts,
    s.p50                                                      AS p50_seg_build_ms,
    s.p95                                                      AS p95_seg_build_ms,
    s.p99                                                      AS p99_seg_build_ms,
    c.p50                                                      AS p50_concat_ms,
    c.p95                                                      AS p95_concat_ms,
    c.p99                                                      AS p99_concat_ms
FROM seg_pct s
FULL OUTER JOIN concat_pct c ON c.day = s.day
ORDER BY day DESC;

-- 1c. Phase timing breakdown by phase name (detailed table)
--     for the last 24h, aggregated across all workers.
SELECT
    pt.component || '.' || pt.action                        AS phase_name,
    COUNT(*)                                                AS observations,
    ROUND(AVG(pt.duration_ms), 1)                           AS avg_ms,
    ROUND(MIN(pt.duration_ms), 1)                           AS min_ms,
    ROUND(MAX(pt.duration_ms), 1)                           AS max_ms
FROM task_phase_timings pt
JOIN task_attempts a ON a.id = pt.attempt_id
WHERE a.updated_at >= datetime('now', '-24 hours')
  AND a.status = 'SUCCEEDED'
  AND pt.component != ''
GROUP BY phase_name
ORDER BY avg_ms DESC;

-- ═══════════════════════════════════════════════════════════════
-- 2. SLOWEST WORKERS
-- ═══════════════════════════════════════════════════════════════

-- 2a. Top 10 slowest workers by p95 engine total time in the
--     last 7 days. Workers with fewer than 10 attempts are
--     excluded to avoid one-off noise.
SELECT
    w.worker_id,
    COUNT(*)                                                AS attempts,
    ROUND(AVG(m.engine_asset_download_ms
           + m.engine_segment_build_ms
           + m.engine_concat_ms
           + m.engine_audio_download_ms
           + m.engine_mux_audio_ms
           + m.engine_copy_final_ms), 0)                    AS avg_engine_total_ms,
    ROUND(AVG(m.engine_segment_build_ms), 0)                AS avg_seg_build_ms,
    ROUND(AVG(m.engine_concat_ms), 0)                       AS avg_concat_ms
FROM task_attempt_metrics m
JOIN task_attempts a ON a.id = m.attempt_id
JOIN workers w ON w.worker_id = a.worker_id
WHERE a.updated_at >= datetime('now', '-7 days')
  AND a.status = 'SUCCEEDED'
  AND m.engine_segment_build_ms > 0
GROUP BY w.worker_id
HAVING COUNT(*) >= 10
ORDER BY avg_engine_total_ms DESC
LIMIT 10;

-- 2b. Per-worker phase breakdown for the single slowest worker
--     (substitute the worker_id from 2a above).
SELECT
    pt.component || '.' || pt.action                        AS phase_name,
    COUNT(*)                                                AS observations,
    ROUND(AVG(pt.duration_ms), 1)                           AS avg_ms,
    ROUND(MAX(pt.duration_ms), 1)                           AS max_ms
FROM task_phase_timings pt
JOIN task_attempts a ON a.id = pt.attempt_id
WHERE a.updated_at >= datetime('now', '-7 days')
  AND a.status = 'SUCCEEDED'
  AND a.worker_id = '<WORKER_ID_FROM_2A>'
  AND pt.component != ''
GROUP BY phase_name
ORDER BY avg_ms DESC;

-- 2c. Worker ranking by p99 segment duration (captures tail
--     latency outliers that avg masks).
SELECT
    a.worker_id,
    COUNT(*)                                                AS segments,
    ROUND(AVG(s.duration_ms), 1)                            AS avg_seg_ms,
    ROUND(MAX(s.duration_ms), 1)                            AS max_seg_ms,
    ROUND(AVG(s.asset_download_ms), 1)                      AS avg_dl_ms,
    ROUND(AVG(s.ffmpeg_encode_ms), 1)                       AS avg_encode_ms
FROM task_attempt_segment_timings s
JOIN task_attempts a ON a.id = s.attempt_id
WHERE s.created_at >= datetime('now', '-7 days')
  AND a.status = 'SUCCEEDED'
  AND s.duration_ms > 0
GROUP BY a.worker_id
HAVING COUNT(*) >= 50
ORDER BY max_seg_ms DESC
LIMIT 10;

-- ═══════════════════════════════════════════════════════════════
-- 3. STRAGGLER ANALYSIS
-- ═══════════════════════════════════════════════════════════════

-- 3a. Straggler segment detection: segments whose duration
--     exceeds the per-source_type p50 by more than 5× (the
--     classic straggler multiplier). Run against the last 24h.
WITH baseline AS (
    SELECT
        s.source_type,
        AVG(s.duration_ms)                                  AS avg_ms
    FROM task_attempt_segment_timings s
    JOIN task_attempts a ON a.id = s.attempt_id
    WHERE s.created_at >= datetime('now', '-24 hours')
      AND a.status = 'SUCCEEDED'
      AND s.duration_ms > 0
    GROUP BY s.source_type
)
SELECT
    s.attempt_id,
    s.worker_id,
    s.segment_index,
    s.source_type,
    ROUND(s.duration_ms, 1)                                 AS segment_ms,
    ROUND(b.avg_ms, 1)                                      AS source_avg_ms,
    ROUND(s.duration_ms / NULLIF(b.avg_ms, 0), 1)           AS x_avg,
    ROUND(s.asset_download_ms, 1)                           AS dl_ms,
    ROUND(s.ffmpeg_encode_ms, 1)                            AS encode_ms
FROM task_attempt_segment_timings s
JOIN baseline b ON b.source_type = s.source_type
JOIN task_attempts a ON a.id = s.attempt_id
WHERE s.created_at >= datetime('now', '-24 hours')
  AND a.status = 'SUCCEEDED'
  AND s.duration_ms > 5 * b.avg_ms
  AND b.avg_ms > 0
ORDER BY s.duration_ms DESC
LIMIT 50;

-- 3b. Straggler worker aggregation: workers with the highest
--     straggler-segment count in the last 7 days (normalized
--     by total segments so high-throughput workers don't
--     dominate).
WITH stragglers AS (
    SELECT
        s.worker_id,
        s.attempt_id,
        s.segment_index,
        s.duration_ms,
        s.source_type,
        AVG(s.duration_ms) OVER (PARTITION BY s.source_type) AS source_avg
    FROM task_attempt_segment_timings s
    JOIN task_attempts a ON a.id = s.attempt_id
    WHERE s.created_at >= datetime('now', '-7 days')
      AND a.status = 'SUCCEEDED'
      AND s.duration_ms > 0
)
SELECT
    worker_id,
    COUNT(*)                                                          AS total_segments,
    SUM(CASE WHEN duration_ms > 5 * source_avg THEN 1 ELSE 0 END)    AS straggler_segments,
    ROUND(100.0 * SUM(CASE WHEN duration_ms > 5 * source_avg THEN 1 ELSE 0 END) / COUNT(*), 2) AS straggler_pct,
    ROUND(AVG(duration_ms), 1)                                        AS avg_seg_ms,
    ROUND(MAX(duration_ms), 1)                                        AS max_seg_ms
FROM stragglers
GROUP BY worker_id
HAVING COUNT(*) >= 50
ORDER BY straggler_pct DESC
LIMIT 10;

-- 3c. Per-source-type straggler distribution: which source
--     types produce the most outliers. Helps pinpoint whether
--     stragglers are concentrated in, e.g., clip downloads or
--     color segment builds.
WITH baseline AS (
    SELECT
        s.source_type,
        AVG(s.duration_ms)                                  AS avg_ms
    FROM task_attempt_segment_timings s
    JOIN task_attempts a ON a.id = s.attempt_id
    WHERE s.created_at >= datetime('now', '-7 days')
      AND a.status = 'SUCCEEDED'
      AND s.duration_ms > 0
    GROUP BY s.source_type
)
SELECT
    s.source_type,
    COUNT(*)                                                AS total,
    SUM(CASE WHEN s.duration_ms > 5 * b.avg_ms THEN 1 ELSE 0 END) AS stragglers,
    ROUND(100.0 * SUM(CASE WHEN s.duration_ms > 5 * b.avg_ms THEN 1 ELSE 0 END) / COUNT(*), 2) AS straggler_pct,
    ROUND(AVG(s.duration_ms), 1)                            AS avg_ms,
    ROUND(b.avg_ms, 1)                                      AS baseline_avg_ms
FROM task_attempt_segment_timings s
JOIN baseline b ON b.source_type = s.source_type
JOIN task_attempts a ON a.id = s.attempt_id
WHERE s.created_at >= datetime('now', '-7 days')
  AND a.status = 'SUCCEEDED'
  AND s.duration_ms > 0
GROUP BY s.source_type
ORDER BY straggler_pct DESC;
