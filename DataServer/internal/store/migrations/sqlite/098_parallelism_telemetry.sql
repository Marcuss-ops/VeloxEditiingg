-- 098: Parallelism telemetry — extend segment timings with wall-clock
-- offsets and persist derived parallelism aggregates.
--
-- New columns on task_attempt_segment_timings:
--   started_offset_ms   monotonic offset from attempt start to segment start
--   finished_offset_ms  monotonic offset from attempt start to segment end
--   worker_slot         which concurrency slot executed this segment
--   cpu_threads         FFmpeg threads allocated to this segment
--   parallel_group      logical grouping (e.g. "scene_0", "concat", "mux")
--
-- New table task_attempt_parallelism stores derived aggregates computed
-- by the master during IngestTaskResultAtomic from the segment rows.
--
-- New table render_parallelism_profiles stores per-profile best-known
-- configuration for the dynamic resolver.

-- ── Extend task_attempt_segment_timings ─────────────────────────────

ALTER TABLE task_attempt_segment_timings
ADD COLUMN started_offset_ms REAL NOT NULL DEFAULT 0;

ALTER TABLE task_attempt_segment_timings
ADD COLUMN finished_offset_ms REAL NOT NULL DEFAULT 0;

ALTER TABLE task_attempt_segment_timings
ADD COLUMN worker_slot INTEGER NOT NULL DEFAULT 0;

ALTER TABLE task_attempt_segment_timings
ADD COLUMN cpu_threads INTEGER NOT NULL DEFAULT 0;

ALTER TABLE task_attempt_segment_timings
ADD COLUMN parallel_group TEXT NOT NULL DEFAULT '';

-- ── Parallelism aggregates (computed by master) ────────────────────

CREATE TABLE IF NOT EXISTS task_attempt_parallelism (
    attempt_id                  TEXT PRIMARY KEY,

    configured_segment_workers  INTEGER NOT NULL DEFAULT 1,
    ffmpeg_threads_per_segment  INTEGER NOT NULL DEFAULT 1,
    logical_cpu_count           INTEGER NOT NULL DEFAULT 0,
    cpu_budget                  INTEGER NOT NULL DEFAULT 0,

    serial_work_ms              REAL    NOT NULL DEFAULT 0,
    render_window_ms            REAL    NOT NULL DEFAULT 0,
    union_busy_ms               REAL    NOT NULL DEFAULT 0,
    overlap_ms                  REAL    NOT NULL DEFAULT 0,
    idle_gap_ms                 REAL    NOT NULL DEFAULT 0,

    peak_concurrency            INTEGER NOT NULL DEFAULT 1,
    average_concurrency         REAL    NOT NULL DEFAULT 1,
    speedup_vs_serial           REAL    NOT NULL DEFAULT 1,
    parallel_efficiency_ratio   REAL    NOT NULL DEFAULT 1,
    cpu_oversubscription_ratio  REAL    NOT NULL DEFAULT 0,

    bottleneck_phase            TEXT    NOT NULL DEFAULT '',
    parallel_strategy           TEXT    NOT NULL DEFAULT '',
    calculated_at               TEXT    NOT NULL DEFAULT ''
);

-- ── Per-profile best-known configuration ───────────────────────────

CREATE TABLE IF NOT EXISTS render_parallelism_profiles (
    profile_key                 TEXT    PRIMARY KEY,

    resolution_width            INTEGER NOT NULL DEFAULT 0,
    resolution_height           INTEGER NOT NULL DEFAULT 0,
    fps                         REAL    NOT NULL DEFAULT 0,
    codec                       TEXT    NOT NULL DEFAULT '',
    scene_count_bucket          TEXT    NOT NULL DEFAULT '',

    segment_workers             INTEGER NOT NULL DEFAULT 1,
    ffmpeg_threads_per_segment  INTEGER NOT NULL DEFAULT 1,

    sample_count                INTEGER NOT NULL DEFAULT 0,
    average_wall_clock_ms       REAL    NOT NULL DEFAULT 0,
    best_wall_clock_ms          REAL    NOT NULL DEFAULT 0,
    average_efficiency_ratio    REAL    NOT NULL DEFAULT 1,

    updated_at                  TEXT    NOT NULL DEFAULT ''
);
