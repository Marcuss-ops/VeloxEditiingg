-- 070_engine_phase_metrics.sql
--
-- Step 6 / Scorecard v2: detailed engine phase + segment-level timings.
--
-- 1. Extends task_attempt_metrics with engine-aggregate counters the
--    Go worker sidecar reader surfaces as dotted-key entries
--    (pipeline.resolve_ms, engine.segment_build_ms, …).
-- 2. Extends task_phase_timings with richer per-phase metadata
--    (component, action, phase_order, status, bytes, frames).
-- 3. Creates task_attempt_segment_timings for per-segment C++ sidecar
--    records (index, source_type, download/encode ms, output_bytes).
--
-- All DEFAULTs are constant (0 / '' / 0.0) per SQLite ALTER TABLE rules.

-- ── 1. Engine-aggregate metrics columns ──────────────────────────
ALTER TABLE task_attempt_metrics
    ADD COLUMN pipeline_resolve_ms      INTEGER NOT NULL DEFAULT 0;
ALTER TABLE task_attempt_metrics
    ADD COLUMN pipeline_validate_ms     INTEGER NOT NULL DEFAULT 0;
ALTER TABLE task_attempt_metrics
    ADD COLUMN pipeline_compile_ms      INTEGER NOT NULL DEFAULT 0;
ALTER TABLE task_attempt_metrics
    ADD COLUMN pipeline_render_ms       INTEGER NOT NULL DEFAULT 0;
ALTER TABLE task_attempt_metrics
    ADD COLUMN pipeline_total_ms        INTEGER NOT NULL DEFAULT 0;
ALTER TABLE task_attempt_metrics
    ADD COLUMN native_total_ms          INTEGER NOT NULL DEFAULT 0;
ALTER TABLE task_attempt_metrics
    ADD COLUMN native_process_wait_ms   INTEGER NOT NULL DEFAULT 0;
ALTER TABLE task_attempt_metrics
    ADD COLUMN engine_asset_download_ms INTEGER NOT NULL DEFAULT 0;
ALTER TABLE task_attempt_metrics
    ADD COLUMN engine_segment_build_ms  INTEGER NOT NULL DEFAULT 0;
ALTER TABLE task_attempt_metrics
    ADD COLUMN engine_concat_ms         INTEGER NOT NULL DEFAULT 0;
ALTER TABLE task_attempt_metrics
    ADD COLUMN engine_audio_download_ms INTEGER NOT NULL DEFAULT 0;
ALTER TABLE task_attempt_metrics
    ADD COLUMN engine_mux_audio_ms      INTEGER NOT NULL DEFAULT 0;
ALTER TABLE task_attempt_metrics
    ADD COLUMN engine_copy_final_ms     INTEGER NOT NULL DEFAULT 0;

-- ── 2. Extended phase timings ────────────────────────────────────
--   Drop old unique index so we can widen the table (phase→action).
DROP INDEX IF EXISTS idx_task_phase_timings_attempt_phase;

ALTER TABLE task_phase_timings
    ADD COLUMN phase_order    INTEGER NOT NULL DEFAULT 0;
ALTER TABLE task_phase_timings
    ADD COLUMN component      TEXT    NOT NULL DEFAULT '';
ALTER TABLE task_phase_timings
    ADD COLUMN action         TEXT    NOT NULL DEFAULT '';
ALTER TABLE task_phase_timings
    ADD COLUMN status         TEXT    NOT NULL DEFAULT 'ok';
ALTER TABLE task_phase_timings
    ADD COLUMN error_code     TEXT    NOT NULL DEFAULT '';
ALTER TABLE task_phase_timings
    ADD COLUMN error_message  TEXT    NOT NULL DEFAULT '';
ALTER TABLE task_phase_timings
    ADD COLUMN bytes_in       INTEGER NOT NULL DEFAULT 0;
ALTER TABLE task_phase_timings
    ADD COLUMN bytes_out      INTEGER NOT NULL DEFAULT 0;
ALTER TABLE task_phase_timings
    ADD COLUMN frames         INTEGER NOT NULL DEFAULT 0;
ALTER TABLE task_phase_timings
    ADD COLUMN metadata_json  TEXT    NOT NULL DEFAULT '{}';

CREATE UNIQUE INDEX IF NOT EXISTS idx_task_phase_timings_attempt_action
    ON task_phase_timings(attempt_id, component, action);

CREATE INDEX IF NOT EXISTS idx_task_phase_timings_phase_name
    ON task_phase_timings(component, action, status);

-- ── 3. Per-segment timing records ────────────────────────────────
CREATE TABLE IF NOT EXISTS task_attempt_segment_timings (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    attempt_id          TEXT    NOT NULL,
    job_id              TEXT    NOT NULL DEFAULT '',
    task_id             TEXT    NOT NULL DEFAULT '',
    worker_id           TEXT    NOT NULL DEFAULT '',
    segment_index       INTEGER NOT NULL DEFAULT 0,
    scene_worker_index  INTEGER NOT NULL DEFAULT 0,
    source_type         TEXT    NOT NULL DEFAULT '',
    duration_ms         REAL    NOT NULL DEFAULT 0.0,
    asset_download_ms   REAL    NOT NULL DEFAULT 0.0,
    ffmpeg_encode_ms    REAL    NOT NULL DEFAULT 0.0,
    source_bytes        INTEGER NOT NULL DEFAULT 0,
    output_bytes        INTEGER NOT NULL DEFAULT 0,
    frames_encoded      INTEGER NOT NULL DEFAULT 0,
    codec               TEXT    NOT NULL DEFAULT '',
    preset              TEXT    NOT NULL DEFAULT '',
    ffmpeg_threads      INTEGER NOT NULL DEFAULT 0,
    status              TEXT    NOT NULL DEFAULT 'ok',
    error_code          TEXT    NOT NULL DEFAULT '',
    error_message       TEXT    NOT NULL DEFAULT '',
    metadata_json       TEXT    NOT NULL DEFAULT '{}',
    created_at          TEXT    NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_segment_attempt
    ON task_attempt_segment_timings(attempt_id, segment_index);

CREATE INDEX IF NOT EXISTS idx_segment_worker_time
    ON task_attempt_segment_timings(worker_id, created_at);

CREATE INDEX IF NOT EXISTS idx_segment_slow
    ON task_attempt_segment_timings(duration_ms DESC);
