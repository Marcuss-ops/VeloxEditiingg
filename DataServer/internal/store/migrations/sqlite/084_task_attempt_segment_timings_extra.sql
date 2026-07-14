-- 084_task_attempt_segment_timings_extra.sql
--
-- Completes the C++ segments → task_attempt_segment_timings linkage by
-- adding the per-segment identity and duration columns that the C++
-- sidecar now emits.

ALTER TABLE task_attempt_segment_timings
    ADD COLUMN source_url_hash TEXT NOT NULL DEFAULT '';

ALTER TABLE task_attempt_segment_timings
    ADD COLUMN cache_key TEXT NOT NULL DEFAULT '';

ALTER TABLE task_attempt_segment_timings
    ADD COLUMN input_duration_ms REAL NOT NULL DEFAULT 0.0;

ALTER TABLE task_attempt_segment_timings
    ADD COLUMN output_duration_ms REAL NOT NULL DEFAULT 0.0;
