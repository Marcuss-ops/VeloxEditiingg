-- 075_input_context.sql
--
-- Step 12 / Scorecard v2: input context metrics on
-- task_attempt_metrics. Captures the shape of the render job so
-- operators can normalize performance across different input
-- complexities (e.g. 5 scenes vs 80 scenes).
--
-- Columns:
--   scene_count              — number of timeline scenes
--   segment_count            — total segments across all scenes
--   total_input_duration_sec — sum of input media durations
--   resolution_width         — output width in pixels
--   resolution_height        — output height in pixels
--   fps                      — output frames per second
--   audio_track_count        — number of audio tracks
--   subtitle_count           — number of subtitle tracks
--   template_id              — rendering template identifier
--
-- All DEFAULT 0 / 0.0 / '' so older workers that don't emit
-- these fields continue to function without a code change.

ALTER TABLE task_attempt_metrics
    ADD COLUMN scene_count               INTEGER NOT NULL DEFAULT 0;
ALTER TABLE task_attempt_metrics
    ADD COLUMN segment_count             INTEGER NOT NULL DEFAULT 0;
ALTER TABLE task_attempt_metrics
    ADD COLUMN total_input_duration_sec  REAL    NOT NULL DEFAULT 0.0;
ALTER TABLE task_attempt_metrics
    ADD COLUMN resolution_width          INTEGER NOT NULL DEFAULT 0;
ALTER TABLE task_attempt_metrics
    ADD COLUMN resolution_height         INTEGER NOT NULL DEFAULT 0;
ALTER TABLE task_attempt_metrics
    ADD COLUMN fps                       REAL    NOT NULL DEFAULT 0.0;
ALTER TABLE task_attempt_metrics
    ADD COLUMN audio_track_count         INTEGER NOT NULL DEFAULT 0;
ALTER TABLE task_attempt_metrics
    ADD COLUMN subtitle_count            INTEGER NOT NULL DEFAULT 0;
ALTER TABLE task_attempt_metrics
    ADD COLUMN template_id               TEXT    NOT NULL DEFAULT '';
