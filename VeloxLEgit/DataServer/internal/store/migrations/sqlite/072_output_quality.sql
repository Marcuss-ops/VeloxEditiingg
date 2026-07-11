-- 072_output_quality.sql
--
-- Step 9 / Scorecard v2: output quality validation columns on
-- task_attempt_metrics. Every attempt records automated quality
-- checks so operators can detect corrupted outputs without
-- manually inspecting each video.
--
-- Columns:
--   ffprobe_valid        — 1 if ffprobe successfully parsed the output
--   duration_diff_sec    — abs(actual - expected) output duration diff
--   has_video_stream     — 1 if output contains a video stream
--   has_audio_stream     — 1 if output contains an audio stream
--   output_file_size     — final output file size in bytes
--   black_frame_ratio    — fraction of black frames detected [0.0, 1.0]
--   audio_sync_offset_ms — max audio/video sync offset in milliseconds
--
-- All DEFAULT 0 / 0.0 so older workers that don't emit these fields
-- continue to function without a code change.

ALTER TABLE task_attempt_metrics
    ADD COLUMN ffprobe_valid        INTEGER NOT NULL DEFAULT 0;
ALTER TABLE task_attempt_metrics
    ADD COLUMN duration_diff_sec    REAL    NOT NULL DEFAULT 0.0;
ALTER TABLE task_attempt_metrics
    ADD COLUMN has_video_stream     INTEGER NOT NULL DEFAULT 0;
ALTER TABLE task_attempt_metrics
    ADD COLUMN has_audio_stream     INTEGER NOT NULL DEFAULT 0;
ALTER TABLE task_attempt_metrics
    ADD COLUMN output_file_size     INTEGER NOT NULL DEFAULT 0;
ALTER TABLE task_attempt_metrics
    ADD COLUMN black_frame_ratio    REAL    NOT NULL DEFAULT 0.0;
ALTER TABLE task_attempt_metrics
    ADD COLUMN audio_sync_offset_ms INTEGER NOT NULL DEFAULT 0;
