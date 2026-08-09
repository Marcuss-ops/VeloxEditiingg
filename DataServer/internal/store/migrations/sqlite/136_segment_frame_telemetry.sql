-- 136_segment_frame_telemetry.sql
-- Preserve the per-scene frame and FFmpeg speed values emitted by the
-- worker sidecar so the Master job inspection surface can expose them.

ALTER TABLE task_attempt_segment_timings
    ADD COLUMN frames_decoded INTEGER NOT NULL DEFAULT 0;

ALTER TABLE task_attempt_segment_timings
    ADD COLUMN frames_composited INTEGER NOT NULL DEFAULT 0;

ALTER TABLE task_attempt_segment_timings
    ADD COLUMN ffmpeg_speed_x REAL NOT NULL DEFAULT 0.0;
