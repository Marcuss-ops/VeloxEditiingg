-- Migration 135: preserve the editorial scene identity on each timeline
-- segment. A scene can generate multiple visual timeline items.

ALTER TABLE task_attempt_segment_timings
    ADD COLUMN scene_id TEXT NOT NULL DEFAULT '';
