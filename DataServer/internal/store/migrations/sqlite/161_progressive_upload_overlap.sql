-- 161_progressive_upload_overlap.sql
--
-- Progressive upload overlap metrics for the capacity scorecard.
-- Captures how much upload work was completed while the engine was
-- still rendering, enabling historical querying of mux→upload overlap.

ALTER TABLE task_attempt_metrics
ADD COLUMN progressive_overlap_first_part_ms INTEGER NOT NULL DEFAULT 0;

ALTER TABLE task_attempt_metrics
ADD COLUMN progressive_overlap_parts_before_render INTEGER NOT NULL DEFAULT 0;

ALTER TABLE task_attempt_metrics
ADD COLUMN progressive_overlap_bytes_before_render INTEGER NOT NULL DEFAULT 0;

ALTER TABLE task_attempt_metrics
ADD COLUMN progressive_overlap_ms INTEGER NOT NULL DEFAULT 0;

ALTER TABLE task_attempt_metrics
ADD COLUMN trailer_to_open_ms INTEGER NOT NULL DEFAULT 0;

ALTER TABLE task_attempt_metrics
ADD COLUMN mux_to_open_us INTEGER NOT NULL DEFAULT 0;
