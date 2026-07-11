-- migrations/sqlite/063_task_specs_required_outputs.sql
--
-- Artifact Commit Protocol (Phase 1.3) — required_outputs lives on
-- the task_specs row.
--
-- Default '[]' (empty JSON array) is intentionally added as a NOT
-- NULL DEFAULT so that pre-Phase 2 specs round-trip byte-identically
-- through PRAGMA integrity_check. Higher-level specs that need
-- required outputs (Phase 2+) will be authored explicitly with
-- non-empty arrays via the creatorflow.RenderPlan code path.
--
-- Payload format convention:
--   [
--     {"kind":          "<string>",    -- e.g. "final_video"
--      "mime_type":     "<string>",    -- e.g. "video/mp4"
--      "min_count":     <int>,
--      "max_count":     <int>},
--     ...
--   ]
--
-- The schema treats the column as opaque TEXT; JSON validation is the
-- creatorflow responsibility (do not embed validation in SQL — SQLite
-- has no native JSON-array-shape enforcement and JSON1 is not loaded
-- by default).

ALTER TABLE task_specs
    ADD COLUMN required_outputs_json TEXT NOT NULL DEFAULT '[]';
