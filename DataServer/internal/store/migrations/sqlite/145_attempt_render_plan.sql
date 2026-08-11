-- 144_attempt_render_plan.sql
--
-- Compiled render plan columns on task_attempts (Fase D). The master
-- RenderPlanCompiler stamps plan_version + plan_sha256 + the canonical plan
-- JSON on the attempt at claim time so the determinism chain
-- job→attempt→plan_version→plan_sha256→renderer_version→artifact_sha256 is
-- fully reconstructable for any attempt.
--
-- Forward-only: plain ADD COLUMN with NOT NULL defaults keeps every existing
-- row valid (plan_version=0 means "no compiled plan yet").

ALTER TABLE task_attempts ADD COLUMN plan_version INTEGER NOT NULL DEFAULT 0;
ALTER TABLE task_attempts ADD COLUMN plan_sha256 TEXT NOT NULL DEFAULT '';
ALTER TABLE task_attempts ADD COLUMN render_plan_json TEXT NOT NULL DEFAULT '';
