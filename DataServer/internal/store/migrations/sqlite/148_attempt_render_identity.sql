-- Determinism chain closure (Fase D follow-up): renderer_version +
-- artifact_sha256 stamped on task_attempts when the worker report arrives.
--
-- The full chain job→attempt→plan_version→plan_sha256→renderer_version→
-- artifact_sha256 is now reconstructable per attempt:
--   plan_version / plan_sha256 / render_plan_json  (migration 145; stamped
--     by the master RenderPlanCompiler at claim time)
--   renderer_version                               (worker engine version at
--     report time — currently equivalent to engine_version; kept as a
--     dedicated column so the renderer identity can diverge from the
--     worker engine version in the future)
--   artifact_sha256                                (worker-declared SHA of the
--     primary output artifact at report time; the master-computed
--     authoritative value lives on artifacts.sha256 after finalization)
--
-- Forward-only: plain ADD COLUMN with NOT NULL DEFAULT '' keeps every
-- existing row valid (empty means "not yet stamped by a report").
ALTER TABLE task_attempts ADD COLUMN renderer_version TEXT NOT NULL DEFAULT '';
ALTER TABLE task_attempts ADD COLUMN artifact_sha256 TEXT NOT NULL DEFAULT '';
