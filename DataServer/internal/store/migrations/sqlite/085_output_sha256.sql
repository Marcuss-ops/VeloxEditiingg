-- 085_output_sha256.sql
--
-- Step 18 / Scorecard v2: output SHA-256 on task_attempt_metrics.
-- Captures the canonical output content hash for quick quality/
-- deterministic checks without joining task_output_artifacts.
--
-- All DEFAULT '' so older workers that don't emit this field
-- continue to function without a code change.

ALTER TABLE task_attempt_metrics
    ADD COLUMN output_sha256 TEXT NOT NULL DEFAULT '';
