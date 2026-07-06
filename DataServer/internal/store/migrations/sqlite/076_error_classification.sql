-- 076_error_classification.sql
--
-- Step 13 / Scorecard v2: error classification refinement on
-- task_attempt_metrics. Captures structured error metadata so
-- operators can group failures by component and phase rather
-- than parsing free-form error strings.
--
-- Columns:
--   error_component  — which component failed (engine, pipeline, etc.)
--   error_phase      — which phase was running when the error occurred
--   error_retryable  — 1 if the error is retryable, 0 otherwise
--   error_message_hash — SHA-256 of the error message for dedup grouping
--
-- All DEFAULT '' / 0 so older workers that don't emit these fields
-- continue to function without a code change.

ALTER TABLE task_attempt_metrics
    ADD COLUMN error_component    TEXT    NOT NULL DEFAULT '';
ALTER TABLE task_attempt_metrics
    ADD COLUMN error_phase        TEXT    NOT NULL DEFAULT '';
ALTER TABLE task_attempt_metrics
    ADD COLUMN error_retryable    INTEGER NOT NULL DEFAULT 0;
ALTER TABLE task_attempt_metrics
    ADD COLUMN error_message_hash TEXT    NOT NULL DEFAULT '';
