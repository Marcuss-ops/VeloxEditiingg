-- 087_failed_partial_metrics.sql
--
-- Step 18 / Scorecard v2: partial-progress counters for FAILED attempts.
-- completed_segments reports how many segments finished before the
-- attempt stopped, so operators can distinguish "failed immediately"
-- from "failed after rendering most of the video".
--
-- All existing rows default to 0 (no partial progress recorded).

ALTER TABLE task_attempt_metrics
    ADD COLUMN completed_segments INTEGER NOT NULL DEFAULT 0;
