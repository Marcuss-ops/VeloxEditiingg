-- 086_task_attempt_reports_version.sql
--
-- Step 18 / Scorecard v2: the worker now emits a PerformanceReport with
-- explicit report_version and report_schema_version. Persist report_version
-- alongside the raw report so re-emissions of the same attempt can be
-- distinguished while remaining idempotent on report_hash.
--
-- Note: SQLite does not support IF NOT EXISTS on ADD COLUMN. The migration
-- runner tracks applied migrations and applies each file exactly once, so
-- this statement is safe.

ALTER TABLE task_attempt_reports ADD COLUMN report_version INTEGER NOT NULL DEFAULT 1;
