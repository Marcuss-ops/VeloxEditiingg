-- 038_drop_jobs_raw_json.sql
--
-- Drops raw_json and last_upload_result from the jobs table.
-- raw_json was the original full-job blob from the legacy orchestrator;
-- all data is now in canonical columns (status, request_json, result_json, etc.).
-- last_upload_result was never populated by any active code path.
--
-- SQLite >= 3.35.0 required for ALTER TABLE DROP COLUMN.
-- Each DROP swallows "no such column" via the migration runner's tolerance.

ALTER TABLE jobs DROP COLUMN raw_json;

ALTER TABLE jobs DROP COLUMN last_upload_result;
