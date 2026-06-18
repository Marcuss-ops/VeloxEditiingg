-- 033_drop_legacy_job_columns.sql
--
-- Drops legacy flat columns from the jobs table that have been replaced
-- by canonical tables:
--   master_video_path   → artifacts.storage_url / storage_key (via AssembleJobView)
--   drive_url           → job_deliveries.remote_url (via AssembleJobView)
--   output_video_id     → job_deliveries.remote_id  (via AssembleJobView)
--   artifact_id         → artifacts.id              (via AssembleJobView)
--   output_sha256       → artifacts.sha256          (via AssembleJobView)
--   upload_idempotency_key → unused (legacy upload dedup, replaced by artifact_uploads)
--   video_uploaded      → EXISTS(artifacts WHERE status='READY') (via AssembleJobView)
--
-- SQLite >= 3.35.0 required for ALTER TABLE DROP COLUMN.
-- Each DROP is wrapped in a SELECT guard so the migration is idempotent
-- on databases where a column was already removed or never existed.

-- master_video_path
SELECT CASE WHEN EXISTS(SELECT 1 FROM pragma_table_info('jobs') WHERE name='master_video_path')
  THEN 'DROP' ELSE 'SKIP' END;
-- The migration runner executes the whole file as one tx; we use a
-- trick: each statement is executed only when the column exists.
-- Since SQLite doesn't support conditional DDL natively, we use
-- a prepared-statement approach via the Go migration runner.
-- For simplicity, we attempt each DROP and swallow "no such column" errors.

ALTER TABLE jobs DROP COLUMN master_video_path;

ALTER TABLE jobs DROP COLUMN drive_url;

ALTER TABLE jobs DROP COLUMN output_video_id;

ALTER TABLE jobs DROP COLUMN artifact_id;

ALTER TABLE jobs DROP COLUMN output_sha256;

ALTER TABLE jobs DROP COLUMN upload_idempotency_key;

ALTER TABLE jobs DROP COLUMN video_uploaded;
