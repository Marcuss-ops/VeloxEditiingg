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

ALTER TABLE jobs DROP COLUMN master_video_path;
ALTER TABLE jobs DROP COLUMN drive_url;
ALTER TABLE jobs DROP COLUMN output_video_id;
ALTER TABLE jobs DROP COLUMN artifact_id;
ALTER TABLE jobs DROP COLUMN output_sha256;
ALTER TABLE jobs DROP COLUMN upload_idempotency_key;
ALTER TABLE jobs DROP COLUMN video_uploaded;
