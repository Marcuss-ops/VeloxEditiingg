-- Migration 023: Add missing columns to the jobs table
--
-- The jobColumns constant in store_jobs_query.go references 45 columns,
-- but the jobs table schema created by migration 001 only has 12 columns,
-- plus revision (015), request_json (017), and result_json (017).
-- The remaining ~31 columns were never added via ALTER TABLE.
--
-- This migration adds all the columns that store_jobs_query.go expects.
-- All new columns use IF NOT EXISTS (requires SQLite >= 3.35) so this
-- migration is idempotent on re-run.

ALTER TABLE jobs ADD COLUMN started_at              TEXT;
ALTER TABLE jobs ADD COLUMN assigned_at             TEXT;
ALTER TABLE jobs ADD COLUMN worker_name             TEXT;
ALTER TABLE jobs ADD COLUMN claimed_by              TEXT;
ALTER TABLE jobs ADD COLUMN claimed_at              TEXT;
ALTER TABLE jobs ADD COLUMN lease_id                TEXT;
ALTER TABLE jobs ADD COLUMN lease_expiry            TEXT;
ALTER TABLE jobs ADD COLUMN attempt                 INTEGER NOT NULL DEFAULT 0;
ALTER TABLE jobs ADD COLUMN max_retries             INTEGER NOT NULL DEFAULT 3;
ALTER TABLE jobs ADD COLUMN last_error_at           TEXT;
ALTER TABLE jobs ADD COLUMN error_message           TEXT;
ALTER TABLE jobs ADD COLUMN failed_at               TEXT;
ALTER TABLE jobs ADD COLUMN failed_by               TEXT;
ALTER TABLE jobs ADD COLUMN processing_at           TEXT;
ALTER TABLE jobs ADD COLUMN video_uploaded          INTEGER NOT NULL DEFAULT 0;
ALTER TABLE jobs ADD COLUMN master_video_path       TEXT;
ALTER TABLE jobs ADD COLUMN last_upload_result      TEXT;
ALTER TABLE jobs ADD COLUMN last_upload_attempt_at  TEXT;
ALTER TABLE jobs ADD COLUMN last_drive_upload_result TEXT;
ALTER TABLE jobs ADD COLUMN remote_status           TEXT;
ALTER TABLE jobs ADD COLUMN artifact_id             TEXT;
ALTER TABLE jobs ADD COLUMN output_sha256           TEXT;
ALTER TABLE jobs ADD COLUMN upload_idempotency_key  TEXT;
ALTER TABLE jobs ADD COLUMN output_video_id         TEXT;
ALTER TABLE jobs ADD COLUMN drive_url               TEXT;
ALTER TABLE jobs ADD COLUMN job_fingerprint         TEXT;
ALTER TABLE jobs ADD COLUMN submitted_via           TEXT;
ALTER TABLE jobs ADD COLUMN last_activity           TEXT;
ALTER TABLE jobs ADD COLUMN run_id                  TEXT;
ALTER TABLE jobs ADD COLUMN job_run_id              TEXT;
ALTER TABLE jobs ADD COLUMN logs_updated_at         TEXT;
ALTER TABLE jobs ADD COLUMN slot_data               TEXT;
