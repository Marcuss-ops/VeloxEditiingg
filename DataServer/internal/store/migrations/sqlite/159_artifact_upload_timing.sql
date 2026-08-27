-- 159_artifact_upload_timing.sql
-- Master-side timing facts for the artifact data plane.
-- Some legacy validation-only databases carry migration markers without the
-- artifact upload tables. Establish the historical base shape here so the
-- forward ALTERs remain bootstrap-safe; production databases already have
-- this table from migration 030 and the CREATE is a no-op.
CREATE TABLE IF NOT EXISTS artifact_uploads (
    upload_id TEXT PRIMARY KEY,
    artifact_id TEXT NOT NULL,
    job_id TEXT NOT NULL,
    attempt_number INTEGER NOT NULL,
    worker_id TEXT NOT NULL,
    lease_id TEXT NOT NULL,
    status TEXT NOT NULL,
    temporary_storage_key TEXT NOT NULL,
    expected_size_bytes INTEGER,
    expected_sha256 TEXT,
    received_size_bytes INTEGER,
    received_sha256 TEXT,
    created_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    completed_at TEXT
);
ALTER TABLE artifact_uploads ADD COLUMN first_byte_received_at TEXT;
ALTER TABLE artifact_uploads ADD COLUMN last_byte_received_at TEXT;
ALTER TABLE artifact_uploads ADD COLUMN verify_started_at TEXT;
ALTER TABLE artifact_uploads ADD COLUMN verify_completed_at TEXT;
ALTER TABLE artifact_uploads ADD COLUMN promote_started_at TEXT;
ALTER TABLE artifact_uploads ADD COLUMN promote_completed_at TEXT;
ALTER TABLE artifact_uploads ADD COLUMN commit_started_at TEXT;
ALTER TABLE artifact_uploads ADD COLUMN commit_completed_at TEXT;
