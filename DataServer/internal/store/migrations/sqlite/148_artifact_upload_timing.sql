-- 148_artifact_upload_timing.sql
-- Master-side timing facts for the artifact data plane.
ALTER TABLE artifact_uploads ADD COLUMN first_byte_received_at TEXT;
ALTER TABLE artifact_uploads ADD COLUMN last_byte_received_at TEXT;
ALTER TABLE artifact_uploads ADD COLUMN verify_started_at TEXT;
ALTER TABLE artifact_uploads ADD COLUMN verify_completed_at TEXT;
ALTER TABLE artifact_uploads ADD COLUMN promote_started_at TEXT;
ALTER TABLE artifact_uploads ADD COLUMN promote_completed_at TEXT;
ALTER TABLE artifact_uploads ADD COLUMN commit_started_at TEXT;
ALTER TABLE artifact_uploads ADD COLUMN commit_completed_at TEXT;
