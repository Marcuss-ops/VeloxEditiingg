-- 034_add_expected_revision.sql
-- Add expected_revision to artifact_uploads so FinalizeVerified can CAS
-- the job revision correctly. BeginUpload writes it; GetUploadSession
-- reads it back; FinalizeVerified uses it in the jobs CAS WHERE clause.
ALTER TABLE artifact_uploads ADD COLUMN expected_revision INTEGER DEFAULT 0;
