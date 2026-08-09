-- Migration 024: allow deployment baselines without rollback provenance.
-- The empty provenance is represented by NULL; no digest is fabricated.

ALTER TABLE deployment_records
  ALTER COLUMN previous_digest DROP NOT NULL;
ALTER TABLE deployment_records
  DROP CONSTRAINT IF EXISTS deployment_records_previous_digest_check;
ALTER TABLE deployment_records
  ADD CONSTRAINT deployment_records_previous_digest_check
  CHECK (previous_digest IS NULL OR length(previous_digest) > 0);
