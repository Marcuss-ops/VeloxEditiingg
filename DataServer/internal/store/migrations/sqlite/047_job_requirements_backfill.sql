-- 047_job_requirements_backfill.sql
--
-- PR #6 (refactor/requirements-ssot): backfill the remaining 3 requirements
-- columns so the full costmodel.JobRequirements (ResourceClass, TemporalMode,
-- Deterministic, Cacheable, MinBandwidthMbps) lives in dedicated columns.
-- Eliminates the _requirements JSON sub-object from request_json/result_json.
-- The first 2 columns were added by migration 045; this adds the remaining 3.

ALTER TABLE jobs ADD COLUMN job_required_deterministic   INTEGER NOT NULL DEFAULT 0;
ALTER TABLE jobs ADD COLUMN job_required_cacheable       INTEGER NOT NULL DEFAULT 0;
ALTER TABLE jobs ADD COLUMN job_required_min_bandwidth_mbps REAL NOT NULL DEFAULT 0.0;
