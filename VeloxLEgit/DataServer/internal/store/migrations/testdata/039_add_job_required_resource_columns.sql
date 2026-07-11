-- 039_add_job_required_resource_columns.sql
--
-- PR-04.5: Adds two dedicated columns to the jobs table that mirror
-- `request_json._requirements.resource_class` and
-- `request_json._requirements.temporal_mode`. We persist these on
-- columns so that:
--
--   * Future query-time filtering (ClaimNext / ListByStatus) can filter
--     by resource class without parsing the request_json blob.
--   * The cost-model eligibility layer (PR-04.4) reads Requirements off
--     the column without re-reading request_json.
--
-- Deterministic + Cacheable continue to live JSON-only inside
-- request_json because the rank layer (PR-04.6+) does not need
-- query-time filtering on those flags today, and the dedicated columns
-- would balloon the schema without ROI at this slice.
--
-- Mirroring (column ↔ JSON) is enforced at the repository Create layer
-- (see internal/store/sqlite_jobs_writer.go::CreateJob). The existing
-- pre-PR-04.5 rows have DEFAULT '' for both columns, which the
-- reconstruction path in toJobsJob folds into JobRequirements{} = the
-- permissive default (matches GetSchedulableWorkers behavior pre-PR-04.5).
--
-- SQLite >= 3.35.0 required for ALTER TABLE ADD COLUMN (with DEFAULT).

ALTER TABLE jobs ADD COLUMN job_required_resource_class TEXT NOT NULL DEFAULT '';

ALTER TABLE jobs ADD COLUMN job_required_temporal_mode  TEXT NOT NULL DEFAULT '';
