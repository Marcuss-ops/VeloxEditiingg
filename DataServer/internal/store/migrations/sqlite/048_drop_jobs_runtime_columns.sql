-- 048_drop_jobs_runtime_columns
-- PR #8 / Ondata: progressive removal of runtime fields from the jobs table.
-- Since task-native dispatch (PR #4) is fully implemented and tasks carry
-- worker_id, lease_id, attempt_number, etc., the jobs table no longer needs
-- these per-execution columns. This migration drops them.
--
-- Columns dropped:
--   assigned_to     → worker assignment is on tasks.worker_id
--   claimed_by      → claim identity is on tasks
--   lease_id        → lease is on tasks.lease_id
--   lease_expiry    → lease TTL is on tasks
--   retry_count     → attempt tracking is on tasks.attempt_count
--
-- Related columns NOT dropped (separate concerns):
--   assigned_at, claimed_at → timestamp columns (separate migration)
--   worker_name, attempt    → legacy metadata (separate migration)

ALTER TABLE jobs DROP COLUMN assigned_to;
ALTER TABLE jobs DROP COLUMN claimed_by;
ALTER TABLE jobs DROP COLUMN lease_id;
ALTER TABLE jobs DROP COLUMN lease_expiry;
ALTER TABLE jobs DROP COLUMN retry_count;
