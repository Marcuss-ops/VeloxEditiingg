-- 108_drop_legacy_indexes_and_recover_columns.sql
--
-- Recovers from the silent DROP COLUMN failures in migrations 035
-- (`035_drop_legacy_delivery_bridge.sql`) and 048
-- (`048_drop_jobs_runtime_columns.sql`).
--
-- Background: SQLite refuses to drop a column that is referenced by
-- an index — the DROP COLUMN statement fails with
--   `error in index <idx_name> after drop column: no such column: <col>`
-- even when the column-drop itself is the desired operation. The Go
-- migrations runner (`migrations.apply.go`) tolerates DROP COLUMN +
-- `no such column` errors silently, so the column drops in 035 + 048
-- were **skipped** in production, leaving both the offending indexes
-- AND the legacy columns in the schema. This migration cleans up
-- both:
--
--   1. Drop the two indexes that block 035 + 048
--      (`idx_delivery_attempts_target`, `idx_jobs_assigned_to`).
--   2. Re-attempt the DROP COLUMN statements that 035 + 048 missed
--      (`delivery_attempts.delivery_target_id`, the 5 `jobs.*`
--      runtime columns).
--
-- Idempotency: every operation uses the safest available fallback
-- (`DROP INDEX IF EXISTS` / `IF EXISTS` via the migrations runner's
-- drop-column tolerance). A second application of this migration is
-- a no-op. No canonical-table semantics change.
--
-- Why we ship a forward-only migration rather than patching 035/048:
-- §1.6 of docs/SOCIAL_API_MIGRATION_RUNBOOK.md pins historical
-- `.sql` files by SHA-256 checksum. Editing 035 or 048 in place
-- would trigger migration-integrity failures on next boot for any
-- operator who already applied them. This 108 migration is the
-- checksum-clean forward-only closure.

-- 1. Drop the indexes that blocked 035 (`delivery_attempts` +
--    `idx_delivery_attempts_target`) and 048 (`jobs` +
--    `idx_jobs_assigned_to`).
DROP INDEX IF EXISTS idx_delivery_attempts_target;
DROP INDEX IF EXISTS idx_delivery_attempts_legacy_target;
DROP INDEX IF EXISTS idx_jobs_assigned_to;

-- 2. Re-attempt the column drops that 035/048 missed. Each is
--    tolerated as a no-op if the column is already gone (apply.go's
--    drop-column + no-such-column pass-through keeps the migration
--    forward-only idempotent).
ALTER TABLE delivery_attempts DROP COLUMN delivery_target_id;
ALTER TABLE jobs           DROP COLUMN assigned_to;
ALTER TABLE jobs           DROP COLUMN claimed_by;
ALTER TABLE jobs           DROP COLUMN lease_id;
ALTER TABLE jobs           DROP COLUMN lease_expiry;
ALTER TABLE jobs           DROP COLUMN retry_count;
