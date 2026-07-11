-- 035_drop_legacy_delivery_bridge.sql
-- Removes the legacy delivery_targets bridge columns that connected the old
-- delivery_targets table to the new job_deliveries model.
--
-- Prerequisites:
--   - FinalizeVerified now creates job_deliveries directly from delivery_destinations
--   - DeliveryRunner reads job_deliveries only
--   - CreateDeliveryTargetsForJob (enqueue) has been removed
--   - InsertJobDeliveriesForArtifact (bridge) has been removed
--   - SQLiteDeliveryRepository is no longer wired in bootstrap

-- 1. Drop legacy index on legacy_delivery_target_id
DROP INDEX IF EXISTS idx_job_deliveries_legacy;

-- 2. Drop legacy_delivery_target_id from job_deliveries
ALTER TABLE job_deliveries DROP COLUMN legacy_delivery_target_id;

-- 3. Drop delivery_target_id from delivery_attempts (already nullable since 032)
ALTER TABLE delivery_attempts DROP COLUMN delivery_target_id;

-- 4. Drop legacy delivery_targets table (no longer written to or read from)
DROP TABLE IF EXISTS delivery_targets;
