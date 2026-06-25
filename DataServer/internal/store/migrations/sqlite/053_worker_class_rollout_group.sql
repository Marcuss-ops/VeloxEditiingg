-- Migration 053 — RW-PROD-005: worker_class + rollout_group
--
-- Background:
--   The operator-facing GET /api/v1/workers API (RW-PROD-005 §2.3) now
--   accepts ?class=cpu-xlarge&status=CONNECTED&rollout_group=v3.4 + a
--   Class + RolloutGroup field on the WorkerResponse. To make
--   ?class= + ?rollout_group= filterable at A7's "DB-level" tier we need
--   these as actual SQL columns instead of buried inside capabilities JSON.
--
--   pre-RW-PROD-005 the workers table (001_initial.sql) only carries:
--     worker_id, worker_name, status, last_heartbeat, schedulable, drain,
--     worker_group, raw_json, migrated_at
--   plus the per-PR columns: display_name, ip_address, first_seen,
--   current_job, code_version, bundle_version, bundle_hash,
--   protocol_version, engine_version, recent_logs, recent_errors,
--   readiness, metrics, capabilities.
--
--   Operators routinely tag workers by class (cpu/gpu/mixed/io) and
--   rollout (v3.3 / v3.4 / canary) — worker_group is the cluster-level
--   grouping and is NOT a substitute.
--
-- Idempotency:
--   ALTER TABLE ... ADD COLUMN on SQLite is opaque to IF NOT EXISTS until
--   3.35 (we target 3.36+). The migration runner checks the schema_migrations
--   table for applied versions; re-runs hit the dedup gate before this file
--   is parsed, so we don't need IF NOT EXISTS guards here. Tests assert the
--   ADD COLUMN is idempotent at the column-presence layer (NOT NULL DEFAULT '')
--   so a legacy row that was inserted before this migration loads with both
--   fields = '' which the handler treats as "no filter on this dimension".
--
-- Indexes:
--   idx_workers_worker_class  — supports ?class=cpu-xlarge filter
--   idx_workers_rollout_group — supports ?rollout_group=v3.4 filter
--   They are NOT unique (multiple workers can share class/rollout); they are
--   b-tree lookups on cardinality ~= fleet_size which is small enough that
--   a hash index would not improve performance.
--
-- Status (the per-worker ConnectionStatus enum) is NOT a column — it is
-- derived at read-time from heartbeat + session_active so a revocation in
-- worker_sessions instantly demotes a worker without an SQLite write.
-- Filtering by status happens post-WHERE in the in-memory registry.

ALTER TABLE workers ADD COLUMN worker_class TEXT NOT NULL DEFAULT '';
ALTER TABLE workers ADD COLUMN rollout_group TEXT NOT NULL DEFAULT '';

CREATE INDEX IF NOT EXISTS idx_workers_worker_class ON workers(worker_class);
CREATE INDEX IF NOT EXISTS idx_workers_rollout_group ON workers(rollout_group);
