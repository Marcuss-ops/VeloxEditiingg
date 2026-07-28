-- 104_smoke_runs.sql — Step 12/15 fleet-operator: smoke_runs analytics
-- table. Records the END-TO-END duration baseline for every Level
-- D smoke run by LevelDSmokeExecutor.
--
-- Distinct from:
--   - fleet_operations (ops audit trail — Step 4/15)
--   - deployment_records (deploy lifecycle — Step 5/15)
--
-- duration_ms is the canonical end-to-end duration column for the
-- analytics baseline. The smoke dashboard's p95 / p99 / moving-avg
-- are all computed from this column.
--
-- Idempotent: CREATE TABLE IF NOT EXISTS so re-running the
-- migration is safe. The schema_migrations row records the
-- checksum at the migration-runner level.

CREATE TABLE IF NOT EXISTS smoke_runs (
  run_id TEXT PRIMARY KEY,
  worker_id TEXT NOT NULL,
  started_at TEXT NOT NULL,
  finished_at TEXT NOT NULL,
  duration_ms INTEGER NOT NULL CHECK (duration_ms >= 0),
  asset_id TEXT NOT NULL CHECK (length(asset_id) > 0),
  artifact_drive_id TEXT,
  status TEXT NOT NULL CHECK (status IN ('PENDING', 'SUCCEEDED', 'FAILED')),
  error_message TEXT,
  requested_by TEXT NOT NULL CHECK (length(requested_by) > 0)
);

CREATE INDEX IF NOT EXISTS idx_smoke_runs_worker ON smoke_runs(worker_id, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_smoke_runs_status ON smoke_runs(status, started_at DESC);
