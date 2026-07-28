-- Migration 013: deployment_records ledger (Step 5/15 fleet-operator
-- rollout).
--
-- See sqlite/103_deployment_records.sql for the full schema
-- invariants. The two schemas MUST stay column-for-column
-- identical (excluding the native BOOLEAN↔INTEGER dialect difference
-- for is_rollback, which Postgres supports natively, and the CHECK
-- length() syntax which both dialects accept for TEXT columns).
-- If a new column lands in one, it MUST land in the same place in
-- the other, otherwise the dual-dialect boot path will diverge.
--
-- IDEMPOTENT: CREATE TABLE IF NOT EXISTS / CREATE INDEX IF NOT EXISTS
-- throughout. A Path-B rollout that already applied the table on a
-- fresh Postgres is a silent no-op.
CREATE TABLE IF NOT EXISTS deployment_records (
  deployment_id TEXT PRIMARY KEY,
  worker_id TEXT NOT NULL REFERENCES workers(worker_id),
  previous_digest TEXT NOT NULL CHECK (length(previous_digest) > 0),
  target_digest TEXT NOT NULL CHECK (length(target_digest) > 0),
  started_at TEXT NOT NULL,
  finished_at TEXT,
  status TEXT NOT NULL CHECK (status IN ('PENDING', 'SUCCEEDED', 'FAILED', 'ROLLED_BACK')),
  applied_by TEXT NOT NULL,
  is_rollback BOOLEAN NOT NULL DEFAULT FALSE
);
CREATE INDEX IF NOT EXISTS idx_deployment_records_worker ON deployment_records(worker_id, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_deployment_records_status ON deployment_records(status, started_at DESC);
