-- Migration 103: deploy ledger for fleet-operator rollout (Step 5/15).
--
-- One row per worker deployment record. The ledger is the canonical
-- audit log fed by the Fleet Controller (Step 2) and consumed by the
-- drain/resume/rollback pipeline (Step 6). Atomic commit scope: this
-- is the foundation table — no lifecycle/metrics integration yet,
-- that lands in Step 6.
--
-- Schema invariants enforced at the SQL level:
--   * status ∈ {PENDING, SUCCEEDED, FAILED, ROLLED_BACK} (CHECK
--     constraint). The CHECK exists for defence-in-depth: the Go
--     repository already enforces this at InsertDeploymentRecord
--     (rejects non-PENDING initial state) and UpdateDeploymentStatus
--     (rejects non-terminal transitions). The DB CHECK catches
--     direct-INSERT bugs that bypass the repository.
--   * previous_digest / target_digest MUST be non-empty (CHECK
--     length() > 0). ValidateImageRef (the Go-side image-ref
--     validator in internal/deploy) rejects empty refs at the API
--     boundary, but a raw INSERT via psql or admin tooling bypasses
--     it — the SQL CHECK is the canonical shape gate that prevents
--     "ghost rows" with zero-length digests from making it into the
--     audit trail.
--   * started_at NOT NULL — every record has a creation moment.
--     finished_at NULL until terminal — distinguishes in-flight
--     deploys ("PENDING" + no finished_at) from completed ones
--     (terminal status + finished_at) at the dashboard query.
--   * FK(worker_id) -> workers(worker_id) — no orphan deploy rows.
--     PRAGMA foreign_keys=ON is enforced on EVERY pooled connection via
--     the DSN param _foreign_keys=true (sqliteDSNParams in
--     platform/database), not via a post-init db.Exec (which only
--     affects the single connection that ran it). This holds for both
--     the production bootstrap and the legacy NewSQLiteStoreFromPath
--     pool (both flow through platform/database.Open), so the FK is
--     enforced end-to-end on production boot regardless of
--     MaxOpenConns.
--   * is_rollback INTEGER (SQLite has no native BOOL; 0/1). Step 6's
--     rollback path sets this flag; the dashboard's "forward vs
--     rollback" view filters on it. Meaningful only with terminal
--     status (SUCCEEDED/ROLLED_BACK/FAILED).
--   * applied_by TEXT NOT NULL — free string identifying who/what
--     initiated the deploy (operator username, fleetctl invocation,
--     automated CI action). No FK to any user table; audit trail is
--     the deploy_records row itself plus the operator audit log.
--
-- IDEMPOTENT: CREATE TABLE IF NOT EXISTS / CREATE INDEX IF NOT EXISTS
-- throughout, so a Path-B rollout that already applied the table is
-- a silent no-op. Safe to re-run on a fresh installer.
CREATE TABLE IF NOT EXISTS deployment_records (
  deployment_id TEXT PRIMARY KEY,
  worker_id TEXT NOT NULL,
  previous_digest TEXT NOT NULL CHECK (length(previous_digest) > 0),
  target_digest TEXT NOT NULL CHECK (length(target_digest) > 0),
  started_at TEXT NOT NULL,
  finished_at TEXT,
  status TEXT NOT NULL CHECK (status IN ('PENDING', 'SUCCEEDED', 'FAILED', 'ROLLED_BACK')),
  applied_by TEXT NOT NULL,
  is_rollback INTEGER NOT NULL DEFAULT 0,
  FOREIGN KEY(worker_id) REFERENCES workers(worker_id)
);
CREATE INDEX IF NOT EXISTS idx_deployment_records_worker ON deployment_records(worker_id, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_deployment_records_status ON deployment_records(status, started_at DESC);
