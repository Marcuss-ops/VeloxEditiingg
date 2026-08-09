-- Migration 134: allow deployment baselines without rollback provenance.
--
-- A ledger bootstrap certifies the authenticated image that is already
-- running. It must not fabricate previous_digest=target_digest, so the
-- absence of rollback provenance is represented by SQL NULL.

DROP INDEX IF EXISTS idx_deployment_records_worker;
DROP INDEX IF EXISTS idx_deployment_records_status;

CREATE TABLE deployment_records_bootstrap (
  deployment_id TEXT PRIMARY KEY,
  worker_id TEXT NOT NULL,
  previous_digest TEXT CHECK (previous_digest IS NULL OR length(previous_digest) > 0),
  target_digest TEXT NOT NULL CHECK (length(target_digest) > 0),
  started_at TEXT NOT NULL,
  finished_at TEXT,
  status TEXT NOT NULL CHECK (status IN ('PENDING', 'SUCCEEDED', 'FAILED', 'ROLLED_BACK')),
  applied_by TEXT NOT NULL,
  is_rollback INTEGER NOT NULL DEFAULT 0,
  FOREIGN KEY(worker_id) REFERENCES workers(worker_id)
);

INSERT INTO deployment_records_bootstrap
  (deployment_id, worker_id, previous_digest, target_digest, started_at,
   finished_at, status, applied_by, is_rollback)
SELECT deployment_id, worker_id, previous_digest, target_digest, started_at,
       finished_at, status, applied_by, is_rollback
  FROM deployment_records;

DROP TABLE deployment_records;
ALTER TABLE deployment_records_bootstrap RENAME TO deployment_records;

CREATE INDEX idx_deployment_records_worker ON deployment_records(worker_id, started_at DESC);
CREATE INDEX idx_deployment_records_status ON deployment_records(status, started_at DESC);
