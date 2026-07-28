-- Migration 104: fleet_operations ledger (Step 4/15 fleet-operator rollout).
--
-- The fleet_operations table is the durable audit trail of every
-- admin-driven fleet mutation (drain / resume / restart / update /
-- rollback / quarantine / smoke). Each row documents the publish
-- (operator + reason + queued_at), the executor attempt
-- (started_at, finished_at, error_message), and the specific
-- payload (op-dependent JSON the executor consumes).
--
-- Atomic in-flight idempotency lives at the DB layer via a
-- PARTIAL UNIQUE INDEX on (worker_id, op) WHERE status IN
-- ('QUEUED', 'RUNNING'): two parallel operators clicking the
-- same action at the same time fail the second INSERT with a
-- UNIQUE-constraint error that the repository translates to
-- ErrOperationInFlight. The partial form MUST NOT cover
-- SUCCEEDED / FAILED — a Monday-reboot and a Tuesday-reboot on
-- the same worker are both legitimate.
--
-- Idempotent (CREATE TABLE / CREATE INDEX IF NOT EXISTS).

CREATE TABLE IF NOT EXISTS fleet_operations (
    operation_id  TEXT PRIMARY KEY,
    worker_id     TEXT NOT NULL
        CHECK (length(worker_id) > 0),
    op            TEXT NOT NULL
        CHECK (op IN ('drain', 'resume', 'restart', 'update', 'rollback', 'quarantine', 'smoke')),
    requested_by  TEXT NOT NULL
        CHECK (length(requested_by) > 0),
    reason        TEXT NOT NULL
        CHECK (length(reason) > 0),
    status        TEXT NOT NULL
        CHECK (status IN ('QUEUED', 'RUNNING', 'SUCCEEDED', 'FAILED')),
    queued_at     TEXT NOT NULL,
    started_at    TEXT,
    finished_at   TEXT,
    payload       TEXT NOT NULL
        CHECK (length(payload) > 0),
    error_message TEXT
);

CREATE INDEX IF NOT EXISTS idx_fleet_ops_worker
    ON fleet_operations(worker_id, queued_at DESC);

CREATE INDEX IF NOT EXISTS idx_fleet_ops_status
    ON fleet_operations(status, queued_at DESC);

-- Partial UNIQUE INDEX. Without WHERE, a re-issue after a prior
-- run would always fail (every (worker, op) eventually terminates,
-- but a fresh run would collide with the historical row). The
-- WHERE clause restricts the uniquness to LIVE rows, so a worker
-- can be drained twice across days.
CREATE UNIQUE INDEX IF NOT EXISTS idx_fleet_ops_worker_op_inflight
    ON fleet_operations(worker_id, op)
    WHERE status IN ('QUEUED', 'RUNNING');
