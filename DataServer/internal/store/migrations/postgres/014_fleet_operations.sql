-- Migration 014: fleet_operations ledger (Step 4/15 fleet-operator
-- rollout). Postgres variant of sqlite/104.
--
-- Differences from the SQLite DDL:
--   * all time columns are TIMESTAMP WITH TIME ZONE instead of TEXT,
--     so the JSON marshal surface stays clean (RFC3339 on both sides)
--   * payload is JSONB so operators can run ad-hoc JSON path
--     queries against the column (JSONB-indexed later as the
--     executor payload shapes stabilise)
--   * partial UNIQUE INDEX repeats the SQLite on-fleet_operations
--     in-flight constraint — the Go-side ErrOperationInFlight
--     translator matches on the index name so both dialects fail
--     cleanly to the same sentinel

CREATE TABLE IF NOT EXISTS fleet_operations (
    operation_id  TEXT                        PRIMARY KEY,
    worker_id     TEXT                        NOT NULL,
    op            TEXT                        NOT NULL
        CHECK (op IN ('drain', 'resume', 'restart', 'update', 'rollback', 'quarantine', 'smoke')),
    requested_by  TEXT                        NOT NULL,
    reason        TEXT                        NOT NULL,
    status        TEXT                        NOT NULL
        CHECK (status IN ('QUEUED', 'RUNNING', 'SUCCEEDED', 'FAILED')),
    queued_at     TIMESTAMP WITH TIME ZONE    NOT NULL,
    started_at    TIMESTAMP WITH TIME ZONE,
    finished_at   TIMESTAMP WITH TIME ZONE,
    payload       JSONB                       NOT NULL,
    error_message TEXT,
    CONSTRAINT fleet_operations_worker_id_nonempty
        CHECK (length(worker_id) > 0),
    CONSTRAINT fleet_operations_requested_by_nonempty
        CHECK (length(requested_by) > 0),
    CONSTRAINT fleet_operations_reason_nonempty
        CHECK (length(reason) > 0)
);

CREATE INDEX IF NOT EXISTS idx_fleet_ops_worker
    ON fleet_operations(worker_id, queued_at DESC);

CREATE INDEX IF NOT EXISTS idx_fleet_ops_status
    ON fleet_operations(status, queued_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS idx_fleet_ops_worker_op_inflight
    ON fleet_operations(worker_id, op)
    WHERE status IN ('QUEUED', 'RUNNING');
