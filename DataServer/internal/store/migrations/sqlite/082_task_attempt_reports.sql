-- 082_task_attempt_reports.sql
--
-- Step 16 / Scorecard v2: stores the raw, versioned worker report payload
-- for every task attempt. The typed tables remain the source of truth for
-- fast queries; this table preserves the exact bytes sent by the worker for
-- audit, replay, and forward-compatible metric extraction.
--
-- attempt_id        — canonical attempt identity (matches task_attempts.id)
-- report_schema     — schema/version of the raw report format (integer)
-- report_hash       — SHA-256 hex digest of raw_report_json
-- raw_report_json   — the complete report payload as received from the worker
-- received_at       — RFC3339 timestamp when the master received the report
-- persisted_at      — RFC3339 timestamp when the row was written to the DB

CREATE TABLE IF NOT EXISTS task_attempt_reports (
    attempt_id       TEXT PRIMARY KEY,
    report_schema    INTEGER NOT NULL DEFAULT 1,
    report_hash      TEXT    NOT NULL,
    raw_report_json  TEXT    NOT NULL,
    received_at      TEXT    NOT NULL,
    persisted_at     TEXT    NOT NULL
);

-- Fast lookup by hash for idempotency / conflict detection.
CREATE INDEX IF NOT EXISTS idx_task_attempt_reports_hash
    ON task_attempt_reports(report_hash);

-- Fast lookup by receive time for retention / audit scans.
CREATE INDEX IF NOT EXISTS idx_task_attempt_reports_received_at
    ON task_attempt_reports(received_at);
