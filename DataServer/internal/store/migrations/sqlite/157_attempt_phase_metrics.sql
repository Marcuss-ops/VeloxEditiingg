-- 157_attempt_phase_metrics.sql
--
-- Canonical phase roll-up for real bottleneck analysis. The detailed
-- task_execution_events table remains the source of truth; this table is a
-- compact, queryable projection written in the same atomic ingest tx.

CREATE TABLE IF NOT EXISTS task_attempt_phase_metrics (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    attempt_id         TEXT    NOT NULL,
    job_id             TEXT    NOT NULL DEFAULT '',
    task_id            TEXT    NOT NULL DEFAULT '',
    worker_id          TEXT    NOT NULL DEFAULT '',
    phase              TEXT    NOT NULL,
    duration_ms        INTEGER NOT NULL DEFAULT 0,
    event_count        INTEGER NOT NULL DEFAULT 0,
    cpu_ms             REAL    NOT NULL DEFAULT 0.0,
    queue_wait_ms      REAL    NOT NULL DEFAULT 0.0,
    bytes_in           INTEGER NOT NULL DEFAULT 0,
    bytes_out          INTEGER NOT NULL DEFAULT 0,
    frames             INTEGER NOT NULL DEFAULT 0,
    max_duration_ms    INTEGER NOT NULL DEFAULT 0,
    first_started_at   TEXT    NOT NULL DEFAULT '',
    last_completed_at  TEXT    NOT NULL DEFAULT '',
    calculated_at      TEXT    NOT NULL DEFAULT '',
    UNIQUE (attempt_id, phase)
);

CREATE INDEX IF NOT EXISTS idx_attempt_phase_metrics_phase
    ON task_attempt_phase_metrics(phase, duration_ms DESC);
CREATE INDEX IF NOT EXISTS idx_attempt_phase_metrics_worker
    ON task_attempt_phase_metrics(worker_id, calculated_at);
