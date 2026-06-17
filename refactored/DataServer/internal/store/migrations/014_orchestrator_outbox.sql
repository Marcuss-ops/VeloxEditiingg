-- Migration 014: Orchestrator transactional outbox
-- Ensures events are never lost by writing state changes and outbox entries in the same transaction.
-- Replaces in-memory channels with SQLite-authoritative outbox polling.

CREATE TABLE IF NOT EXISTS orchestrator_outbox (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type TEXT NOT NULL CHECK(event_type IN ('step_dispatch', 'step_complete', 'step_failed', 'job_complete', 'job_fail', 'step_ready')),
    job_id TEXT NOT NULL,
    step_id TEXT,
    payload TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    processed INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_outbox_processed ON orchestrator_outbox(processed, created_at);
CREATE INDEX IF NOT EXISTS idx_outbox_job ON orchestrator_outbox(job_id);
