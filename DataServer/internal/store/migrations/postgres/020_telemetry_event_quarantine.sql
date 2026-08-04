-- Migration 020: best-effort telemetry event quarantine.

CREATE TABLE IF NOT EXISTS telemetry_event_quarantine (
    id                     BIGSERIAL PRIMARY KEY,
    attempt_id             TEXT NOT NULL,
    event_id               TEXT NOT NULL,
    origin                 TEXT NOT NULL DEFAULT '',
    scope                  TEXT NOT NULL DEFAULT '',
    component              TEXT NOT NULL DEFAULT '',
    action                 TEXT NOT NULL DEFAULT '',
    schema_version         INTEGER NOT NULL DEFAULT 0,
    reason                 TEXT NOT NULL,
    event_json             TEXT NOT NULL DEFAULT '{}',
    created_at             TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_telemetry_event_quarantine_attempt_event
    ON telemetry_event_quarantine(attempt_id, event_id);
CREATE INDEX IF NOT EXISTS idx_telemetry_event_quarantine_created
    ON telemetry_event_quarantine(created_at);
