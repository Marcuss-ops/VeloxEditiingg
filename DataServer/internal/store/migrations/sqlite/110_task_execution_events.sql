-- 110_task_execution_events.sql
--
-- Observability chain / block 1: append-only per-attempt execution
-- events.
--
-- task_execution_events is the timeline layer of the observability
-- chain: one row per discrete execution event (phase start/complete,
-- segment encode, engine round-trip, upload progress, validation
-- verdict, …). It exists BECAUSE task_phase_timings' unique key
-- (attempt_id, component, action) cannot hold a second, third, … tenth
-- occurrence of the same (component, action) — e.g. ten distinct
-- engine.encode events on one attempt. Events are append-only and
-- indexed by (attempt_id, origin, event_index) so replays of the same
-- report are idempotent.
--
-- Origin is a CLOSED enum (CHECK-guarded):
--   master, worker, engine, ffmpeg, upload, validation
-- Scope is a CLOSED enum (CHECK-guarded):
--   job, task, attempt, segment, audio_track, subtitle_track, artifact
-- The identity tuple (job/task/worker/session/snapshot/lease/executor)
-- is stamped by the MASTER from the canonical identity tuple at ingest;
-- values echoed by the worker are never trusted verbatim.
--
-- Idempotency: INSERT OR IGNORE on the UNIQUE(attempt_id, origin,
-- event_index) key — a replayed report re-inserts nothing.

CREATE TABLE IF NOT EXISTS task_execution_events (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    attempt_id         TEXT    NOT NULL,
    job_id             TEXT    NOT NULL DEFAULT '',
    task_id            TEXT    NOT NULL DEFAULT '',
    worker_id          TEXT    NOT NULL DEFAULT '',
    worker_session_id  TEXT    NOT NULL DEFAULT '',
    worker_snapshot_id TEXT    NOT NULL DEFAULT '',
    lease_id           TEXT    NOT NULL DEFAULT '',
    executor_id        TEXT    NOT NULL DEFAULT '',
    executor_version   INTEGER NOT NULL DEFAULT 0,
    event_index        INTEGER NOT NULL DEFAULT 0,
    origin             TEXT    NOT NULL,
    scope              TEXT    NOT NULL DEFAULT 'task',
    event_type         TEXT    NOT NULL DEFAULT '',
    event_name         TEXT    NOT NULL DEFAULT '',
    component          TEXT    NOT NULL DEFAULT '',
    action             TEXT    NOT NULL DEFAULT '',
    phase              TEXT    NOT NULL DEFAULT '',
    status             TEXT    NOT NULL DEFAULT 'ok',
    error_code         TEXT    NOT NULL DEFAULT '',
    error_message      TEXT    NOT NULL DEFAULT '',
    started_at         TEXT    NOT NULL DEFAULT '',
    completed_at       TEXT    NOT NULL DEFAULT '',
    duration_ms        INTEGER NOT NULL DEFAULT 0,
    bytes_in           INTEGER NOT NULL DEFAULT 0,
    bytes_out          INTEGER NOT NULL DEFAULT 0,
    frames             INTEGER NOT NULL DEFAULT 0,
    metadata_json      TEXT    NOT NULL DEFAULT '{}',
    created_at         TEXT    NOT NULL DEFAULT '',
    CHECK (origin IN ('master', 'worker', 'engine', 'ffmpeg', 'upload', 'validation')),
    CHECK (scope IN ('job', 'task', 'attempt', 'segment', 'audio_track', 'subtitle_track', 'artifact'))
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_task_execution_events_attempt_origin_index
    ON task_execution_events(attempt_id, origin, event_index);

CREATE INDEX IF NOT EXISTS idx_task_execution_events_attempt
    ON task_execution_events(attempt_id, event_index);

CREATE INDEX IF NOT EXISTS idx_task_execution_events_worker
    ON task_execution_events(worker_id, worker_snapshot_id, created_at);

CREATE INDEX IF NOT EXISTS idx_task_execution_events_phase
    ON task_execution_events(component, action, status);
