-- Migration 027 — Workflow v2 (PR 8).
--
-- Replaces the legacy `orchestrator_jobs.raw_json` blob + read-through cache
-- with normalized tables: workflow_runs, workflow_steps, workflow_dependencies,
-- workflow_events.
--
-- Lifecycle states are application-enforced by internal/workflow.
--   workflow_runs.status:  PENDING | RUNNING | SUCCEEDED | FAILED | CANCELLED
--   workflow_steps.status: BLOCKED | READY | RUNNING | SUCCEEDED | FAILED
--   (BLOCKED = at least one predecessor not terminal; READY = deps met, ready
--    to be dispatched by the handler.)

CREATE TABLE IF NOT EXISTS workflow_runs (
    run_id             TEXT PRIMARY KEY,
    workflow_type      TEXT NOT NULL,
    status             TEXT NOT NULL,
    input_json         TEXT NOT NULL DEFAULT '{}',
    output_json        TEXT NOT NULL DEFAULT '{}',
    revision           INTEGER NOT NULL DEFAULT 0,
    created_at         TEXT NOT NULL,
    updated_at         TEXT NOT NULL,
    started_at         TEXT,
    completed_at       TEXT,
    last_error_code    TEXT,
    last_error_message TEXT
);

CREATE INDEX IF NOT EXISTS idx_workflow_runs_status
    ON workflow_runs(status, updated_at);

CREATE TABLE IF NOT EXISTS workflow_steps (
    step_id        TEXT PRIMARY KEY,
    run_id         TEXT NOT NULL,
    step_key       TEXT NOT NULL,
    job_id         TEXT,
    status         TEXT NOT NULL,
    attempt        INTEGER NOT NULL DEFAULT 0,
    max_attempts   INTEGER NOT NULL DEFAULT 3,
    input_json     TEXT NOT NULL DEFAULT '{}',
    output_json    TEXT NOT NULL DEFAULT '{}',
    revision       INTEGER NOT NULL DEFAULT 0,
    created_at     TEXT NOT NULL,
    updated_at     TEXT NOT NULL,
    started_at     TEXT,
    completed_at   TEXT,
    error_code     TEXT,
    error_message  TEXT,
    UNIQUE(run_id, step_key),
    UNIQUE(job_id),
    FOREIGN KEY(run_id) REFERENCES workflow_runs(run_id)
);

CREATE INDEX IF NOT EXISTS idx_workflow_steps_run
    ON workflow_steps(run_id, status);

CREATE INDEX IF NOT EXISTS idx_workflow_steps_status
    ON workflow_steps(status, updated_at);

CREATE TABLE IF NOT EXISTS workflow_dependencies (
    run_id              TEXT NOT NULL,
    step_id             TEXT NOT NULL,
    depends_on_step_id  TEXT NOT NULL,
    PRIMARY KEY(run_id, step_id, depends_on_step_id)
);

CREATE INDEX IF NOT EXISTS idx_workflow_dependencies_target
    ON workflow_dependencies(run_id, step_id);

CREATE INDEX IF NOT EXISTS idx_workflow_dependencies_source
    ON workflow_dependencies(run_id, depends_on_step_id);

CREATE TABLE IF NOT EXISTS workflow_events (
    event_id      TEXT PRIMARY KEY,
    run_id        TEXT NOT NULL,
    step_id       TEXT,
    event_type    TEXT NOT NULL,
    payload_json  TEXT NOT NULL DEFAULT '{}',
    created_at    TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_workflow_events_run
    ON workflow_events(run_id, created_at);
