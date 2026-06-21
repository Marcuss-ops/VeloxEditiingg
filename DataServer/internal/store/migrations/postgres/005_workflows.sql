-- Migration 005: workflows (Postgres dialect)
--
-- Workflow v2 persistence (per PR 9). Mirrors SQLite migration
-- 027_workflow_v2.sql: runs (top-level orchestration units) +
-- steps (individual nodes within a run) + events (audit trail) +
-- outbox (transactional event emission).
--
-- All FKs are intra-domain (workflow_runs ↔ workflow_steps ↔
-- workflow_events/workflow_outbox) so we declare them inline.

CREATE TABLE IF NOT EXISTS workflow_runs (
    run_id            TEXT PRIMARY KEY,
    spec_json         TEXT NOT NULL,
    status            TEXT NOT NULL DEFAULT 'PENDING',
    created_at        TEXT NOT NULL,
    updated_at        TEXT NOT NULL,
    completed_at      TEXT,
    revision          BIGINT NOT NULL DEFAULT 0,
    parent_run_id     TEXT,
    triggered_by      TEXT,
    metadata_json     TEXT
);

CREATE INDEX IF NOT EXISTS idx_pg_workflow_runs_status ON workflow_runs(status);
CREATE INDEX IF NOT EXISTS idx_pg_workflow_runs_updated ON workflow_runs(updated_at);

CREATE TABLE IF NOT EXISTS workflow_steps (
    step_id           TEXT PRIMARY KEY,
    run_id            TEXT NOT NULL REFERENCES workflow_runs(run_id) ON DELETE CASCADE,
    name              TEXT NOT NULL,
    kind              TEXT NOT NULL,
    status            TEXT NOT NULL DEFAULT 'BLOCKED',
    depends_on_json   TEXT NOT NULL DEFAULT '[]',
    spec_json         TEXT NOT NULL,
    output_json       TEXT,
    started_at        TEXT,
    completed_at      TEXT,
    job_id            TEXT,
    revision          BIGINT NOT NULL DEFAULT 0,
    created_at        TEXT NOT NULL,
    updated_at        TEXT NOT NULL,
    UNIQUE(run_id, name)
);

CREATE INDEX IF NOT EXISTS idx_pg_workflow_steps_run ON workflow_steps(run_id);
CREATE INDEX IF NOT EXISTS idx_pg_workflow_steps_status ON workflow_steps(status);

CREATE TABLE IF NOT EXISTS workflow_events (
    event_id          TEXT PRIMARY KEY,
    run_id            TEXT NOT NULL REFERENCES workflow_runs(run_id) ON DELETE CASCADE,
    step_id           TEXT REFERENCES workflow_steps(step_id) ON DELETE SET NULL,
    event_type        TEXT NOT NULL,
    payload_json      TEXT NOT NULL DEFAULT '{}',
    created_at        TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_pg_workflow_events_run ON workflow_events(run_id, created_at);

CREATE TABLE IF NOT EXISTS workflow_outbox (
    event_id          TEXT PRIMARY KEY,
    aggregate_type    TEXT NOT NULL DEFAULT 'workflow',
    aggregate_id      TEXT NOT NULL,
    event_type        TEXT NOT NULL,
    payload_json      TEXT NOT NULL DEFAULT '{}',
    status            TEXT NOT NULL DEFAULT 'PENDING',
    available_at      TEXT NOT NULL,
    created_at        TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_pg_workflow_outbox_pending ON workflow_outbox(available_at)
    WHERE status = 'PENDING';
