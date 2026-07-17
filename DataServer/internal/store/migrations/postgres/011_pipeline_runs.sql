-- Migration 011: pipeline_runs (Postgres dialect)
--
-- Pipeline runs — durable, versioned source of truth for the lifecycle of a
-- client-initiated generation pipeline.
--
-- A pipeline_run is created before any remote call is made and tracks the
-- aggregated status exposed to API clients. It does not replace the internal
-- state machines of jobs, tasks, artifacts or deliveries; it projects them.

CREATE TABLE IF NOT EXISTS pipeline_runs (
    id                       TEXT PRIMARY KEY,
    request_id               TEXT NOT NULL DEFAULT '',
    idempotency_key          TEXT NOT NULL,
    user_id                  TEXT NOT NULL DEFAULT '',
    campaign_id              TEXT NOT NULL DEFAULT '',
    campaign_item_id         TEXT NOT NULL DEFAULT '',
    status                   TEXT NOT NULL DEFAULT 'ACCEPTED',
    current_stage            TEXT NOT NULL DEFAULT '',
    remote_provider          TEXT NOT NULL DEFAULT '',
    remote_job_id            TEXT NOT NULL DEFAULT '',
    forwarding_id            TEXT NOT NULL DEFAULT '',
    velox_job_id             TEXT NOT NULL DEFAULT '',
    artifact_id              TEXT NOT NULL DEFAULT '',
    delivery_id              TEXT NOT NULL DEFAULT '',
    requested_payload_json   TEXT NOT NULL DEFAULT '',
    normalized_payload_json  TEXT NOT NULL DEFAULT '',
    result_json              TEXT NOT NULL DEFAULT '',
    error_code               TEXT NOT NULL DEFAULT '',
    error_message            TEXT NOT NULL DEFAULT '',
    failed_stage             TEXT NOT NULL DEFAULT '',
    created_at               TEXT NOT NULL DEFAULT '',
    updated_at               TEXT NOT NULL DEFAULT '',
    completed_at             TEXT NOT NULL DEFAULT ''
);

-- Enforce idempotency: one pipeline_run per idempotency_key.
CREATE UNIQUE INDEX IF NOT EXISTS idx_pg_pipeline_runs_idempotency_key
    ON pipeline_runs(idempotency_key);

-- Lookup by the originating request.
CREATE INDEX IF NOT EXISTS idx_pg_pipeline_runs_request_id
    ON pipeline_runs(request_id);

-- Common list/filter queries.
CREATE INDEX IF NOT EXISTS idx_pg_pipeline_runs_user_status
    ON pipeline_runs(user_id, status);

CREATE INDEX IF NOT EXISTS idx_pg_pipeline_runs_created_at
    ON pipeline_runs(created_at);

-- Cross-reference indexes for internal reconciliation.
CREATE INDEX IF NOT EXISTS idx_pg_pipeline_runs_remote_job_id
    ON pipeline_runs(remote_job_id)
    WHERE remote_job_id IS NOT NULL AND remote_job_id != '';

CREATE INDEX IF NOT EXISTS idx_pg_pipeline_runs_velox_job_id
    ON pipeline_runs(velox_job_id)
    WHERE velox_job_id IS NOT NULL AND velox_job_id != '';

CREATE INDEX IF NOT EXISTS idx_pg_pipeline_runs_campaign
    ON pipeline_runs(campaign_id, campaign_item_id);
