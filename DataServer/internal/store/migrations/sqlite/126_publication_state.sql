-- 126_publication_state.sql
-- Durable publication orchestration state. Delivery rows remain the transport
-- ledger; these tables record publication phases and their idempotency
-- boundary so a retry can resume after the last completed phase.

CREATE TABLE IF NOT EXISTS publication_states (
    publication_id TEXT PRIMARY KEY,
    job_id         TEXT,
    state          TEXT NOT NULL CHECK (state IN (
        'PENDING', 'WAITING_FOR_RENDER', 'ARTIFACT_BOUND', 'READY',
        'SCHEDULED', 'UPLOADING', 'VIDEO_CREATED', 'METADATA_APPLYING',
        'LOCALIZATIONS_APPLYING', 'VERIFYING', 'PUBLISHED', 'PARTIAL',
        'RETRY_WAIT', 'FAILED', 'CANCELLED'
    )),
    retry_from     TEXT CHECK (retry_from IS NULL OR retry_from IN (
        'WAITING_FOR_RENDER', 'ARTIFACT_BOUND', 'UPLOADING',
        'METADATA_APPLYING', 'LOCALIZATIONS_APPLYING', 'VERIFYING'
    )),
    artifact_id    TEXT,
    remote_id      TEXT,
    remote_url     TEXT,
    revision       INTEGER NOT NULL DEFAULT 0,
    last_error_code TEXT,
    created_at     TEXT NOT NULL,
    updated_at     TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_publication_states_state
    ON publication_states(state, updated_at);

CREATE TABLE IF NOT EXISTS publication_phase_effects (
    publication_id TEXT NOT NULL,
    phase          TEXT NOT NULL,
    operation      TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    status         TEXT NOT NULL CHECK (status IN ('RUNNING', 'SUCCEEDED', 'FAILED')),
    error_code     TEXT,
    created_at     TEXT NOT NULL,
    updated_at     TEXT NOT NULL,
    PRIMARY KEY (publication_id, phase, operation),
    UNIQUE (idempotency_key),
    FOREIGN KEY (publication_id) REFERENCES publication_states(publication_id)
);

CREATE INDEX IF NOT EXISTS idx_publication_phase_effects_status
    ON publication_phase_effects(status, updated_at);
