-- Migration 022: asynchronous media probing queue.

CREATE TABLE IF NOT EXISTS media_probe_jobs (
    id                    BIGSERIAL PRIMARY KEY,
    artifact_id           TEXT NOT NULL,
    sha256                TEXT NOT NULL,
    storage_key           TEXT NOT NULL,
    expected_audio_streams INTEGER NOT NULL DEFAULT 0,
    destination_id        TEXT NOT NULL DEFAULT '',
    status                TEXT NOT NULL DEFAULT 'PENDING',
    attempt_count         INTEGER NOT NULL DEFAULT 0,
    max_attempts          INTEGER NOT NULL DEFAULT 5,
    next_attempt_at       TIMESTAMPTZ NOT NULL,
    lease_id              TEXT NOT NULL DEFAULT '',
    lease_until           TIMESTAMPTZ,
    actual_audio_streams  INTEGER NOT NULL DEFAULT 0,
    duration_ms           BIGINT NOT NULL DEFAULT 0,
    last_error            TEXT NOT NULL DEFAULT '',
    created_at            TIMESTAMPTZ NOT NULL,
    updated_at            TIMESTAMPTZ NOT NULL,
    completed_at          TIMESTAMPTZ,
    UNIQUE (artifact_id, sha256)
);

CREATE INDEX IF NOT EXISTS idx_media_probe_jobs_claim
    ON media_probe_jobs(status, next_attempt_at, lease_until);
CREATE INDEX IF NOT EXISTS idx_media_probe_jobs_sha
    ON media_probe_jobs(sha256);
