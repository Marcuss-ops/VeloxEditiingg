-- Migration 133: asynchronous media probing queue.
--
-- Finalize must not hold the ingest request open while ffprobe runs. One row
-- represents a probe for a content-addressed artifact revision; the unique
-- artifact+sha key makes retries and duplicate enqueue calls idempotent.

CREATE TABLE IF NOT EXISTS media_probe_jobs (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    artifact_id           TEXT NOT NULL,
    sha256                TEXT NOT NULL,
    storage_key           TEXT NOT NULL,
    expected_audio_streams INTEGER NOT NULL DEFAULT 0,
    destination_id        TEXT NOT NULL DEFAULT '',
    status                TEXT NOT NULL DEFAULT 'PENDING',
    attempt_count         INTEGER NOT NULL DEFAULT 0,
    max_attempts          INTEGER NOT NULL DEFAULT 5,
    next_attempt_at       TEXT NOT NULL,
    lease_id              TEXT NOT NULL DEFAULT '',
    lease_until            TEXT NOT NULL DEFAULT '',
    actual_audio_streams  INTEGER NOT NULL DEFAULT 0,
    duration_ms           INTEGER NOT NULL DEFAULT 0,
    last_error            TEXT NOT NULL DEFAULT '',
    created_at            TEXT NOT NULL,
    updated_at            TEXT NOT NULL,
    completed_at          TEXT,
    UNIQUE (artifact_id, sha256)
);

CREATE INDEX IF NOT EXISTS idx_media_probe_jobs_claim
    ON media_probe_jobs(status, next_attempt_at, lease_until);
CREATE INDEX IF NOT EXISTS idx_media_probe_jobs_sha
    ON media_probe_jobs(sha256);
