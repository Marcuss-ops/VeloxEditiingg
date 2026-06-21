-- Migration 006: artifacts (Postgres dialect)
--
-- Artifact persistence: the canonical row per Artifact() in store/artifacts.go,
-- plus upload session bookkeeping, plus chunked-upload records. Mirrors
-- SQLite migrations 010_job_attempts_and_artifacts.sql (initial
-- artifacts table), 030_artifact_uploads.sql (upload_sessions), and
-- 036_artifact_upload_chunks.sql (chunked upload rows).
--
-- No FK to jobs or workers — those reference the ported cross-domain
-- tables and ship in their own PG migration files (002_jobs,
-- 003_workers). A later migration adds the FKs.
--
-- The unique partial index on (storage_provider, storage_key) mirrors
-- SQLite migration 030's UNIQUE INDEX … WHERE storage_key <> '' so an
-- empty storage_key (in-flight uploads) does not collide.

CREATE TABLE IF NOT EXISTS artifacts (
    id                  TEXT PRIMARY KEY,
    job_id              TEXT NOT NULL,
    attempt_id          BIGINT NOT NULL DEFAULT 0,
    type                TEXT NOT NULL,
    storage_provider    TEXT NOT NULL DEFAULT 'local',
    storage_key         TEXT,
    storage_url         TEXT,
    local_path          TEXT,
    sha256              TEXT,
    size_bytes          BIGINT NOT NULL DEFAULT 0,
    duration_seconds    DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    status              TEXT NOT NULL DEFAULT 'pending',
    verified_at         TEXT,
    created_at          TEXT NOT NULL,
    updated_at          TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_pg_artifacts_job ON artifacts(job_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_pg_artifacts_storage_key
    ON artifacts(storage_provider, storage_key)
    WHERE storage_key IS NOT NULL AND storage_key <> '';
CREATE INDEX IF NOT EXISTS idx_pg_artifacts_status ON artifacts(status);

CREATE TABLE IF NOT EXISTS artifact_upload_sessions (
    upload_id           TEXT PRIMARY KEY,
    artifact_id         TEXT NOT NULL,
    job_id              TEXT NOT NULL,
    status              TEXT NOT NULL DEFAULT 'CREATED',
    bytes_uploaded      BIGINT NOT NULL DEFAULT 0,
    bytes_total         BIGINT NOT NULL DEFAULT 0,
    storage_provider    TEXT NOT NULL DEFAULT 'local',
    storage_key         TEXT,
    created_at          TEXT NOT NULL,
    updated_at          TEXT NOT NULL,
    finalized_at        TEXT,
    expires_at          TEXT
);

CREATE INDEX IF NOT EXISTS idx_pg_upload_sessions_artifact ON artifact_upload_sessions(artifact_id);
CREATE INDEX IF NOT EXISTS idx_pg_upload_sessions_status ON artifact_upload_sessions(status);

CREATE TABLE IF NOT EXISTS artifact_chunks (
    upload_id           TEXT NOT NULL,
    chunk_index         INTEGER NOT NULL,
    offset_bytes        BIGINT NOT NULL,
    size_bytes          BIGINT NOT NULL,
    sha256              TEXT,
    status              TEXT NOT NULL DEFAULT 'PENDING',
    created_at          TEXT NOT NULL,
    PRIMARY KEY (upload_id, chunk_index)
);
