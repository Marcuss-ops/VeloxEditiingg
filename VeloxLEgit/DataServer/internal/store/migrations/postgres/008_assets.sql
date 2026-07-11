-- Migration 008: assets (Postgres dialect)
--
-- Voiceover asset registry. Mirrors SQLite migration
-- 029_asset_registry.sql. The Postgres shape uses TEXT for storage
-- paths (so paths with special characters pass through unchanged) and
-- keeps the optional metadata column for forward-compatibility with
-- the future rich-asset extension (subtitle clips, B-roll, etc.).

CREATE TABLE IF NOT EXISTS voiceover_assets (
    asset_id            TEXT PRIMARY KEY,
    name                TEXT NOT NULL,
    language            TEXT,
    source_kind         TEXT NOT NULL DEFAULT 'manual',
    source_ref          TEXT,
    storage_provider    TEXT NOT NULL DEFAULT 'local',
    storage_key         TEXT,
    duration_seconds    DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    sample_rate_hz      INTEGER,
    channels            INTEGER,
    sha256              TEXT,
    bytes               BIGINT NOT NULL DEFAULT 0,
    metadata_json       TEXT,
    created_at          TEXT NOT NULL,
    updated_at          TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_pg_voiceover_assets_language ON voiceover_assets(language);
CREATE INDEX IF NOT EXISTS idx_pg_voiceover_assets_source ON voiceover_assets(source_kind, source_ref);
