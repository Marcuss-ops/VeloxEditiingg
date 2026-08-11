-- Migration 143: canonical asset media metadata
--
-- asset_media_metadata: registry-authoritative media description of an
-- asset, captured ONCE at ingestion by the canonical MediaMetadataResolver
-- (a single ffprobe extraction). Job-time consumers MUST read these
-- columns from the registry instead of spawning their own ffprobe.
--
-- The row is optional: only media assets (video/* or audio/* MIME) get a
-- row. Presence of a row with a non-empty metadata_verified_at AND the
-- canonical metadata_schema_version IS the authoritative
-- "metadata_verified=true" signal; an absent row means
-- metadata_verified=false. blob_sha256 and size_bytes are already
-- authoritative on the assets table and are deliberately NOT duplicated
-- here.
CREATE TABLE IF NOT EXISTS asset_media_metadata (
    asset_id             TEXT PRIMARY KEY,

    container            TEXT NOT NULL DEFAULT '',
    duration_ms          INTEGER NOT NULL DEFAULT 0,

    video_codec          TEXT NOT NULL DEFAULT '',
    pix_fmt              TEXT NOT NULL DEFAULT '',
    width                INTEGER NOT NULL DEFAULT 0,
    height               INTEGER NOT NULL DEFAULT 0,
    fps_num              INTEGER NOT NULL DEFAULT 0,
    fps_den              INTEGER NOT NULL DEFAULT 0,
    time_base_num        INTEGER NOT NULL DEFAULT 0,
    time_base_den        INTEGER NOT NULL DEFAULT 0,

    audio_codec          TEXT NOT NULL DEFAULT '',
    audio_sample_rate    INTEGER NOT NULL DEFAULT 0,
    audio_channels       INTEGER NOT NULL DEFAULT 0,

    metadata_verified_at TEXT,
    metadata_schema_version INTEGER NOT NULL DEFAULT 1,

    FOREIGN KEY (asset_id) REFERENCES assets(asset_id),
    CHECK (metadata_schema_version = 1)
);

CREATE INDEX IF NOT EXISTS idx_asset_media_metadata_verified
    ON asset_media_metadata(metadata_verified_at);
