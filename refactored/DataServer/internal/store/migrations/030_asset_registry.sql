-- Migration 029: Generic asset registry
--
-- assets: central content-addressed asset store (replaces voiceover-only bridge)
-- asset_sources: provenance tracking (where each asset came from)
-- job_assets: per-job asset bindings with role + ordinal

-- ============================================================
-- assets: content-addressed asset registry
-- ============================================================
CREATE TABLE IF NOT EXISTS assets (
    asset_id         TEXT PRIMARY KEY,
    kind             TEXT NOT NULL,
    status           TEXT NOT NULL,

    sha256           TEXT NOT NULL,
    mime_type        TEXT,
    size_bytes       INTEGER NOT NULL,

    storage_provider TEXT NOT NULL,
    storage_key      TEXT NOT NULL,

    metadata_json    TEXT,

    created_at       TEXT NOT NULL,
    verified_at      TEXT,
    deleted_at       TEXT,

    UNIQUE(sha256),
    UNIQUE(storage_provider, storage_key),

    CHECK (
        status IN (
            'STAGING',
            'READY',
            'QUARANTINED',
            'DELETED'
        )
    )
);

CREATE INDEX IF NOT EXISTS idx_assets_kind ON assets(kind);
CREATE INDEX IF NOT EXISTS idx_assets_status ON assets(status);
CREATE INDEX IF NOT EXISTS idx_assets_sha256 ON assets(sha256);

-- ============================================================
-- asset_sources: provenance for each asset
-- ============================================================
CREATE TABLE IF NOT EXISTS asset_sources (
    source_id         TEXT PRIMARY KEY,
    asset_id          TEXT NOT NULL,

    source_type       TEXT NOT NULL,
    source_reference  TEXT NOT NULL,
    source_account_id TEXT,
    metadata_json     TEXT,

    created_at        TEXT NOT NULL,

    FOREIGN KEY (asset_id) REFERENCES assets(asset_id)
);

CREATE INDEX IF NOT EXISTS idx_asset_sources_asset_id ON asset_sources(asset_id);

-- ============================================================
-- job_assets: per-job asset bindings
-- ============================================================
CREATE TABLE IF NOT EXISTS job_assets (
    job_id     TEXT NOT NULL,
    asset_id   TEXT NOT NULL,
    role       TEXT NOT NULL,
    ordinal    INTEGER NOT NULL DEFAULT 0,
    required   INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,

    PRIMARY KEY (job_id, role, ordinal),
    FOREIGN KEY (job_id) REFERENCES jobs(job_id),
    FOREIGN KEY (asset_id) REFERENCES assets(asset_id)
);

CREATE INDEX IF NOT EXISTS idx_job_assets_job_id ON job_assets(job_id);
CREATE INDEX IF NOT EXISTS idx_job_assets_asset_id ON job_assets(asset_id);
