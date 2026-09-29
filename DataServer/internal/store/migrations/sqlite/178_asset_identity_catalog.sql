CREATE TABLE IF NOT EXISTS asset_identity_catalog (
    asset_id      TEXT PRIMARY KEY,
    sha256        TEXT NOT NULL CHECK(length(sha256) = 64),
    size_bytes    INTEGER NOT NULL CHECK(size_bytes > 0),
    verified_at   TEXT NOT NULL,
    worker_id     TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_asset_identity_catalog_sha256
    ON asset_identity_catalog(sha256);
