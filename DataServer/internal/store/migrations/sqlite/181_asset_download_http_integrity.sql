-- Persist the final HTTP response and SHA-256 verification outcome for each
-- canonical worker asset transfer. Older workers leave these at defaults.
ALTER TABLE worker_asset_downloads
    ADD COLUMN http_status INTEGER NOT NULL DEFAULT 0;

ALTER TABLE worker_asset_downloads
    ADD COLUMN hash_verified INTEGER NOT NULL DEFAULT 0;
