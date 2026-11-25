-- Preserve byte-movement and hash-verification durations reported by workers.
-- Older workers keep zero defaults and remain wire compatible.
ALTER TABLE worker_asset_downloads
    ADD COLUMN download_work_ms INTEGER NOT NULL DEFAULT 0;

ALTER TABLE worker_asset_downloads
    ADD COLUMN hash_verify_ms INTEGER NOT NULL DEFAULT 0;
