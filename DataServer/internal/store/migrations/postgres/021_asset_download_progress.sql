-- Migration 021: latest worker asset-download progress read model.
-- One physical transfer is shared across jobs; job_asset_refs projects that
-- transfer into every job that currently references it.

CREATE TABLE IF NOT EXISTS worker_asset_downloads (
    worker_id                    TEXT NOT NULL,
    asset_key                    TEXT NOT NULL,
    transfer_id                  TEXT NOT NULL DEFAULT '',
    asset_id                     TEXT NOT NULL DEFAULT '',
    role                         TEXT NOT NULL DEFAULT '',
    state                        TEXT NOT NULL DEFAULT '',
    bytes_downloaded            BIGINT NOT NULL DEFAULT 0,
    bytes_total                 BIGINT NOT NULL DEFAULT 0,
    bytes_per_second             DOUBLE PRECISION NOT NULL DEFAULT 0,
    eta_seconds                 BIGINT NOT NULL DEFAULT 0,
    attempt                     INTEGER NOT NULL DEFAULT 0,
    shared_waiters              INTEGER NOT NULL DEFAULT 0,
    cache_hit                   BOOLEAN NOT NULL DEFAULT FALSE,
    queued_at                   TEXT NOT NULL DEFAULT '',
    started_at                  TEXT NOT NULL DEFAULT '',
    updated_at                  TEXT NOT NULL DEFAULT '',
    completed_at                TEXT NOT NULL DEFAULT '',
    task_id                     TEXT NOT NULL DEFAULT '',
    scene_ids_json              TEXT NOT NULL DEFAULT '[]',
    mime_type                   TEXT NOT NULL DEFAULT '',
    sha256                      TEXT NOT NULL DEFAULT '',
    error_code                  TEXT NOT NULL DEFAULT '',
    error_detail                TEXT NOT NULL DEFAULT '',
    checkpoint_sequence         BIGINT NOT NULL DEFAULT 0,
    transfer_generation         BIGINT NOT NULL DEFAULT 0,
    received_at                 TEXT NOT NULL,
    PRIMARY KEY (worker_id, asset_key)
);

CREATE INDEX IF NOT EXISTS idx_worker_asset_downloads_worker_state
    ON worker_asset_downloads(worker_id, state);
CREATE INDEX IF NOT EXISTS idx_worker_asset_downloads_updated
    ON worker_asset_downloads(updated_at);

CREATE TABLE IF NOT EXISTS job_asset_refs (
    job_id                      TEXT NOT NULL,
    worker_id                   TEXT NOT NULL,
    asset_key                   TEXT NOT NULL,
    transfer_id                 TEXT NOT NULL DEFAULT '',
    asset_id                    TEXT NOT NULL DEFAULT '',
    role                        TEXT NOT NULL DEFAULT '',
    scene_ids_json              TEXT NOT NULL DEFAULT '[]',
    task_id                     TEXT NOT NULL DEFAULT '',
    created_at                  TEXT NOT NULL,
    PRIMARY KEY (job_id, worker_id, asset_key)
);

CREATE INDEX IF NOT EXISTS idx_job_asset_refs_job
    ON job_asset_refs(job_id);
CREATE INDEX IF NOT EXISTS idx_job_asset_refs_transfer
    ON job_asset_refs(worker_id, asset_key, transfer_id);
