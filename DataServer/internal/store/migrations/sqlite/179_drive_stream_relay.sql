-- Durable state for the optional master-side Drive stream relay.
-- session_uri is a bearer capability for one Drive resumable upload; it is
-- kept only in the private server database and is never sent to a worker.
CREATE TABLE IF NOT EXISTS drive_relay_sessions (
    upload_id       TEXT NOT NULL,
    artifact_id     TEXT NOT NULL,
    job_id          TEXT NOT NULL,
    destination_id  TEXT NOT NULL,
    publication_id  TEXT NOT NULL DEFAULT '',
    folder_id       TEXT NOT NULL,
    session_uri     TEXT NOT NULL,
    expires_at      TEXT NOT NULL,
    part_size       INTEGER NOT NULL,
    next_offset     INTEGER NOT NULL DEFAULT 0,
    in_flight       INTEGER NOT NULL DEFAULT 0,
    state           TEXT NOT NULL DEFAULT 'PREPARED',
    remote_id       TEXT,
    remote_url      TEXT,
    verified_sha256 TEXT,
    verified_size   INTEGER,
    last_error      TEXT,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL,
    PRIMARY KEY (upload_id, destination_id, publication_id)
);

CREATE INDEX IF NOT EXISTS idx_drive_relay_artifact_destination
    ON drive_relay_sessions(artifact_id, destination_id, publication_id, state);
