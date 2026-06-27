-- 036_artifact_upload_chunks.sql — PR chunked upload persistence
--
-- Rationale:
--   * artifact_upload_chunks tracks individual chunks of a chunked upload
--     session so the master survives restarts mid-upload.
--   * Each chunk is stored in blob store staging as a separate file;
--     the storage_key column records the path.
--   * The PRIMARY KEY (upload_id, chunk_index) prevents duplicate chunk
--     writes and makes resume trivial (SELECT chunk_index WHERE upload_id = ?).
--   * The FOREIGN KEY references artifact_uploads so orphan cleanup is
--     cascade-delete when the session is expired/deleted.
--
-- DOWN-MIGRATION NOTE: not provided. Drop and backfill from backup.

CREATE TABLE IF NOT EXISTS artifact_upload_chunks (
    upload_id    TEXT NOT NULL,
    chunk_index  INTEGER NOT NULL,
    size_bytes   INTEGER NOT NULL DEFAULT 0,
    sha256       TEXT,
    storage_key  TEXT NOT NULL,
    received_at  TEXT NOT NULL,

    PRIMARY KEY (upload_id, chunk_index),
    FOREIGN KEY (upload_id) REFERENCES artifact_uploads(upload_id)
);

-- Resume / state query: which chunks are present for a given upload.
CREATE INDEX IF NOT EXISTS idx_artifact_upload_chunks_upload
    ON artifact_upload_chunks(upload_id, chunk_index);
