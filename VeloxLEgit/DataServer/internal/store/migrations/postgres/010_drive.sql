-- Migration 010: drive (Postgres dialect)
--
-- Google Drive link source-of-truth table. Mirrors SQLite migration
-- 006_drive_links_source_of_truth.sql (the cumulative state after all
-- earlier 001-005 Drive-shaped things got merged into a single
-- canonical table).
--
-- The unique partial index on (folder_id, file_id) mirrors SQLite's
-- UNIQUE INDEX … WHERE file_id <> '' so draft rows with an empty
-- file_id don't collide during upload progress.

CREATE TABLE IF NOT EXISTS drive_links (
    link_id             TEXT PRIMARY KEY,
    folder_id           TEXT NOT NULL,
    file_id             TEXT,
    name                TEXT NOT NULL,
    mime_type           TEXT,
    size_bytes          BIGINT NOT NULL DEFAULT 0,
    source              TEXT NOT NULL,
    source_ref          TEXT,
    metadata_json       TEXT,
    created_at          TEXT NOT NULL,
    updated_at          TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_pg_drive_links_file
    ON drive_links(folder_id, file_id)
    WHERE file_id IS NOT NULL AND file_id <> '';
CREATE INDEX IF NOT EXISTS idx_pg_drive_links_folder ON drive_links(folder_id);
CREATE INDEX IF NOT EXISTS idx_pg_drive_links_source ON drive_links(source, source_ref);
