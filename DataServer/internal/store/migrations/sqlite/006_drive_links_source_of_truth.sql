-- Migration 006: Drive links - make SQLite the source of truth
--
-- Enhances the drive_links table with additional columns and creates
-- a dedicated table for master folders. After this migration, JSON files
-- (drive_links.json, drive_master_folders_list.json) become legacy backups only.

-- ============================================================
-- Enhance drive_links with is_master and subfolders_count
-- ============================================================
ALTER TABLE drive_links ADD COLUMN is_master INTEGER NOT NULL DEFAULT 0;
ALTER TABLE drive_links ADD COLUMN subfolders_count INTEGER NOT NULL DEFAULT 0;
-- updated_at already exists from 001_initial.sql, skip ALTER

-- ============================================================
-- drive_master_folders: structured master folders
-- ============================================================
CREATE TABLE IF NOT EXISTS drive_master_folders (
    id               TEXT PRIMARY KEY,
    name             TEXT NOT NULL DEFAULT '',
    url              TEXT NOT NULL DEFAULT '',
    subfolders_count INTEGER NOT NULL DEFAULT 0,
    language         TEXT NOT NULL DEFAULT '',
    created_at       TEXT NOT NULL DEFAULT '',
    updated_at       TEXT NOT NULL DEFAULT '',
    metadata_json    TEXT NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_drive_mf_language ON drive_master_folders(language);
