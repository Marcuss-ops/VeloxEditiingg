-- Migration 002: Legacy imports tracking and workers refinement
--
-- This migration adds:
--   1. legacy_imports table — idempotent, auditable tracking of JSON→SQLite imports
--   2. Additional columns on the workers table for richer worker state

-- ============================================================
-- legacy_imports: Tracks every JSON→SQLite import operation.
--   Prevents duplicate imports, enables auditing, and supports
--   dry-run verification. One row per (source, checksum, version).
-- ============================================================
CREATE TABLE IF NOT EXISTS legacy_imports (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    source_name      TEXT NOT NULL,          -- e.g. "workers", "youtube_channels", "ansible_hosts"
    source_path      TEXT NOT NULL,          -- absolute path of the JSON file
    source_sha256    TEXT NOT NULL,          -- SHA256 of the file at import time
    importer_version INTEGER NOT NULL DEFAULT 1,
    status           TEXT NOT NULL DEFAULT 'applied',  -- applied | rejected | rolled_back
    imported_rows    INTEGER NOT NULL DEFAULT 0,
    rejected_rows    INTEGER NOT NULL DEFAULT 0,
    conflict_rows    INTEGER NOT NULL DEFAULT 0,
    report_path      TEXT,                   -- path to conflict/diagnostic report
    error_message    TEXT,                   -- if status = 'rejected'
    imported_at      TEXT NOT NULL,
    UNIQUE(source_name, source_sha256, importer_version)
);

CREATE INDEX IF NOT EXISTS idx_legacy_imports_source ON legacy_imports(source_name);

-- ============================================================
-- Workers table: add richer state columns beyond raw_json
-- ============================================================
ALTER TABLE workers ADD COLUMN display_name TEXT NOT NULL DEFAULT '';
ALTER TABLE workers ADD COLUMN ip_address TEXT NOT NULL DEFAULT '';
ALTER TABLE workers ADD COLUMN first_seen TEXT;
ALTER TABLE workers ADD COLUMN current_job TEXT NOT NULL DEFAULT '';
ALTER TABLE workers ADD COLUMN code_version TEXT NOT NULL DEFAULT '';
ALTER TABLE workers ADD COLUMN bundle_version TEXT NOT NULL DEFAULT '';
ALTER TABLE workers ADD COLUMN bundle_hash TEXT NOT NULL DEFAULT '';
ALTER TABLE workers ADD COLUMN protocol_version TEXT NOT NULL DEFAULT '';
ALTER TABLE workers ADD COLUMN engine_version TEXT NOT NULL DEFAULT '';
ALTER TABLE workers ADD COLUMN recent_logs TEXT NOT NULL DEFAULT '[]';
ALTER TABLE workers ADD COLUMN recent_errors TEXT NOT NULL DEFAULT '[]';
ALTER TABLE workers ADD COLUMN readiness TEXT NOT NULL DEFAULT '{}';
ALTER TABLE workers ADD COLUMN metrics TEXT NOT NULL DEFAULT '{}';
ALTER TABLE workers ADD COLUMN capabilities TEXT NOT NULL DEFAULT '{}';
