-- Migration 128: Drop the Dark Editor storage domain
--
-- Dark Editor (the browser editor) now lives outside Velox: the
-- frontend UI is served by VeloxFrontend and project/workspace/session
-- state is owned by InstaeditLogin. Velox receives only the canonical
-- render job. Its SQLite schema no longer needs the Dark Editor
-- tables.
--
-- WARNING: This migration is irreversible and destroys Dark Editor
-- data still stored in this database. Ensure any required data has
-- been migrated to the editor domain owner before applying.
--
-- Ordering follows the historical parent/child shape of the domain
-- (temp files and generations reference assets/projects conceptually);
-- no real FOREIGN KEY constraints exist between these tables.

-- ============================================================
-- Drop temp files (self-contained)
-- ============================================================
DROP TABLE IF EXISTS dark_editor_temp_files;

-- ============================================================
-- Drop generations (reference projects + assets)
-- ============================================================
DROP TABLE IF EXISTS dark_editor_generations;

-- ============================================================
-- Drop assets (reference projects)
-- ============================================================
DROP TABLE IF EXISTS dark_editor_assets;

-- ============================================================
-- Drop templates (standalone)
-- ============================================================
DROP TABLE IF EXISTS dark_editor_templates;

-- ============================================================
-- Drop projects (reference folders)
-- ============================================================
DROP TABLE IF EXISTS dark_editor_projects;

-- ============================================================
-- Drop folders last (parent/child shape)
-- ============================================================
DROP TABLE IF EXISTS dark_editor_folders;
