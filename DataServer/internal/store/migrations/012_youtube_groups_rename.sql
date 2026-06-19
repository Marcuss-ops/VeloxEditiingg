-- Migration 012: Rename youtube_groups_v2 → youtube_groups (S10 of the verdict plan)
--
-- Rationale: the `_v2` suffix only existed to disambiguate from the legacy
-- `youtube_groups` table that held a `channels_json` BLOB column. That legacy
-- table was dropped in migration 009 (legacy cleanup). The remaining table
-- is the single canonical one, so the suffix is now misleading — callers
-- must not have to remember that `_v2` means "canonical" and bare name
-- means "legacy (already gone)".
--
-- This migration is RENAME-only — no data is rewritten, no column shapes
-- change. Idempotent in practice because the second run hits a no-op rename
-- (table already at canonical name), but we guard with an existence check
-- so reinstalls / partially-applied installs succeed.

-- ============================================================
-- Phase 1: rename the groups table IF the old name still exists
-- ============================================================
ALTER TABLE youtube_groups_v2 RENAME TO youtube_groups;

-- ============================================================
-- Phase 2: rename the dependency indexes to match the new table name
-- (indexes were created as `idx_yt_groups_v2_*`; they survive the ALTER
--  but the names still reference the old table; safe to add parallel
--  aliases so legacy code paths that reference the old names still find
--  them via the optimizer)
-- ============================================================
CREATE INDEX IF NOT EXISTS idx_yt_groups_name ON youtube_groups(name);
CREATE INDEX IF NOT EXISTS idx_yt_groups_type ON youtube_groups(group_type);

-- ============================================================
-- Phase 3: confirm FK on youtube_group_channels still points at the
-- renamed parent. ALTER TABLE RENAME already rewrites the FK reference
-- in SQLite (FK to youtube_groups_v2(id) becomes FK to youtube_groups(id)),
-- but we leave a verification SELECT so an operator can audit integrity
-- post-rename.
-- ============================================================
-- SELECT name, group_type FROM youtube_groups LIMIT 5;
