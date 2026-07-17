-- Migration 090: Drop YouTube domain tables and columns
--
-- YouTube-specific storage (channels, groups, OAuth tokens, metrics,
-- API cache) is moving to the Social service. This migration removes
-- the domain from Velox's SQLite schema.
--
-- WARNING: This migration is irreversible and destroys YouTube data
-- still stored in this database. Ensure any required data has been
-- migrated to the Social service before applying.

-- ============================================================
-- Drop OAuth tokens first (foreign key to youtube_channels)
-- ============================================================
DROP TABLE IF EXISTS youtube_oauth_tokens;

-- ============================================================
-- Drop group membership next (foreign keys to groups + channels)
-- ============================================================
DROP TABLE IF EXISTS youtube_group_channels;

-- ============================================================
-- Drop remaining YouTube domain tables
-- ============================================================
DROP TABLE IF EXISTS youtube_groups;
DROP TABLE IF EXISTS youtube_channels;
DROP TABLE IF EXISTS youtube_tracked_niches;

-- ============================================================
-- Drop YouTube metrics / cache tables
-- ============================================================
DROP TABLE IF EXISTS youtube_channel_metrics;
DROP TABLE IF EXISTS youtube_revenue_metrics;
DROP TABLE IF EXISTS youtube_video_metrics;
DROP TABLE IF EXISTS youtube_quota_usage;
DROP TABLE IF EXISTS youtube_api_cache;

-- ============================================================
-- Remove YouTube columns from domain tables
-- SQLite >= 3.35.0 supports DROP COLUMN (bundled via go-sqlite3 v1.14.15+)
-- ============================================================
DROP INDEX IF EXISTS idx_yt_oauth_tokens_revoked;
DROP INDEX IF EXISTS idx_yt_oauth_tokens_key_version;
DROP INDEX IF EXISTS idx_yt_group_channels_group;
DROP INDEX IF EXISTS idx_yt_group_channels_channel;
DROP INDEX IF EXISTS idx_yt_groups_v2_name;
DROP INDEX IF EXISTS idx_yt_groups_v2_type;
DROP INDEX IF EXISTS idx_yt_groups_name;
DROP INDEX IF EXISTS idx_yt_groups_type;
DROP INDEX IF EXISTS idx_youtube_api_cache_ts;

ALTER TABLE calendar_events DROP COLUMN youtube_group;
ALTER TABLE calendar_events DROP COLUMN youtube_links_json;
ALTER TABLE dark_editor_folders DROP COLUMN youtube_group;
