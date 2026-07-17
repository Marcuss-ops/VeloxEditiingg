-- Migration 010: Drop YouTube domain tables (Postgres dialect)
--
-- YouTube-specific storage (channels, groups, OAuth tokens, metrics,
-- video metadata) is moving to the Social service. This migration
-- removes the domain from Velox's Postgres schema.
--
-- WARNING: This migration is irreversible and destroys YouTube data
-- still stored in this database. Ensure any required data has been
-- migrated to the Social service before applying.

-- Drop tables in dependency order (children first)
DROP TABLE IF EXISTS youtube_videos CASCADE;
DROP TABLE IF EXISTS youtube_oauth_tokens CASCADE;
DROP TABLE IF EXISTS youtube_group_channels CASCADE;
DROP TABLE IF EXISTS youtube_groups CASCADE;
DROP TABLE IF EXISTS youtube_channels CASCADE;
