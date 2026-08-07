-- Migration 023: Drop YouTube domain tables (Postgres dialect)
--
-- NOTE: renumbered from 010 to 023 (2026-08-07). The original
-- postgres/010_drop_youtube_domain.sql collided with
-- postgres/010_drive.sql on version 010, which makes the runner's
-- duplicate-version discovery fail closed and the drop unreachable.
-- The postgres chain is test-only today (runtime postgres is
-- rejected in bootstrap), so no deployed schema_migrations row ever
-- recorded version 10 as this migration; renumbering is safe.
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
