-- Migration 009: DROP legacy tables (IRREVERSIBLE — apply only after verifying 008 data copy)
--
-- Drops legacy tables that have been replaced by canonical models.
-- Migration 008 already copied any remaining data from these tables
-- into the canonical tables. This migration is the final cleanup step.
--
-- Tables dropped:
--   - youtube_channel_metadata  → replaced by youtube_channels (003)
--   - youtube_groups (old)      → replaced by youtube_groups_v2 (003)
--   - youtube_manager_channels  → data moved to youtube_channels + youtube_group_channels (003)
--   - youtube_manager_groups    → replaced by youtube_groups_v2 (003)
--   - ansible_computers         → replaced by ansible_hosts (004)
--
-- WARNING: This migration is IRREVERSIBLE. Ensure that:
--   1. Migration 008 has been applied (data copied to canonical tables)
--   2. All services using legacy tables have been redeployed
--   3. Data integrity has been verified (counts match between legacy and canonical)
-- The legacy_imports table provides an audit trail for verification.

-- ============================================================
-- Phase 1: Drop YouTube legacy tables
-- ============================================================
DROP TABLE IF EXISTS youtube_channel_metadata;
DROP TABLE IF EXISTS youtube_groups;
DROP TABLE IF EXISTS youtube_manager_channels;
DROP TABLE IF EXISTS youtube_manager_groups;

-- ============================================================
-- Phase 2: Drop Ansible legacy table
-- ============================================================
DROP TABLE IF EXISTS ansible_computers;
