-- Migration 008: DROP legacy tables and add CI guard
--
-- Drops legacy tables that have been replaced by canonical models:
--   - youtube_channel_metadata  → replaced by youtube_channels (003)
--   - youtube_groups (old)      → replaced by youtube_groups_v2 (003)
--   - youtube_manager_channels  → data moved to youtube_channels + youtube_group_channels (003)
--   - youtube_manager_groups    → replaced by youtube_groups_v2 (003)
--   - ansible_computers         → replaced by ansible_hosts (004)
--
-- WARNING: This migration is irreversible. Ensure data has been migrated
-- before applying. The legacy_imports table provides an audit trail.

-- ============================================================
-- Drop YouTube legacy tables
-- ============================================================
DROP TABLE IF EXISTS youtube_channel_metadata;
DROP TABLE IF EXISTS youtube_groups;
DROP TABLE IF EXISTS youtube_manager_channels;
DROP TABLE IF EXISTS youtube_manager_groups;

-- ============================================================
-- Drop Ansible legacy table
-- ============================================================
DROP TABLE IF EXISTS ansible_computers;

-- ============================================================
-- CI guard: registry of known legacy JSON paths
-- Used by CI scripts to detect if legacy persistence is reintroduced.
-- ============================================================
CREATE TABLE IF NOT EXISTS legacy_json_registry (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    json_path   TEXT NOT NULL UNIQUE,
    domain      TEXT NOT NULL,
    status      TEXT NOT NULL DEFAULT 'banned',  -- banned = must not exist
    banned_at   TEXT NOT NULL,
    notes       TEXT NOT NULL DEFAULT ''
);

-- Register all banned legacy JSON paths
INSERT OR IGNORE INTO legacy_json_registry (json_path, domain, status, banned_at, notes) VALUES
    ('workers.json', 'workers', 'banned', datetime('now'), 'Replaced by SQLite workers table'),
    ('youtube/youtube_manager.json', 'youtube', 'banned', datetime('now'), 'Replaced by youtube_channels + youtube_groups_v2'),
    ('youtube/groups.json', 'youtube', 'banned', datetime('now'), 'Replaced by youtube_groups_v2'),
    ('youtube/channels/channels.json', 'youtube', 'banned', datetime('now'), 'Replaced by youtube_channels'),
    ('youtube/GroupYoutubeManager/ChannelsSaved.json', 'youtube', 'banned', datetime('now'), 'Replaced by youtube_channels'),
    ('ansible/ansible_computers.json', 'ansible', 'banned', datetime('now'), 'Replaced by ansible_hosts'),
    ('ansible/ansible_runs.json', 'ansible', 'banned', datetime('now'), 'Replaced by ansible_runs + ansible_run_hosts'),
    ('analytics/feed_cache.json', 'cache', 'banned', datetime('now'), 'In-memory volatile cache only'),
    ('youtube/history/upload_history.json', 'youtube', 'banned', datetime('now'), 'Use SQLite-based tracking'),
    ('job_queue.json', 'queue', 'banned', datetime('now'), 'Replaced by SQLite job queue'),
    ('job_queue_recovered.json', 'queue', 'banned', datetime('now'), 'Replaced by SQLite job queue'),
    ('jobs_queue.json', 'queue', 'banned', datetime('now'), 'Replaced by SQLite job queue'),
    ('video_uploads.db', 'uploads', 'banned', datetime('now'), 'Replaced by SQLite');
