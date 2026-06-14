-- Migration 008: Data migration from legacy tables to canonical (SAFE UPGRADE)
--
-- This migration copies all remaining data from legacy tables into the
-- canonical models. It does NOT drop any tables — that happens in migration 009.
--
-- Legacy tables migrated:
--   - youtube_channel_metadata  → youtube_channels (003)
--   - youtube_groups (old)      → youtube_groups_v2 + youtube_group_channels (003)
--   - youtube_manager_channels  → youtube_channels + youtube_group_channels (003)
--   - youtube_manager_groups    → youtube_groups_v2 (003)
--   - ansible_computers         → ansible_hosts (004)
--
-- SAFE: This migration only INSERTs. No data is removed.
-- Rollback is safe by just deleting from canonical tables.

-- ============================================================
-- Phase 1: Migrate legacy youtube_channel_metadata → youtube_channels
-- ============================================================
INSERT OR IGNORE INTO youtube_channels (channel_id, title, display_name, language, added_at, last_sync_at, metadata_json, created_at, updated_at)
SELECT
    channel_id,
    COALESCE(title, ''),
    COALESCE(title, ''),
    COALESCE(language, ''),
    COALESCE(added_date, ''),
    COALESCE(last_used, ''),
    COALESCE(raw_json, '{}'),
    COALESCE(updated_at, datetime('now')),
    COALESCE(updated_at, datetime('now'))
FROM youtube_channel_metadata
WHERE channel_id NOT IN (SELECT channel_id FROM youtube_channels);

-- ============================================================
-- Phase 2: Migrate legacy youtube_groups → youtube_groups_v2 + youtube_group_channels
-- ============================================================
INSERT OR IGNORE INTO youtube_groups_v2 (name, group_type, description, privacy, created_at, updated_at)
SELECT
    g.name,
    'upload',
    COALESCE(g.description, ''),
    COALESCE(g.privacy, ''),
    COALESCE(g.updated_at, datetime('now')),
    datetime('now')
FROM youtube_groups g
WHERE g.name NOT IN (SELECT name FROM youtube_groups_v2 WHERE group_type='upload');

-- Link channels from legacy groups (channels_json is a JSON array of channel IDs)
INSERT OR IGNORE INTO youtube_group_channels (group_id, channel_id, position, added_at)
SELECT
    v2.id,
    ch.value,
    0,
    datetime('now')
FROM youtube_groups g,
     json_each(g.channels_json) AS ch,
     youtube_groups_v2 v2
WHERE v2.name = g.name AND v2.group_type = 'upload'
  AND NOT EXISTS (SELECT 1 FROM youtube_group_channels gc WHERE gc.group_id = v2.id AND gc.channel_id = ch.value);

-- ============================================================
-- Phase 3: Migrate legacy youtube_manager_channels → youtube_channels + youtube_group_channels
-- ============================================================
INSERT OR IGNORE INTO youtube_channels (channel_id, title, display_name, channel_url, thumbnail_url, language, view_count, subscriber_count, added_at, last_sync_at, metadata_json, created_at, updated_at)
SELECT
    mc.channel_id,
    COALESCE(mc.title, ''),
    COALESCE(mc.name, mc.title, ''),
    COALESCE(mc.url, ''),
    COALESCE(mc.thumbnail, ''),
    COALESCE(mc.language, ''),
    COALESCE(mc.view_count, 0),
    COALESCE(mc.sub_count, 0),
    COALESCE(mc.added_at, ''),
    COALESCE(mc.last_sync, ''),
    COALESCE(mc.raw_json, '{}'),
    COALESCE(mc.updated_at, datetime('now')),
    datetime('now')
FROM youtube_manager_channels mc
WHERE mc.channel_id NOT IN (SELECT channel_id FROM youtube_channels);

-- Ensure manager groups exist in youtube_groups_v2
INSERT OR IGNORE INTO youtube_groups_v2 (name, group_type, description, privacy, created_at, updated_at)
SELECT DISTINCT
    mc.group_name,
    'manager',
    '',
    '',
    datetime('now'),
    datetime('now')
FROM youtube_manager_channels mc
WHERE mc.group_name IS NOT NULL AND mc.group_name != ''
  AND mc.group_name NOT IN (SELECT name FROM youtube_groups_v2 WHERE group_type='manager');

-- Link manager channels to groups
INSERT OR IGNORE INTO youtube_group_channels (group_id, channel_id, position, added_at)
SELECT
    v2.id,
    mc.channel_id,
    0,
    datetime('now')
FROM youtube_manager_channels mc
JOIN youtube_groups_v2 v2 ON v2.name = mc.group_name AND v2.group_type = 'manager'
WHERE NOT EXISTS (SELECT 1 FROM youtube_group_channels gc WHERE gc.group_id = v2.id AND gc.channel_id = mc.channel_id);

-- ============================================================
-- Phase 4: Migrate legacy youtube_manager_groups → youtube_groups_v2
-- ============================================================
INSERT OR IGNORE INTO youtube_groups_v2 (name, group_type, description, privacy, created_at, updated_at)
SELECT
    mg.name,
    COALESCE(mg.group_type, 'manager'),
    '',
    '',
    COALESCE(mg.created_at, datetime('now')),
    datetime('now')
FROM youtube_manager_groups mg
WHERE mg.name NOT IN (SELECT name FROM youtube_groups_v2 WHERE group_type=COALESCE(mg.group_type, 'manager'));

-- ============================================================
-- Phase 5: Migrate legacy ansible_computers → ansible_hosts
-- ============================================================
INSERT OR IGNORE INTO ansible_hosts (host, ansible_user, enabled, created_at, updated_at)
SELECT
    c.host,
    'pierone',
    1,
    COALESCE(c.updated_at, datetime('now')),
    datetime('now')
FROM ansible_computers c
WHERE c.host NOT IN (SELECT host FROM ansible_hosts);

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
