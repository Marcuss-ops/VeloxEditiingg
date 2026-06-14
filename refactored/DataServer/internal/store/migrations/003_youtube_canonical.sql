-- Migration 003: YouTube canonical catalog model
--
-- Unifies the 4 legacy tables (youtube_channel_metadata, youtube_groups,
-- youtube_manager_channels, youtube_manager_groups) into a single canonical model
-- with proper foreign keys.
--
-- New tables:
--   youtube_channels   — Single source of truth for channel metadata
--   youtube_groups     — Named collections of channels (upload groups, manager groups)
--   youtube_group_channels — Many-to-many membership with position
--   youtube_tracked_niches — Tracked niche keywords
--
-- Legacy tables are NOT dropped in this migration. They will be removed
-- in a later cleanup migration once data validation is complete.

-- ============================================================
-- youtube_channels: canonical channel metadata
-- ============================================================
CREATE TABLE IF NOT EXISTS youtube_channels (
    channel_id       TEXT PRIMARY KEY,
    title            TEXT NOT NULL DEFAULT '',
    display_name     TEXT NOT NULL DEFAULT '',
    channel_url      TEXT NOT NULL DEFAULT '',
    thumbnail_url    TEXT NOT NULL DEFAULT '',
    language         TEXT NOT NULL DEFAULT '',
    notes            TEXT NOT NULL DEFAULT '',
    view_count       INTEGER NOT NULL DEFAULT 0,
    subscriber_count INTEGER NOT NULL DEFAULT 0,
    added_at         TEXT,
    last_sync_at     TEXT,
    metadata_json    TEXT NOT NULL DEFAULT '{}',
    created_at       TEXT NOT NULL,
    updated_at       TEXT NOT NULL
);

-- ============================================================
-- youtube_groups: named channel groups
-- ============================================================
CREATE TABLE IF NOT EXISTS youtube_groups_v2 (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT NOT NULL,
    group_type  TEXT NOT NULL DEFAULT 'manager',
    description TEXT NOT NULL DEFAULT '',
    privacy     TEXT NOT NULL DEFAULT '',
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL,
    UNIQUE(name, group_type)
);

CREATE INDEX IF NOT EXISTS idx_yt_groups_v2_name ON youtube_groups_v2(name);
CREATE INDEX IF NOT EXISTS idx_yt_groups_v2_type ON youtube_groups_v2(group_type);

-- ============================================================
-- youtube_group_channels: many-to-many membership
-- ============================================================
CREATE TABLE IF NOT EXISTS youtube_group_channels (
    group_id   INTEGER NOT NULL,
    channel_id TEXT NOT NULL,
    position   INTEGER NOT NULL DEFAULT 0,
    added_at   TEXT NOT NULL,
    PRIMARY KEY (group_id, channel_id),
    FOREIGN KEY (group_id)   REFERENCES youtube_groups_v2(id) ON DELETE CASCADE,
    FOREIGN KEY (channel_id) REFERENCES youtube_channels(channel_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_yt_group_channels_group ON youtube_group_channels(group_id);
CREATE INDEX IF NOT EXISTS idx_yt_group_channels_channel ON youtube_group_channels(channel_id);

-- ============================================================
-- youtube_tracked_niches
-- ============================================================
CREATE TABLE IF NOT EXISTS youtube_tracked_niches (
    niche      TEXT PRIMARY KEY,
    created_at TEXT NOT NULL
);
