-- Migration 009: youtube (Postgres dialect)
--
-- YouTube domain canonical tables: channel + group registry, membership
-- links, OAuth token storage, video metadata. Mirrors SQLite migrations
-- 003_youtube_canonical.sql, 011_youtube_oauth_tokens.sql,
-- 012_youtube_groups_rename.sql, plus an additional youtube_videos
-- mirror that the SQLite cumulative path kept implicit through
-- analytics_cache rows.
--
-- FK strategy: group_channels links groups + channels with ON DELETE
-- CASCADE so removing a channel cleans up its group memberships.

CREATE TABLE IF NOT EXISTS youtube_channels (
    channel_id          TEXT PRIMARY KEY,
    title               TEXT NOT NULL,
    display_name        TEXT,
    language            TEXT,
    view_count          BIGINT NOT NULL DEFAULT 0,
    subscriber_count    BIGINT NOT NULL DEFAULT 0,
    created_at          TEXT NOT NULL,
    updated_at          TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS youtube_groups (
    id                  BIGSERIAL PRIMARY KEY,
    name                TEXT NOT NULL,
    group_type          TEXT NOT NULL,
    description         TEXT,
    created_at          TEXT NOT NULL,
    updated_at          TEXT NOT NULL,
    UNIQUE(name, group_type)
);

CREATE TABLE IF NOT EXISTS youtube_group_channels (
    group_id            BIGINT NOT NULL REFERENCES youtube_groups(id) ON DELETE CASCADE,
    channel_id          TEXT NOT NULL REFERENCES youtube_channels(channel_id) ON DELETE CASCADE,
    position            INTEGER NOT NULL DEFAULT 0,
    added_at            TEXT NOT NULL,
    PRIMARY KEY (group_id, channel_id)
);

CREATE TABLE IF NOT EXISTS youtube_oauth_tokens (
    channel_id          TEXT PRIMARY KEY REFERENCES youtube_channels(channel_id) ON DELETE CASCADE,
    access_token        TEXT NOT NULL,
    refresh_token       TEXT,
    token_type          TEXT NOT NULL DEFAULT 'Bearer',
    scope               TEXT,
    expiry              TEXT NOT NULL,
    refresh_expiry      TEXT,
    updated_at          TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS youtube_videos (
    video_id            TEXT PRIMARY KEY,
    channel_id          TEXT NOT NULL REFERENCES youtube_channels(channel_id) ON DELETE CASCADE,
    title               TEXT NOT NULL,
    description         TEXT,
    published_at        TEXT,
    duration_seconds    INTEGER NOT NULL DEFAULT 0,
    view_count          BIGINT NOT NULL DEFAULT 0,
    like_count          BIGINT NOT NULL DEFAULT 0,
    privacy_status      TEXT NOT NULL DEFAULT 'private',
    upload_status       TEXT NOT NULL DEFAULT 'pending',
    created_at          TEXT NOT NULL,
    updated_at          TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_pg_youtube_videos_channel ON youtube_videos(channel_id);
CREATE INDEX IF NOT EXISTS idx_pg_youtube_videos_published ON youtube_videos(published_at);
