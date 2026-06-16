-- Migration 011: youtube_oauth_tokens (encrypted OAuth credentials)
--
-- YouTube OAuth secrets are now stored exclusively in this table.
-- Encryption at-rest is handled by the application layer using AES-256-GCM
-- via the internal/secrets/aesgcm package — the BLOB columns hold
-- `nonce(12 bytes) || ciphertext || gcm_tag(16 bytes)` produced by
-- cipher.NewGCM. The encryption key lives outside the database (env var
-- VELOX_YT_OAUTH_TOKEN_KEY or its _FILE sibling), so even a stolen
-- database file alone cannot recover usable credentials.
--
-- This migration is INSERT-only (no destructive work). The existing
-- account_*.json token files remain on disk for one release so installs
-- upgrading without the env var set can backfill by hand. New writes go
-- to this table; the JSON write path is still present in the code on a
-- deprecation trail (step 6 of the verdict's 12-step plan).
--
-- Relationships:
--   - channel_id PRIMARY KEY, FK cascade to youtube_channels
--   - revoked_at NULL while active, RFC3339 timestamp once revoked
--   - key_version lets us support rotation without destructive re-encrypt
--
-- Idempotent: CREATE TABLE IF NOT EXISTS / CREATE INDEX IF NOT EXISTS.

CREATE TABLE IF NOT EXISTS youtube_oauth_tokens (
    channel_id              TEXT PRIMARY KEY,
    access_token_encrypted  BLOB NOT NULL,
    refresh_token_encrypted BLOB,
    token_type              TEXT NOT NULL DEFAULT 'Bearer',
    expiry                  TEXT,
    scopes                  TEXT NOT NULL DEFAULT '',
    key_version             INTEGER NOT NULL DEFAULT 1,
    revoked_at              TEXT,
    created_at              TEXT NOT NULL,
    updated_at              TEXT NOT NULL,
    FOREIGN KEY (channel_id) REFERENCES youtube_channels(channel_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_yt_oauth_tokens_revoked ON youtube_oauth_tokens(revoked_at);
CREATE INDEX IF NOT EXISTS idx_yt_oauth_tokens_key_version ON youtube_oauth_tokens(key_version);
