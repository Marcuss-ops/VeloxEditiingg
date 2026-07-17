-- Migration 091: externalize destination identity (Residuo 2 of YouTube→Social closure)
--
-- Opaque-mode destination: drop YouTube-specific columns (`account_id`,
-- `channel_id`, `language`) from `delivery_destinations` and add the
-- opaque `social_destination_id` column that resolves to
-- (platform, account, channel, language, credentials) server-side via
-- the external Social API.
--
-- Forward-only (no DOWN): existing rows with non-null
-- account_id / channel_id / language lose those values. Operators must
-- export any still-needed data via the Velox → Social migration before
-- applying this migration. Velox no longer recognises any of those
-- three columns; their absence is a feature, not a bug.
--
-- SQLite >= 3.35.0 supports ALTER TABLE … DROP COLUMN (bundled via
-- go-sqlite3 v1.14.15+, which Velox already uses — see migration 090).

-- ============================================================
-- Step 1: Add the opaque Social-API reference column.
-- TEXT, no NOT NULL, no DEFAULT — operators must populate it
-- explicitly per row before the runner can dispatch. Empty / NULL
-- means "unmapped": the runner MUST fail-closed (ErrNotConfigured)
-- on dispatch, never silently fall back to the old columns.
-- ============================================================

ALTER TABLE delivery_destinations
    ADD COLUMN social_destination_id TEXT;

-- ============================================================
-- Step 2: Drop the three YouTube-specific columns. Order is
-- inconsequential (no FK, no index relies on any of them — see
-- migration 022 for the indexes; they only cover `provider` and
-- `enabled`).
-- ============================================================

ALTER TABLE delivery_destinations DROP COLUMN account_id;
ALTER TABLE delivery_destinations DROP COLUMN channel_id;
ALTER TABLE delivery_destinations DROP COLUMN language;
