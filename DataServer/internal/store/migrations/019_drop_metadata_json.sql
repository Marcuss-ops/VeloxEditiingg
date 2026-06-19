-- Migration 014: DROP COLUMN metadata_json (S8 of the verdict plan)
--
-- IRREVERSIBLE — apply only after migration 013's backfill has normalised
-- every row's metadata_json to '{}' (no data is lost in this DROP because
-- the blob's only legacy field, `token_path`, has no typed column and was
-- already unrecoverably stale post-S6).
--
-- Google's ALTER TABLE ... DROP COLUMN support landed in SQLite 3.35.0
-- (2021-03-12). This migration deliberately uses DROP COLUMN rather than
-- a shadow-table copy because the column is NOT NULL DEFAULT '{}' and
-- contains no fields the application reads.
--
-- Why this is the final step:
--   S7 (migration 013) emptied the column to '{}'.
--   This migration removes the column from the schema entirely.
--   S12 (CI guard) forbids any future `_json` writes to youtube_channels.
--   Future metadata needs must be expressed as a typed column.

ALTER TABLE youtube_channels DROP COLUMN metadata_json;
