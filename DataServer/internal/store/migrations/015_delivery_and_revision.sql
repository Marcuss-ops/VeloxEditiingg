-- Migration 015: Add revision column for CAS (compare-and-swap) job transitions
--
-- Enables optimistic locking: every status transition must match the current
-- revision, preventing two workers from completing the same job concurrently.
--
-- Safe to apply: ADD COLUMN with DEFAULT is instant in SQLite.

ALTER TABLE jobs ADD COLUMN revision INTEGER NOT NULL DEFAULT 0;
