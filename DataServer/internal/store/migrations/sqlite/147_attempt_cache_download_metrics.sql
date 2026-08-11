-- Per-attempt asset download volume (Phase A1.5).
--
-- The CacheResolver (Phase A1) emits attempt-scoped download counters that
-- start from zero on every attempt. This migration hoists them onto
-- task_attempt_cache_stats (created in 054, extended in 140 with
-- cache_lookups + unique_assets_requested) so job certification and
-- per-attempt SQL queries can read download volume without walking the
-- dotted report map or notes JSON.
ALTER TABLE task_attempt_cache_stats ADD COLUMN cache_download_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE task_attempt_cache_stats ADD COLUMN cache_download_bytes INTEGER NOT NULL DEFAULT 0;
