-- Cache accounting needed to distinguish repeated lookups from distinct
-- requested assets. Some legacy fixtures reach this migration without the
-- metrics table, so establish the base shape before adding the new columns.
CREATE TABLE IF NOT EXISTS task_attempt_cache_stats (
    attempt_id          TEXT PRIMARY KEY,
    cache_hits          INTEGER NOT NULL DEFAULT 0,
    cache_misses        INTEGER NOT NULL DEFAULT 0,
    cache_evictions     INTEGER NOT NULL DEFAULT 0,
    cache_corruptions   INTEGER NOT NULL DEFAULT 0,
    cache_bytes_used    INTEGER NOT NULL DEFAULT 0,
    cache_entries       INTEGER NOT NULL DEFAULT 0
);
ALTER TABLE task_attempt_cache_stats ADD COLUMN cache_lookups INTEGER NOT NULL DEFAULT 0;
ALTER TABLE task_attempt_cache_stats ADD COLUMN unique_assets_requested INTEGER NOT NULL DEFAULT 0;
