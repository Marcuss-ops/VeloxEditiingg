-- The fixture migration chain is intentionally sparse and does not include
-- the production migration that first creates task_attempt_cache_stats.
-- Create the complete fixture shape here; production/sqlite/140 upgrades the
-- existing table with ALTER TABLE instead.
CREATE TABLE IF NOT EXISTS task_attempt_cache_stats (
    attempt_id          TEXT PRIMARY KEY,
    cache_hits          INTEGER NOT NULL DEFAULT 0,
    cache_misses        INTEGER NOT NULL DEFAULT 0,
    cache_evictions     INTEGER NOT NULL DEFAULT 0,
    cache_corruptions   INTEGER NOT NULL DEFAULT 0,
    cache_bytes_used    INTEGER NOT NULL DEFAULT 0,
    cache_entries       INTEGER NOT NULL DEFAULT 0,
    cache_lookups       INTEGER NOT NULL DEFAULT 0,
    unique_assets_requested INTEGER NOT NULL DEFAULT 0
);
