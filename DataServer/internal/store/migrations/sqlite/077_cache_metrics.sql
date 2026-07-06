-- 077_cache_metrics.sql
--
-- Step 16 / Scorecard v2: cache metrics refinement on task_attempt_metrics.
-- Granular cache hit/miss counters per cache tier so operators can
-- compute tier-specific hit ratios for asset, blob, and render caches.
--
-- Columns:
--   asset_cache_hit_count  — asset (media) cache hits per attempt
--   asset_cache_miss_count — asset (media) cache misses per attempt
--   blob_cache_hit_count   — blobstore-object cache hits per attempt
--   blob_cache_miss_count  — blobstore-object cache misses per attempt
--   render_cache_hit_count — render-output cache hits per attempt
--
-- All DEFAULT 0 so older workers that don't emit these fields
-- continue to function without a code change.

ALTER TABLE task_attempt_metrics
    ADD COLUMN asset_cache_hit_count   INTEGER NOT NULL DEFAULT 0;
ALTER TABLE task_attempt_metrics
    ADD COLUMN asset_cache_miss_count  INTEGER NOT NULL DEFAULT 0;
ALTER TABLE task_attempt_metrics
    ADD COLUMN blob_cache_hit_count    INTEGER NOT NULL DEFAULT 0;
ALTER TABLE task_attempt_metrics
    ADD COLUMN blob_cache_miss_count   INTEGER NOT NULL DEFAULT 0;
ALTER TABLE task_attempt_metrics
    ADD COLUMN render_cache_hit_count  INTEGER NOT NULL DEFAULT 0;
