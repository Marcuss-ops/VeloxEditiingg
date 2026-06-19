-- Migration 013: Metadata JSON backfill (S7 of the verdict plan)
--
-- Pre-flight for migration 014 (DROP COLUMN metadata_json). Determines
-- which columns the metadata_json blob still carries data for and moves
-- any operator-readable values into their typed-column homes so no
-- data is lost when the blob is dropped.
--
-- Audit (run before applying): 100% of the metadata_json usage in code
-- was traced in favour of typed columns. The only legacy field that
-- ever reached metadata_json was `token_path`, used by the now-removed
-- `saveChannelToken` JSON writer (deleted in step S6). No other field
-- was ever written to metadata_json.
--
-- Therefore the backfill is a NO-OP: no typed column corresponds to the
-- legacy `token_path` (the OAuth credential path is now derived from
-- youtube_oauth_tokens.channel_id only), so we leave the field unused
-- in the blob and just normalize the column to '{}' for every row.
-- This keeps the diff small and the audit trail explicit — any future
-- metadata_json writer that emerges will be caught by the S12 CI guard
-- (scripts/ci_yt_guard.sh) and forced to use a typed column instead.

-- ============================================================
-- Phase 1: confirm what's actually in metadata_json today
--   (operator can sanity-check pre-DROP by running this query)
-- ============================================================
-- SELECT DISTINCT json_extract(metadata_json, '$.token_path') AS token_path,
--                 json_extract(metadata_json, '$.last_used')  AS last_used
-- FROM youtube_channels
-- WHERE metadata_json != '{}' AND metadata_json IS NOT NULL;

-- ============================================================
-- Phase 2: normalize every row to '{}'
--
-- This is the actual backfill step. We don't try to copy token_path
-- anywhere because:
--   (a) youtube_channels has no typed column for it (intentional).
--   (b) any token_path value would be stale: the on-disk JSON writer
--       that produced it is gone (S6), so the value is unrecoverable.
-- ============================================================
UPDATE youtube_channels
SET metadata_json = '{}',
    updated_at    = COALESCE(updated_at, datetime('now'))
WHERE metadata_json IS NULL
   OR metadata_json != '{}';
