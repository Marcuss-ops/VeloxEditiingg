-- 050_task_lease_expires_backfill
--
-- PR-05 stage-2: idempotent UPDATE backfill for pre-049 LEASED/RUNNING rows.
--
-- Migration 049 added `lease_expires_at TEXT` (nullable) to `tasks`. New claims
-- populate it via ClaimNextReadyTask's `now + 30min` write. Pre-049 rows
-- inserted with the column dropped (i.e. NULL) were intentionally treated by
-- the reaper guard as "never expires" so the cutover window did not accidentally
-- reap long-running tasks.
--
-- AFTER 050:
--   Pre-049 rows in LEASED or RUNNING with NULL `lease_expires_at` get a
--   backfill value derived from `created_at + 30 min` (matching the canonical
--   defaultTaskLeaseTTL = 30 * time.Minute keyword). Once filled, those rows
--   fall back into the reaper's normal SELECT path, so a long-stalled
--   pre-cutover task is no longer silently invisible to the sweep.
--   PENDING / READY / SUCCEEDED / FAILED / CANCELLED rows keep NULL
--   (`lease_expires_at` has no meaning outside the LEASED/RUNNING boundary).
--
-- Idempotency strategy:
--   - The WHERE clause restricts to NULL + LEASED/RUNNING. After the first
--     run, no NULL rows remain in those states, so a re-apply is a no-op
--     without error.
--   - DATETIME(created_at, '+30 minutes') uses SQLite's built-in date
--     math; the result is RFC3339-format-compatible with what
--     ClaimNextReadyTask writes (also via time.Format(time.RFC3339)).
--
-- 050 does NOT alter the column to NOT NULL. Adding NOT NULL would require
-- CREATE TABLE-AS-SELECT (SQLite has no DROP/ADD CONSTRAINT for existing
-- columns), which risks a destructive rebuild for marginal benefit. The
-- reaper already tolerates NULL via COALESCE, so the schema stays nullable
-- by design.
UPDATE tasks
SET lease_expires_at = DATETIME(created_at, '+30 minutes'),
    updated_at       = DATETIME(created_at, '+30 minutes')
WHERE status IN ('LEASED', 'RUNNING')
  AND lease_expires_at IS NULL;
