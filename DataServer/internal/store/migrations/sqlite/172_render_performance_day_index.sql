-- 172_render_performance_day_index.sql
--
-- Expression index for the render-performance daily rollup day filter.
--
-- The rollup (internal/metrics/render_performance_rollup.go) filters
-- attempts with:
--
--   WHERE substr(COALESCE(NULLIF(completed_at, ''), updated_at), 1, 10) = ?
--   (and the same expression with `<` for the prior-day baseline pass)
--
-- Without this index the planner scans task_attempts and evaluates the
-- COALESCE/substr expression per row on every rollup pass. The expression
-- index below is an EXACT match of that expression, so both the `=` and `<`
-- variants can use it for a range/constraint seek instead of a scan.
--
-- The effective-day expression (completed_at falling back to updated_at)
-- mirrors the query's ORDER BY COALESCE(...) too, which additionally lets
-- the planner satisfy the ordering without a sort for the common case.
--
-- Idempotent via IF NOT EXISTS, matching the 158 convention. Expression
-- indexes require SQLite >= 3.9 (2015) — satisfied by every mattn
-- go-sqlite3 build shipped since the repo adopted Go 1.25.

CREATE INDEX IF NOT EXISTS idx_task_attempts_perf_day
    ON task_attempts(substr(COALESCE(NULLIF(completed_at, ''), updated_at), 1, 10));
