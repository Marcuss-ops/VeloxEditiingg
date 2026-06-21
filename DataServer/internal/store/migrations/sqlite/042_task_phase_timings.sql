-- 042_task_phase_timings.sql
--
-- Stores one row per canonical phase per attempt.
-- Phase names are fixed: queue, asset_wait, cache_lookup, download, decode,
-- compile, simulate, render, composite, encode, upload, finalize.

CREATE TABLE IF NOT EXISTS task_phase_timings (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    attempt_id  TEXT NOT NULL,
    phase       TEXT NOT NULL,
    duration_ms INTEGER NOT NULL DEFAULT 0,
    wall_start  TEXT NOT NULL DEFAULT '',
    wall_end    TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_task_phase_timings_attempt_id
    ON task_phase_timings(attempt_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_task_phase_timings_attempt_phase
    ON task_phase_timings(attempt_id, phase);
