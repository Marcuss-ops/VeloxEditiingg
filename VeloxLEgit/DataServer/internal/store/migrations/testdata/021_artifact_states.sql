-- Migration 021: artifact states hardening for the ArtifactFinalizationService.
--
-- Add support for a clean STAGING → VERIFYING → READY → DELETED state machine
-- with QUARANTINED as a failed/suspect fallback. The previous "pending" /
-- "completed" / "staged" labels were loose and combined the "I have the
-- file" lifecycle with "the master verified it" — those now live as separate
-- columns (storage_provider + verified_at + sha256).
--
-- Idempotency: the migrations runner records each migration in the inline
-- schema_migrations table once committed, so re-running skips 021 entirely.
-- ALTER TABLE ADD COLUMN cannot use IF NOT EXISTS on SQLite versions older
-- than 3.35 (2021); we use plain ADD COLUMN here and rely on the runner for
-- idempotency. The application's postMigrationAdjustments() in store/sqlite.go
-- also re-applies any missing column via ensureColumn as a defensive measure
-- for partial-migration recovery.

-- 1. Status vocabulary normalization. Idempotent by WHERE filter: only touches
--    rows whose status is outside the canonical set; a re-run is a no-op.

UPDATE artifacts
SET status = CASE
    WHEN status = 'pending'   THEN 'STAGING'
    WHEN status = 'completed' THEN 'READY'
    WHEN status = 'staged'    THEN 'STAGING'
    WHEN status = ''          THEN 'STAGING'
    ELSE status
END
WHERE status NOT IN ('STAGING','VERIFYING','READY','QUARANTINED','DELETED');

-- 2. Add verified_at column (RFC3339 stamp set by the master when SHA + size
--    + mime all match). NULL = not yet verified.

ALTER TABLE artifacts ADD COLUMN verified_at TEXT;

-- 3. mime_type — the master sniffs the body's MIME via http.DetectContentType
--    before promoting STAGING → READY.

ALTER TABLE artifacts ADD COLUMN mime_type TEXT;

-- 4. duration_ms — for video artifacts, this is the actual media duration
--    after muxing in milliseconds. Default 0.0 for legacy rows.

ALTER TABLE artifacts ADD COLUMN duration_ms REAL NOT NULL DEFAULT 0.0;

-- 5. Index for the "find READY artifacts for a job" JOIN path that the
--    JobViewAssembler uses to surface legacy master_video_path and
--    video_uploaded.

CREATE INDEX IF NOT EXISTS idx_artifacts_job_status ON artifacts(job_id, status);

-- 6. Application-side CHECK on status is enforced in
--    ArtifactFinalizationService.FinalizeRender (transition maps every
--    emerging status to a legal value before COMMIT). The CHECK is enforced
--    by the application layer rather than at the SQL level so legacy rows
--    don't fail validation during a partial-status migration.
