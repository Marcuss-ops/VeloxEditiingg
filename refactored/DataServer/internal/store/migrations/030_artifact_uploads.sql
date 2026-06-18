-- 030_artifact_uploads.sql — PR 2 (Chunk 1/3 foundation)
--
-- Rationale (PR 2 spec, "Nuova struttura applicativa"):
--   * artifact_uploads is the master-side record of a single upload session
--     ("BeginUpload -> Receive -> Finalize"). It lets the master enforce that
--     the blob is opened, hashed, sized, and only THEN promoted to a
--     deterministic content-addressable storage_key derived from the
--     master-computed SHA-256 (artifacts/sha256/<aa>/<sha256>.<ext>).
--   * artifacts.storage_key is content-addressable; the
--     UNIQUE(storage_provider, storage_key) makes duplicate FINALIZEs a
--     SQL no-op, complementing the ON CONFLICT DO NOTHING on
--     job_deliveries and the INSERT ... ON CONFLICT DO NOTHING already
--     used by the spec for delivery creation.
--   * idx_artifacts_job_status speeds up the "no READY of same kind" gate
--     in BeginUpload (Fase 1) and the reconciler queries (chunk 5).
--
-- DOWN-MIGRATION NOTE: not provided. Drop and backfill from backup.

-- ============================================================
-- artifact_uploads: master-side upload session record
-- ============================================================
CREATE TABLE IF NOT EXISTS artifact_uploads (
    upload_id    TEXT PRIMARY KEY,
    artifact_id  TEXT NOT NULL,
    job_id       TEXT NOT NULL,
    attempt_number INTEGER NOT NULL,
    worker_id    TEXT NOT NULL,
    lease_id     TEXT NOT NULL,

    status                   TEXT NOT NULL,
    temporary_storage_key    TEXT NOT NULL,

    expected_size_bytes      INTEGER,
    expected_sha256          TEXT,

    received_size_bytes      INTEGER,
    received_sha256          TEXT,

    created_at   TEXT NOT NULL,
    expires_at   TEXT NOT NULL,
    completed_at TEXT,

    FOREIGN KEY (artifact_id) REFERENCES artifacts(id),
    FOREIGN KEY (job_id) REFERENCES jobs(job_id),

    CHECK (
        status IN (
            'CREATED',
            'UPLOADING',
            'RECEIVED',
            'FINALIZING',
            'COMPLETED',
            'FAILED',
            'EXPIRED'
        )
    )
);

-- BeginUpload session lookup by job_id+status and reconciler scans.
CREATE INDEX IF NOT EXISTS idx_artifact_uploads_job
    ON artifact_uploads(job_id, status);

-- Reconciler rule: "staging session troppo vecchio -> EXPIRED".
CREATE INDEX IF NOT EXISTS idx_artifact_uploads_expiry
    ON artifact_uploads(status, expires_at);

-- ============================================================
-- artifacts: enforce deterministic, content-addressable storage_key.
--
-- The WHERE clause ignores placeholder rows (storage_key='') used during
-- reconciliation. If duplicates exist for non-empty keys, migration aborts
-- and an operator must clean them before re-running; this mirrors the
-- pre_check.go pattern used in 028_legacy_drop.
-- ============================================================
CREATE UNIQUE INDEX IF NOT EXISTS idx_artifacts_storage_key
    ON artifacts(storage_provider, storage_key)
    WHERE storage_key <> '';

-- BeginUpload Fase 1 gate: 'no READY artifact of this kind for this job'.
CREATE INDEX IF NOT EXISTS idx_artifacts_job_status
    ON artifacts(job_id, status);
