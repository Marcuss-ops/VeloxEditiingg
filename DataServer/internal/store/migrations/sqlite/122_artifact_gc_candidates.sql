CREATE TABLE IF NOT EXISTS artifact_gc_candidates (
    artifact_id TEXT PRIMARY KEY REFERENCES artifacts(id) ON DELETE CASCADE,
    reason TEXT NOT NULL,
    eligible_at TEXT NOT NULL,
    lease_owner TEXT NOT NULL DEFAULT '',
    lease_expires_at TEXT,
    delete_attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'ELIGIBLE'
        CHECK (status IN ('ELIGIBLE','DELETING','DELETED','FAILED'))
);
CREATE INDEX IF NOT EXISTS idx_artifact_gc_candidates_ready
    ON artifact_gc_candidates(status, eligible_at, lease_expires_at);
