-- Migration 101: M2M auth for POST /api/v1/jobs
--
-- POST /api/v1/jobs is the simplified M2M intake for external
-- automation. Different from the existing VELOX_ADMIN_TOKEN (a
-- single secret for ops-only endpoints), M2M clients have per-client
-- identity (client_id + scoped permissions + rate-limit + quota) and
-- are managed at runtime via admin CRUD endpoints under /api/v1/admin/m2m.
--
-- Two new tables:
--
--   m2m_api_keys: per-client M2M credentials. The secret is
--     machine-generated high-entropy random bytes; only the SHA-256
--     hash is persisted (the plaintext is returned ONCE at creation
--     time, never stored, never re-displayed). Soft-revocation via
--     is_active=0 preserves the audit trail integrity needed by the
--     audit log FK.
--
--   m2m_audit_log: append-only record of every M2M-authenticated
--     request. Written from the middleware AFTER the handler returns
--     so the recorded status_code reflects what the client actually
--     saw. The idem_key column stores a 12-byte SHA-256 prefix — not
--     the raw key — so log scrapers and DB dumps cannot extract
--     original client-provided PII from the row.
--
-- One column added to an existing table:
--
--   creator_forwardings.external_client_id: nullable TEXT. Stamped
--     from the M2M middleware's resolved client_id. Decoupled from
--     source_job_id (which remains the validated idempotency_key for
--     dedup invariants); the column exists so dashboards can group
--     forwardings by client without scanning source_job_id.

-- =====================================================================
-- m2m_api_keys
-- =====================================================================
CREATE TABLE IF NOT EXISTS m2m_api_keys (
    client_id              TEXT    PRIMARY KEY,
    secret_hash            TEXT    NOT NULL,
    scopes                 TEXT    NOT NULL DEFAULT 'jobs.submit',
    is_active              INTEGER NOT NULL DEFAULT 1,
    description            TEXT    NOT NULL DEFAULT '',
    rate_limit_rps         INTEGER NOT NULL DEFAULT 0,  -- 0 → cfg.M2M.DefaultRPS
    rate_limit_burst       INTEGER NOT NULL DEFAULT 0,  -- 0 → cfg.M2M.DefaultBurst
    quota_max_scenes       INTEGER NOT NULL DEFAULT 0,  -- 0 → cfg.M2M.MaxScenesPerRequest
    quota_max_total_secs   REAL    NOT NULL DEFAULT 0.0, -- 0 → cfg.M2M.MaxTotalDurationSecondsPerRequest
    created_at             TEXT    NOT NULL DEFAULT (datetime('now')),
    updated_at             TEXT    NOT NULL DEFAULT (datetime('now')),
    last_used_at           TEXT    NULL
);

CREATE INDEX IF NOT EXISTS idx_m2m_api_keys_active ON m2m_api_keys(is_active);
CREATE INDEX IF NOT EXISTS idx_m2m_api_keys_secret_hash ON m2m_api_keys(secret_hash);

-- =====================================================================
-- m2m_audit_log
-- =====================================================================
CREATE TABLE IF NOT EXISTS m2m_audit_log (
    id                     INTEGER PRIMARY KEY AUTOINCREMENT,
    client_id              TEXT    NOT NULL,
    idem_key_hash          TEXT    NOT NULL DEFAULT '',   -- 12-byte sha256 prefix
    method                 TEXT    NOT NULL DEFAULT 'POST',
    path                   TEXT    NOT NULL DEFAULT '/api/v1/jobs',
    status_code            INTEGER NOT NULL,
    scope                  TEXT    NOT NULL DEFAULT 'jobs.submit',
    scene_count            INTEGER NOT NULL DEFAULT 0,
    total_duration_seconds REAL    NOT NULL DEFAULT 0.0,
    ip_address             TEXT    NOT NULL DEFAULT '',
    reject_reason          TEXT    NULL,                  -- NULL when status_code < 400
    created_at             TEXT    NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (client_id) REFERENCES m2m_api_keys(client_id) ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS idx_m2m_audit_log_client_id ON m2m_audit_log(client_id);
CREATE INDEX IF NOT EXISTS idx_m2m_audit_log_created_at ON m2m_audit_log(created_at);
CREATE INDEX IF NOT EXISTS idx_m2m_audit_log_status_code ON m2m_audit_log(status_code);

-- =====================================================================
-- creator_forwardings.external_client_id
-- =====================================================================
ALTER TABLE creator_forwardings ADD COLUMN external_client_id TEXT DEFAULT NULL;

CREATE INDEX IF NOT EXISTS idx_creator_forwardings_external_client_id
    ON creator_forwardings(external_client_id)
    WHERE external_client_id IS NOT NULL;
