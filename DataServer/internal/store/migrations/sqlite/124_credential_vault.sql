-- Central encrypted provider credential vault. Plaintext material never
-- belongs in SQLite; ciphertext is AES-GCM output owned by internal/credentials.
CREATE TABLE IF NOT EXISTS credential_vault (
    credential_ref       TEXT PRIMARY KEY,
    provider             TEXT NOT NULL,
    owner                TEXT NOT NULL,
    ciphertext           BLOB NOT NULL,
    key_version          INTEGER NOT NULL,
    scopes_json          TEXT NOT NULL DEFAULT '[]',
    expires_at           TEXT,
    rotation_due_at      TEXT,
    revoked_at           TEXT,
    created_at           TEXT NOT NULL,
    updated_at           TEXT NOT NULL,
    last_used_at         TEXT
);

CREATE INDEX IF NOT EXISTS idx_credential_vault_provider ON credential_vault(provider);
CREATE INDEX IF NOT EXISTS idx_credential_vault_revoked ON credential_vault(revoked_at);

CREATE TABLE IF NOT EXISTS credential_usage_audit (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    credential_ref       TEXT NOT NULL,
    worker_id            TEXT NOT NULL DEFAULT '',
    publication_id       TEXT NOT NULL DEFAULT '',
    scopes_json          TEXT NOT NULL DEFAULT '[]',
    used_at              TEXT NOT NULL,
    success              INTEGER NOT NULL DEFAULT 0,
    error_code           TEXT NOT NULL DEFAULT '',
    FOREIGN KEY (credential_ref) REFERENCES credential_vault(credential_ref) ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS idx_credential_usage_ref ON credential_usage_audit(credential_ref);
CREATE INDEX IF NOT EXISTS idx_credential_usage_time ON credential_usage_audit(used_at);
