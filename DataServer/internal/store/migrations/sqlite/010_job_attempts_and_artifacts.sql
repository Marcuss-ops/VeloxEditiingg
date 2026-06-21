-- Migration 010: job_attempts and artifacts tables
--
-- job_attempts: ogni esecuzione reale di un job (storia separata dal record job)
-- artifacts: registro centralizzato di tutti gli output prodotti
--
-- Entrambe le tabelle preparano il modello dati per PostgreSQL/storage esterno
-- mantenendo SQLite come implementazione corrente.

-- ============================================================
-- job_attempts: each execution attempt of a job
-- ============================================================
CREATE TABLE IF NOT EXISTS job_attempts (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id           TEXT NOT NULL,
    attempt_number   INTEGER NOT NULL DEFAULT 1,
    worker_id        TEXT NOT NULL DEFAULT '',
    lease_id         TEXT NOT NULL DEFAULT '',
    status           TEXT NOT NULL DEFAULT 'pending',
    started_at       TEXT,
    finished_at      TEXT,
    error_code       TEXT NOT NULL DEFAULT '',
    engine_version   TEXT NOT NULL DEFAULT '',
    bundle_hash      TEXT NOT NULL DEFAULT '',
    created_at       TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_job_attempts_job_id ON job_attempts(job_id);
CREATE INDEX IF NOT EXISTS idx_job_attempts_status ON job_attempts(status);
CREATE INDEX IF NOT EXISTS idx_job_attempts_worker ON job_attempts(worker_id);

-- ============================================================
-- artifacts: central artifact registry
-- ============================================================
CREATE TABLE IF NOT EXISTS artifacts (
    id                TEXT PRIMARY KEY,
    job_id            TEXT NOT NULL,
    attempt_id        INTEGER,
    type              TEXT NOT NULL DEFAULT 'video',
    storage_provider  TEXT NOT NULL DEFAULT 'local',
    storage_key       TEXT NOT NULL DEFAULT '',
    storage_url       TEXT NOT NULL DEFAULT '',
    local_path        TEXT NOT NULL DEFAULT '',
    sha256            TEXT NOT NULL DEFAULT '',
    size_bytes        INTEGER NOT NULL DEFAULT 0,
    duration_seconds  REAL NOT NULL DEFAULT 0.0,
    status            TEXT NOT NULL DEFAULT 'pending',
    created_at        TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_artifacts_job_id ON artifacts(job_id);
CREATE INDEX IF NOT EXISTS idx_artifacts_sha256 ON artifacts(sha256);
CREATE INDEX IF NOT EXISTS idx_artifacts_provider ON artifacts(storage_provider);
