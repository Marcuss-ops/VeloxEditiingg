-- 040_task_specs.sql
--
-- Stores validated, immutable, versioned task specifications.
-- spec_hash is a deterministic SHA-256 for integrity verification.
-- This is NOT an opaque unvalidated parameter dump.

CREATE TABLE IF NOT EXISTS task_specs (
    task_id        TEXT NOT NULL,
    spec_version   INTEGER NOT NULL DEFAULT 1,
    spec_hash      TEXT NOT NULL DEFAULT '',
    executor_id    TEXT NOT NULL DEFAULT '',
    payload_json   TEXT NOT NULL DEFAULT '{}',
    created_at     TEXT NOT NULL,
    PRIMARY KEY (task_id)
);
