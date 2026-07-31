-- 109_worker_runtime_snapshots.sql
--
-- Observability chain / block 1: worker runtime identity snapshots.
--
-- A `worker_runtime_snapshots` row is minted by the MASTER during the
-- Hello handshake (handler_stream.go) for every admitted session. It
-- freezes the worker's declared runtime identity at connect time
-- (hostname, node_id, bundle/engine/worker versions, protocol version)
-- so a session's identity is stable and queryable long after the
-- session disconnects and its volatile rows are gone.
--
-- The snapshot is linked to task_attempts via two new columns:
--   worker_session_id  — the gRPC session (grpc-<worker>-<ts>) that
--                        received the task at Claim time.
--   worker_snapshot_id — the worker_runtime_snapshots row minted for
--                        that session at Hello time.
-- Together they make every attempt attributable to the exact worker
-- runtime that executed it (recoverable-time + version-regression
-- analysis depends on this link).
--
-- All DEFAULTs are constant ('' / 0) per SQLite ALTER TABLE rules.

-- ── 1. worker_runtime_snapshots ────────────────────────────────────
CREATE TABLE IF NOT EXISTS worker_runtime_snapshots (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_id      TEXT    NOT NULL UNIQUE,
    worker_id        TEXT    NOT NULL,
    session_id       TEXT    NOT NULL,
    hostname         TEXT    NOT NULL DEFAULT '',
    node_id          TEXT    NOT NULL DEFAULT '',
    worker_version   TEXT    NOT NULL DEFAULT '',
    bundle_version   TEXT    NOT NULL DEFAULT '',
    bundle_hash      TEXT    NOT NULL DEFAULT '',
    engine_version   TEXT    NOT NULL DEFAULT '',
    git_sha          TEXT    NOT NULL DEFAULT '',
    protocol_version TEXT    NOT NULL DEFAULT '',
    created_at       TEXT    NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_worker_runtime_snapshots_worker
    ON worker_runtime_snapshots(worker_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_worker_runtime_snapshots_session
    ON worker_runtime_snapshots(session_id);

-- ── 2. Attempt → session/snapshot linkage ─────────────────────────
ALTER TABLE task_attempts ADD COLUMN worker_session_id  TEXT NOT NULL DEFAULT '';
ALTER TABLE task_attempts ADD COLUMN worker_snapshot_id TEXT NOT NULL DEFAULT '';

CREATE INDEX IF NOT EXISTS idx_task_attempts_worker_session
    ON task_attempts(worker_session_id, worker_snapshot_id);
