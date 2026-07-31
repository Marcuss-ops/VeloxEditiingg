-- 112_worker_runtime_identity.sql
--
-- Observability chain / block 1: complete the immutable worker runtime
-- identity snapshot introduced by migration 109.
--
-- Migration 109 already created the snapshot table and linked
-- task_attempts to worker_session_id / worker_snapshot_id. This
-- follow-up is intentionally additive: migration checksums are immutable
-- after release, so the original migration must not be rewritten.
--
-- A snapshot is minted by the master for one worker session. The
-- worker_id + session_id pair is therefore unique, while snapshot_id
-- remains the stable identifier referenced by attempts and events.

-- ── Runtime identity and deployment metadata ───────────────────────
ALTER TABLE worker_runtime_snapshots
    ADD COLUMN worker_name TEXT NOT NULL DEFAULT '';

ALTER TABLE worker_runtime_snapshots
    ADD COLUMN worker_class TEXT NOT NULL DEFAULT '';

ALTER TABLE worker_runtime_snapshots
    ADD COLUMN rollout_group TEXT NOT NULL DEFAULT '';

ALTER TABLE worker_runtime_snapshots
    ADD COLUMN ffmpeg_version TEXT NOT NULL DEFAULT '';

ALTER TABLE worker_runtime_snapshots
    ADD COLUMN config_hash TEXT NOT NULL DEFAULT '';

ALTER TABLE worker_runtime_snapshots
    ADD COLUMN docker_image_digest TEXT NOT NULL DEFAULT '';

ALTER TABLE worker_runtime_snapshots
    ADD COLUMN capabilities_json TEXT NOT NULL DEFAULT '{}';

-- ── Immutable host hardware/software fingerprint ───────────────────
ALTER TABLE worker_runtime_snapshots
    ADD COLUMN cpu_model TEXT NOT NULL DEFAULT '';

ALTER TABLE worker_runtime_snapshots
    ADD COLUMN logical_cpu_count INTEGER NOT NULL DEFAULT 0;

ALTER TABLE worker_runtime_snapshots
    ADD COLUMN effective_cpu_count INTEGER NOT NULL DEFAULT 0;

ALTER TABLE worker_runtime_snapshots
    ADD COLUMN cpu_quota REAL NOT NULL DEFAULT 0;

ALTER TABLE worker_runtime_snapshots
    ADD COLUMN total_memory_bytes INTEGER NOT NULL DEFAULT 0;

ALTER TABLE worker_runtime_snapshots
    ADD COLUMN gpu_model TEXT NOT NULL DEFAULT '';

ALTER TABLE worker_runtime_snapshots
    ADD COLUMN gpu_driver TEXT NOT NULL DEFAULT '';

ALTER TABLE worker_runtime_snapshots
    ADD COLUMN kernel_version TEXT NOT NULL DEFAULT '';

ALTER TABLE worker_runtime_snapshots
    ADD COLUMN os_release TEXT NOT NULL DEFAULT '';

ALTER TABLE worker_runtime_snapshots
    ADD COLUMN storage_class TEXT NOT NULL DEFAULT '';

-- ── Session lifecycle timestamps ───────────────────────────────────
ALTER TABLE worker_runtime_snapshots
    ADD COLUMN connected_at TEXT NOT NULL DEFAULT '';

ALTER TABLE worker_runtime_snapshots
    ADD COLUMN disconnected_at TEXT;

-- ── Uniqueness and query indexes ───────────────────────────────────
CREATE UNIQUE INDEX IF NOT EXISTS uq_worker_runtime_snapshots_worker_session
    ON worker_runtime_snapshots(worker_id, session_id);

CREATE INDEX IF NOT EXISTS idx_worker_snapshot_version
    ON worker_runtime_snapshots(engine_version, ffmpeg_version, docker_image_digest);

CREATE INDEX IF NOT EXISTS idx_attempt_worker_snapshot
    ON task_attempts(worker_snapshot_id);
