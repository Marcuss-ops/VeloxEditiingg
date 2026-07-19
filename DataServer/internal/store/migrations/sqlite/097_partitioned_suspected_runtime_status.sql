-- 097: Extend worker_task_runtime trigger allow-list to include
-- PARTITIONED_SUSPECTED for the heartbeat-driven network-partition
-- propagation feature (commit landing this migration).
--
-- Background:
--   bulkEmitTaskRuntimeDisappearedOnPartition (in
--   store_worker_runtime_projection.go) flips every active
--   worker_task_runtime row's runtime_status to 'PARTITIONED_SUSPECTED'
--   when the parent worker's connection_state crosses the partition
--   threshold via the heartbeat-time detector. The status flip is
--   paired with a TASK_RUNTIME_DISAPPEARED worker_events row carrying
--   reason_code='partition_timeout'.
--
-- Why this migration is necessary:
--   Migration 094 shipped trg_worker_runtime_shape_guard +
--   trg_worker_runtime_shape_guard_update with a closed allow-list
--   (ACCEPTED, STARTING, RUNNING, CANCELLING, UPLOADING, FINALIZING).
--   Any INSERT or UPDATE OF runtime_status using a token outside that
--   allow-list raises ABORT and rolls back the surrounding tx — which
--   would silently drop the heartbeat pipeline's partition-time
--   telemetry AND the per-task disappearance events.
--
-- Why DROP + CREATE rather than ALTER TRIGGER:
--   SQLite does not support ALTER TRIGGER in the way Postgres/
--   MySQL do; the canonical pattern in this codebase's migration
--   history (per migrations/sqlite/021_artifact_states.sql and other
--   trigger-touching migrations) is DROP TRIGGER IF EXISTS followed
--   by CREATE TRIGGER with the updated body. This preserves the
--   trigger-name binding and the BEFORE/AFTER semantics.

DROP TRIGGER IF EXISTS trg_worker_runtime_shape_guard;
CREATE TRIGGER trg_worker_runtime_shape_guard
BEFORE INSERT ON worker_task_runtime
WHEN NEW.runtime_status NOT IN ('ACCEPTED','STARTING','RUNNING','CANCELLING','UPLOADING','FINALIZING','PARTITIONED_SUSPECTED')
BEGIN SELECT RAISE(ABORT, 'invalid worker runtime status'); END;

DROP TRIGGER IF EXISTS trg_worker_runtime_shape_guard_update;
CREATE TRIGGER trg_worker_runtime_shape_guard_update
BEFORE UPDATE OF runtime_status ON worker_task_runtime
WHEN NEW.runtime_status NOT IN ('ACCEPTED','STARTING','RUNNING','CANCELLING','UPLOADING','FINALIZING','PARTITIONED_SUSPECTED')
BEGIN SELECT RAISE(ABORT, 'invalid worker runtime status'); END;
