-- 180_runtime_finalize_queue_index.sql
-- Keep the durable asynchronous FINALIZE queue cheap to poll. Payload state is
-- stored in task_specs, the canonical task-input record; this expression index
-- is only an index over its queue state, not a second source of truth.

CREATE INDEX IF NOT EXISTS idx_task_specs_runtime_finalize_status
    ON task_specs(json_extract(payload_json, '$.runtime_finalize_status'), task_id);
