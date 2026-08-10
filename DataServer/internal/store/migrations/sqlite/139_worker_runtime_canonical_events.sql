-- 139: canonical Attempt lifecycle events on the existing live runtime read model.
-- This is a JSON projection only; detailed event persistence remains the
-- existing TaskResult task_execution_events path.
ALTER TABLE worker_task_runtime ADD COLUMN canonical_events_json TEXT NOT NULL DEFAULT '[]';
