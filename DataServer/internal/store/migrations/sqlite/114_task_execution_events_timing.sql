-- 114_task_execution_events_timing.sql
--
-- Additive timing payload extension for task_execution_events. The
-- append-only event identity remains event_id; these columns preserve the
-- monotonic offsets, CPU/queue wait, and frame counters emitted by the
-- worker PhaseTimingDetailed protocol.

ALTER TABLE task_execution_events
    ADD COLUMN started_offset_ms REAL NOT NULL DEFAULT 0;
ALTER TABLE task_execution_events
    ADD COLUMN finished_offset_ms REAL NOT NULL DEFAULT 0;
ALTER TABLE task_execution_events
    ADD COLUMN cpu_ms REAL NOT NULL DEFAULT 0;
ALTER TABLE task_execution_events
    ADD COLUMN queue_wait_ms REAL NOT NULL DEFAULT 0;
ALTER TABLE task_execution_events
    ADD COLUMN frames_in INTEGER NOT NULL DEFAULT 0;
ALTER TABLE task_execution_events
    ADD COLUMN frames_out INTEGER NOT NULL DEFAULT 0;

DROP TRIGGER IF EXISTS trg_task_execution_events_handle_duplicate_event_id;

CREATE TRIGGER IF NOT EXISTS trg_task_execution_events_handle_duplicate_event_id BEFORE INSERT ON task_execution_events FOR EACH ROW WHEN EXISTS (SELECT 1 FROM task_execution_events WHERE event_id = NEW.event_id) BEGIN SELECT CASE WHEN EXISTS (SELECT 1 FROM task_execution_events WHERE event_id = NEW.event_id AND attempt_id IS NEW.attempt_id AND job_id IS NEW.job_id AND task_id IS NEW.task_id AND worker_id IS NEW.worker_id AND worker_session_id IS NEW.worker_session_id AND worker_snapshot_id IS NEW.worker_snapshot_id AND lease_id IS NEW.lease_id AND executor_id IS NEW.executor_id AND executor_version IS NEW.executor_version AND event_index IS NEW.event_index AND origin IS NEW.origin AND scope IS NEW.scope AND event_type IS NEW.event_type AND event_name IS NEW.event_name AND component IS NEW.component AND action IS NEW.action AND phase IS NEW.phase AND status IS NEW.status AND error_code IS NEW.error_code AND error_message IS NEW.error_message AND started_at IS NEW.started_at AND completed_at IS NEW.completed_at AND duration_ms IS NEW.duration_ms AND bytes_in IS NEW.bytes_in AND bytes_out IS NEW.bytes_out AND frames IS NEW.frames AND metadata_json IS NEW.metadata_json AND created_at IS NEW.created_at AND segment_index IS NEW.segment_index AND track_kind IS NEW.track_kind AND track_index IS NEW.track_index AND artifact_id IS NEW.artifact_id AND started_offset_ms IS NEW.started_offset_ms AND finished_offset_ms IS NEW.finished_offset_ms AND cpu_ms IS NEW.cpu_ms AND queue_wait_ms IS NEW.queue_wait_ms AND frames_in IS NEW.frames_in AND frames_out IS NEW.frames_out) THEN RAISE(IGNORE) ELSE RAISE(ABORT, 'task_execution_events.event_id conflict') END; END;
