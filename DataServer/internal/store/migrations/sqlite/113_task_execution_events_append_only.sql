-- 113_task_execution_events_append_only.sql
--
-- Append-only repair for task_execution_events (migration 110).
--
-- Migration 110 correctly separated the event timeline from
-- task_phase_timings, but its UNIQUE(attempt_id, origin, event_index)
-- identity assumes every producer shares one event-index namespace.
-- Segment, track, and artifact recorders may legitimately reuse an index
-- in their own scope. event_id is therefore the sole idempotency key;
-- the former unique index is retained only as a non-unique query index.
--
-- Migration files are immutable after application. This migration is
-- intentionally additive and safe to apply after 110 on existing stores.

ALTER TABLE task_execution_events
    ADD COLUMN event_id TEXT NOT NULL DEFAULT '';

ALTER TABLE task_execution_events
    ADD COLUMN segment_index INTEGER;

ALTER TABLE task_execution_events
    ADD COLUMN track_kind TEXT NOT NULL DEFAULT '';

ALTER TABLE task_execution_events
    ADD COLUMN track_index INTEGER;

ALTER TABLE task_execution_events
    ADD COLUMN artifact_id TEXT NOT NULL DEFAULT '';

-- Give pre-113 rows a stable replay identity before adding the unique
-- event_id index. New writers must provide a non-empty event_id.
UPDATE task_execution_events
   SET event_id = 'legacy-' || CAST(id AS TEXT)
 WHERE event_id = '';

DROP INDEX IF EXISTS idx_task_execution_events_attempt_origin_index;

CREATE UNIQUE INDEX IF NOT EXISTS uq_task_execution_events_event_id
    ON task_execution_events(event_id);

-- Non-unique ordering/filtering index: event_index is useful for timeline
-- scans but is not an identity constraint because independent scopes may
-- legitimately start at the same index.
CREATE INDEX IF NOT EXISTS idx_task_execution_events_attempt_origin_index
    ON task_execution_events(attempt_id, origin, event_index);

CREATE INDEX IF NOT EXISTS idx_task_execution_events_attempt_scope
    ON task_execution_events(
        attempt_id, scope, segment_index, track_kind, track_index, artifact_id, event_index
    );

CREATE INDEX IF NOT EXISTS idx_task_execution_events_artifact
    ON task_execution_events(artifact_id, created_at)
    WHERE artifact_id <> '';

CREATE INDEX IF NOT EXISTS idx_task_execution_events_track
    ON task_execution_events(attempt_id, track_kind, track_index, event_index)
    WHERE track_kind <> '';

-- SQLite cannot add CHECK constraints to an existing table without a
-- destructive table rebuild. These triggers provide equivalent write-time
-- guards while preserving the append-only migration path.
-- Keep each trigger definition on one physical line: the migration runner
-- splits statements at line-ending semicolons.
CREATE TRIGGER IF NOT EXISTS trg_task_execution_events_validate_event_id BEFORE INSERT ON task_execution_events FOR EACH ROW WHEN NEW.event_id IS NULL OR trim(NEW.event_id) = '' BEGIN SELECT RAISE(ABORT, 'task_execution_events.event_id is required'); END;
CREATE TRIGGER IF NOT EXISTS trg_task_execution_events_handle_duplicate_event_id BEFORE INSERT ON task_execution_events FOR EACH ROW WHEN EXISTS (SELECT 1 FROM task_execution_events WHERE event_id = NEW.event_id) BEGIN SELECT CASE WHEN EXISTS (SELECT 1 FROM task_execution_events WHERE event_id = NEW.event_id AND attempt_id IS NEW.attempt_id AND job_id IS NEW.job_id AND task_id IS NEW.task_id AND worker_id IS NEW.worker_id AND worker_session_id IS NEW.worker_session_id AND worker_snapshot_id IS NEW.worker_snapshot_id AND lease_id IS NEW.lease_id AND executor_id IS NEW.executor_id AND executor_version IS NEW.executor_version AND event_index IS NEW.event_index AND origin IS NEW.origin AND scope IS NEW.scope AND event_type IS NEW.event_type AND event_name IS NEW.event_name AND component IS NEW.component AND action IS NEW.action AND phase IS NEW.phase AND status IS NEW.status AND error_code IS NEW.error_code AND error_message IS NEW.error_message AND started_at IS NEW.started_at AND completed_at IS NEW.completed_at AND duration_ms IS NEW.duration_ms AND bytes_in IS NEW.bytes_in AND bytes_out IS NEW.bytes_out AND frames IS NEW.frames AND metadata_json IS NEW.metadata_json AND created_at IS NEW.created_at AND segment_index IS NEW.segment_index AND track_kind IS NEW.track_kind AND track_index IS NEW.track_index AND artifact_id IS NEW.artifact_id) THEN RAISE(IGNORE) ELSE RAISE(ABORT, 'task_execution_events.event_id conflict') END; END;
CREATE TRIGGER IF NOT EXISTS trg_task_execution_events_validate_event_index BEFORE INSERT ON task_execution_events FOR EACH ROW WHEN NEW.event_index < 0 BEGIN SELECT RAISE(ABORT, 'task_execution_events.event_index must be non-negative'); END;
CREATE TRIGGER IF NOT EXISTS trg_task_execution_events_validate_segment BEFORE INSERT ON task_execution_events FOR EACH ROW WHEN NEW.scope = 'segment' AND NEW.segment_index IS NULL BEGIN SELECT RAISE(ABORT, 'segment events require segment_index'); END;
CREATE TRIGGER IF NOT EXISTS trg_task_execution_events_validate_artifact BEFORE INSERT ON task_execution_events FOR EACH ROW WHEN NEW.scope = 'artifact' AND trim(NEW.artifact_id) = '' BEGIN SELECT RAISE(ABORT, 'artifact events require artifact_id'); END;
CREATE TRIGGER IF NOT EXISTS trg_task_execution_events_validate_track BEFORE INSERT ON task_execution_events FOR EACH ROW WHEN NEW.scope IN ('audio_track', 'subtitle_track') AND NEW.track_index IS NULL BEGIN SELECT RAISE(ABORT, 'track events require track_index'); END;
CREATE TRIGGER IF NOT EXISTS trg_task_execution_events_append_only_update BEFORE UPDATE ON task_execution_events FOR EACH ROW BEGIN SELECT RAISE(ABORT, 'task_execution_events is append-only'); END;
CREATE TRIGGER IF NOT EXISTS trg_task_execution_events_append_only_delete BEFORE DELETE ON task_execution_events FOR EACH ROW BEGIN SELECT RAISE(ABORT, 'task_execution_events is append-only'); END;

-- Idempotent writers should use INSERT OR IGNORE (or an equivalent
-- ON CONFLICT DO NOTHING) against the UNIQUE event_id index. An identical
-- replay is ignored, while reuse of an event_id with different payload
-- fails as a conflict. Both outcomes happen before INSERT OR REPLACE can
-- delete an existing event. The append-only DELETE trigger rejects
-- ordinary deletion as well.
