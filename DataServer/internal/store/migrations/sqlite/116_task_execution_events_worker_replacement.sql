-- 116_task_execution_events_worker_replacement.sql
--
-- The terminal worker report is authoritative for worker-produced events.
-- IngestTaskResultAtomic replaces the non-master event set for an attempt
-- inside its existing transaction. Master-origin events are orchestration
-- history and remain immutable.
--
-- SQLite has no transaction-local trigger variables. This marker table is
-- the explicit authorization protocol for the single atomic writer: the
-- ingest transaction inserts the fixed marker, performs its replacement,
-- and removes the marker before commit. Any DELETE without that marker is
-- rejected, so ordinary callers cannot bypass append-only semantics.

CREATE TABLE IF NOT EXISTS task_execution_event_replacement_authorizations (
    attempt_id TEXT PRIMARY KEY,
    authorization TEXT NOT NULL,
    created_at TEXT NOT NULL
);

DROP TRIGGER IF EXISTS trg_task_execution_events_append_only_delete;
DROP TRIGGER IF EXISTS trg_task_execution_events_protect_master_delete;
DROP TRIGGER IF EXISTS trg_task_execution_events_require_replacement_authorization;

CREATE TRIGGER IF NOT EXISTS trg_task_execution_events_protect_master_delete BEFORE DELETE ON task_execution_events FOR EACH ROW WHEN OLD.origin = 'master' BEGIN SELECT RAISE(ABORT, 'master task_execution_events are immutable'); END;
CREATE TRIGGER IF NOT EXISTS trg_task_execution_events_require_replacement_authorization BEFORE DELETE ON task_execution_events FOR EACH ROW WHEN OLD.origin <> 'master' AND NOT EXISTS (SELECT 1 FROM task_execution_event_replacement_authorizations WHERE attempt_id = OLD.attempt_id AND authorization = 'atomic_ingest') BEGIN SELECT RAISE(ABORT, 'task_execution_events deletion requires atomic ingest authorization'); END;
