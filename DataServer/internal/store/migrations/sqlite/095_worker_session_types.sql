-- 095: separate HTTP asset-token sessions from gRPC control sessions.
-- A worker legitimately needs both while a Task is running.

ALTER TABLE worker_sessions ADD COLUMN session_type TEXT NOT NULL DEFAULT 'control';

DROP TRIGGER IF EXISTS trg_worker_sessions_one_active;
DROP TRIGGER IF EXISTS trg_worker_sessions_one_active_update;

CREATE TRIGGER IF NOT EXISTS trg_worker_sessions_one_active
BEFORE INSERT ON worker_sessions
WHEN NEW.status='ACTIVE' AND NEW.revoked=0
 AND EXISTS (
   SELECT 1 FROM worker_sessions
   WHERE worker_id=NEW.worker_id
     AND session_type=NEW.session_type
     AND status='ACTIVE'
     AND revoked=0
 )
BEGIN SELECT RAISE(ABORT, 'worker already has an active session of this type'); END;

CREATE TRIGGER IF NOT EXISTS trg_worker_sessions_one_active_update
BEFORE UPDATE OF worker_id,status,revoked,session_type ON worker_sessions
WHEN NEW.status='ACTIVE' AND NEW.revoked=0
 AND EXISTS (
   SELECT 1 FROM worker_sessions
   WHERE worker_id=NEW.worker_id
     AND session_type=NEW.session_type
     AND session_id<>NEW.session_id
     AND status='ACTIVE'
     AND revoked=0
 )
BEGIN SELECT RAISE(ABORT, 'worker already has an active session of this type'); END;
