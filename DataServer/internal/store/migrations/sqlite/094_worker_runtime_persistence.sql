-- 094: structured worker runtime persistence.
-- The legacy workers.raw_json snapshot remains for compatibility, but the
-- volatile/session/audit projections below are queryable and transactional.

ALTER TABLE workers ADD COLUMN node_id TEXT NOT NULL DEFAULT '';
ALTER TABLE workers ADD COLUMN node_role TEXT NOT NULL DEFAULT 'worker';
ALTER TABLE workers ADD COLUMN cluster_id TEXT NOT NULL DEFAULT '';
ALTER TABLE workers ADD COLUMN host_fingerprint TEXT NOT NULL DEFAULT '';
ALTER TABLE workers ADD COLUMN certificate_fingerprint TEXT NOT NULL DEFAULT '';
ALTER TABLE workers ADD COLUMN connection_status TEXT NOT NULL DEFAULT '';
ALTER TABLE workers ADD COLUMN connection_reason TEXT NOT NULL DEFAULT '';
ALTER TABLE workers ADD COLUMN session_active INTEGER NOT NULL DEFAULT 0;
ALTER TABLE workers ADD COLUMN current_task_id TEXT NOT NULL DEFAULT '';
ALTER TABLE workers ADD COLUMN active_task_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE workers ADD COLUMN task_slots INTEGER NOT NULL DEFAULT 1;
ALTER TABLE workers ADD COLUMN cpu_utilization_ratio REAL NOT NULL DEFAULT 0;
ALTER TABLE workers ADD COLUMN memory_used_bytes INTEGER NOT NULL DEFAULT 0;
ALTER TABLE workers ADD COLUMN disk_free_bytes INTEGER NOT NULL DEFAULT 0;
ALTER TABLE workers ADD COLUMN jobs_completed INTEGER NOT NULL DEFAULT 0;
ALTER TABLE workers ADD COLUMN jobs_failed INTEGER NOT NULL DEFAULT 0;
ALTER TABLE workers ADD COLUMN connected_at TEXT;
ALTER TABLE workers ADD COLUMN last_heartbeat_at TEXT;
ALTER TABLE workers ADD COLUMN updated_at TEXT;

ALTER TABLE worker_sessions ADD COLUMN cluster_id TEXT NOT NULL DEFAULT '';
ALTER TABLE worker_sessions ADD COLUMN host_fingerprint TEXT NOT NULL DEFAULT '';
ALTER TABLE worker_sessions ADD COLUMN certificate_fingerprint TEXT NOT NULL DEFAULT '';
ALTER TABLE worker_sessions ADD COLUMN protocol_version TEXT NOT NULL DEFAULT '';
ALTER TABLE worker_sessions ADD COLUMN bundle_version TEXT;
ALTER TABLE worker_sessions ADD COLUMN status TEXT NOT NULL DEFAULT 'ACTIVE';
ALTER TABLE worker_sessions ADD COLUMN connected_at TEXT;
ALTER TABLE worker_sessions ADD COLUMN last_seen_at TEXT;
ALTER TABLE worker_sessions ADD COLUMN disconnected_at TEXT;
ALTER TABLE worker_sessions ADD COLUMN disconnect_reason TEXT;

-- Legacy InsertSession deliberately permits historical rows for the same
-- worker. The authenticated session validator is the authority for the
-- active row; enforcing uniqueness here would make old databases fail to
-- migrate before those historical rows are normalized.

UPDATE worker_sessions
SET status='DISCONNECTED', revoked=1, disconnect_reason='migration_normalized'
WHERE revoked=0
  AND session_id NOT IN (
    SELECT ws.session_id
    FROM worker_sessions ws
    WHERE ws.revoked=0
      AND ws.created_at = (SELECT MAX(ws2.created_at)
                           FROM worker_sessions ws2
                           WHERE ws2.worker_id=ws.worker_id AND ws2.revoked=0)
  );

CREATE TRIGGER IF NOT EXISTS trg_workers_node_role_guard
BEFORE INSERT ON workers
WHEN NEW.node_role <> 'worker'
BEGIN SELECT RAISE(ABORT, 'workers.node_role must be worker'); END;
CREATE TRIGGER IF NOT EXISTS trg_workers_node_role_guard_update
BEFORE UPDATE OF node_role ON workers
WHEN NEW.node_role <> 'worker'
BEGIN SELECT RAISE(ABORT, 'workers.node_role must be worker'); END;

CREATE TRIGGER IF NOT EXISTS trg_worker_sessions_shape_guard
BEFORE INSERT ON worker_sessions
WHEN NEW.status NOT IN ('ACTIVE','DISCONNECTED','REVOKED','EXPIRED')
BEGIN SELECT RAISE(ABORT, 'invalid worker session status'); END;
CREATE TRIGGER IF NOT EXISTS trg_worker_sessions_worker_guard
BEFORE INSERT ON worker_sessions
WHEN NOT EXISTS (SELECT 1 FROM workers WHERE worker_id=NEW.worker_id)
BEGIN SELECT RAISE(ABORT, 'worker session references unknown worker'); END;
CREATE TRIGGER IF NOT EXISTS trg_worker_sessions_one_active
BEFORE INSERT ON worker_sessions
WHEN NEW.status='ACTIVE' AND NEW.revoked=0
 AND EXISTS (SELECT 1 FROM worker_sessions WHERE worker_id=NEW.worker_id AND status='ACTIVE' AND revoked=0)
BEGIN SELECT RAISE(ABORT, 'worker already has an active session'); END;
CREATE TRIGGER IF NOT EXISTS trg_worker_sessions_one_active_update
BEFORE UPDATE OF worker_id,status,revoked ON worker_sessions
WHEN NEW.status='ACTIVE' AND NEW.revoked=0
 AND EXISTS (SELECT 1 FROM worker_sessions WHERE worker_id=NEW.worker_id AND session_id<>NEW.session_id AND status='ACTIVE' AND revoked=0)
BEGIN SELECT RAISE(ABORT, 'worker already has an active session'); END;

CREATE TABLE IF NOT EXISTS worker_task_runtime (
  task_id TEXT PRIMARY KEY,
  job_id TEXT NOT NULL,
  attempt_id TEXT NOT NULL,
  attempt_number INTEGER NOT NULL,
  worker_id TEXT NOT NULL,
  session_id TEXT NOT NULL,
  lease_id TEXT NOT NULL,
  executor_id TEXT NOT NULL,
  executor_version INTEGER NOT NULL DEFAULT 0,
  runtime_status TEXT NOT NULL,
  progress_percent INTEGER NOT NULL DEFAULT 0 CHECK(progress_percent BETWEEN 0 AND 100),
  progress_stage TEXT,
  current_scene INTEGER NOT NULL DEFAULT 0,
  total_scenes INTEGER NOT NULL DEFAULT 0,
  started_at TEXT NOT NULL,
  last_progress_at TEXT,
  cancel_requested_at TEXT,
  updated_at TEXT NOT NULL,
  missing_heartbeats INTEGER NOT NULL DEFAULT 0,
  FOREIGN KEY(worker_id) REFERENCES workers(worker_id)
);
CREATE INDEX IF NOT EXISTS idx_worker_runtime_worker ON worker_task_runtime(worker_id, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_worker_runtime_job ON worker_task_runtime(job_id);

CREATE TRIGGER IF NOT EXISTS trg_worker_runtime_shape_guard
BEFORE INSERT ON worker_task_runtime
WHEN NEW.runtime_status NOT IN ('ACCEPTED','STARTING','RUNNING','CANCELLING','UPLOADING','FINALIZING')
BEGIN SELECT RAISE(ABORT, 'invalid worker runtime status'); END;
CREATE TRIGGER IF NOT EXISTS trg_worker_runtime_shape_guard_update
BEFORE UPDATE OF runtime_status ON worker_task_runtime
WHEN NEW.runtime_status NOT IN ('ACCEPTED','STARTING','RUNNING','CANCELLING','UPLOADING','FINALIZING')
BEGIN SELECT RAISE(ABORT, 'invalid worker runtime status'); END;

CREATE TABLE IF NOT EXISTS worker_metric_samples (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  worker_id TEXT NOT NULL,
  session_id TEXT,
  sampled_at TEXT NOT NULL,
  connection_status TEXT NOT NULL,
  active_tasks INTEGER NOT NULL,
  task_slots INTEGER NOT NULL,
  cpu_utilization_ratio REAL NOT NULL,
  memory_used_bytes INTEGER NOT NULL,
  disk_free_bytes INTEGER NOT NULL,
  load_average REAL,
  process_rss_bytes INTEGER,
  network_rx_bytes INTEGER,
  network_tx_bytes INTEGER,
  FOREIGN KEY(worker_id) REFERENCES workers(worker_id)
);
CREATE INDEX IF NOT EXISTS idx_worker_metrics_time ON worker_metric_samples(worker_id, sampled_at DESC);

CREATE TABLE IF NOT EXISTS worker_events (
  event_id TEXT PRIMARY KEY,
  worker_id TEXT,
  session_id TEXT,
  job_id TEXT,
  task_id TEXT,
  attempt_id TEXT,
  event_type TEXT NOT NULL,
  severity TEXT NOT NULL,
  reason_code TEXT,
  details_json TEXT,
  created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_worker_events_worker_time ON worker_events(worker_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_worker_events_job ON worker_events(job_id, created_at);
