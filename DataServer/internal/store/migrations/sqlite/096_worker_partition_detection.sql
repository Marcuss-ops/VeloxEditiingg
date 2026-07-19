-- 096: STALE threshold + network partition detection state machine.
--
-- Adds the persistent state columns on the workers row that allow
-- PersistWorkerHeartbeat to detect transitions between CONNECTED,
-- STALE, and PARTITIONED on every heartbeat, and emit the
-- canonical worker_events rows.
--
-- connection_state     - canonical state (CONNECTED | STALE |
--                        PARTITIONED) maintained by
--                        PersistWorkerHeartbeat at WRITE time
--                        (the read-time derivation in
--                        workers.ConnectionStatus is unchanged and
--                        remains the source of truth for the API
--                        surface; this column is the persistent
--                        mirror used to detect transitions).
-- last_state_change_at - RFC3339 timestamp of the last transition;
--                        bumped on every state change so dashboards
--                        can show "since when" without re-deriving
--                        from the workers_events ledger.
--
-- Both columns default to a "fresh / CONNECTED" posture so existing
-- rows on upgrade behave as if the worker just connected. The state
-- machine is owned by PersistWorkerHeartbeat (single-writer tx
-- contract) and reconciled on every heartbeat.
ALTER TABLE workers ADD COLUMN connection_state TEXT NOT NULL DEFAULT 'CONNECTED';
ALTER TABLE workers ADD COLUMN last_state_change_at TEXT;

-- Index to make the ReconcileWorkerPartitions scan O(matched workers)
-- rather than O(all workers). The reconciler fires periodically
-- from the master to detect workers whose heartbeat stream has
-- stopped without ever hitting PersistWorkerHeartbeat again
-- (network-partition detection surface).
CREATE INDEX IF NOT EXISTS idx_workers_connection_state
  ON workers(connection_state, last_heartbeat_at);