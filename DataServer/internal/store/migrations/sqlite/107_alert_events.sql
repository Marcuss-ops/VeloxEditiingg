-- alert_events table for Step 16/15 fleet-operator structured alerting.
--
-- 12 rules per the user spec, each producing 0..N AlertEvent rows over time:
--   - heartbeat_stale (>90s CRITICAL)
--   - container_unhealthy (CRITICAL)
--   - restart_loop (>=3/h CRITICAL)
--   - disk_pressure (>85% WARNING, >95% CRITICAL)
--   - ram_pressure (>90% CRITICAL)
--   - consecutive_job_failures (>=3 in row, CRITICAL)
--   - smoke_failed (CRITICAL)
--   - version_drift (WARNING)
--   - cert_expiring (<15d WARNING, <5d CRITICAL)
--   - drive_delivery_failed (CRITICAL)
--   - deployment_rollback (CRITICAL)
--
-- Each rule emits at most one ACTIVE row per (worker_id, rule_id) at a time;
-- on resolution the row's resolved_at is stamped. The dedup logic lives in
-- internal/fleet/opsalerts/dedup.go (in-memory dedup window keyed by
-- (worker_id, rule_id, severity)); the alert_events table is the persisted
-- record the dashboard renders.
--
-- Retention: 30 days matching worker_metrics_snapshots. The Q4 KEEP-WITHIN-WINDOW
-- convention; pruner will drop resolved rows older than 30d in a future
-- hardening commit.
--
-- Informational severity (INFO) is NOT written here per the user spec
-- "Sopprimere eventi normali" — the engine logs INFO and never emits an
-- AlertEvent row. WARNING/CRITICAL both write rows; WARNING dedups over a
-- 5-minute window, CRITICAL fires immediately.

CREATE TABLE IF NOT EXISTS alert_events (
  event_id TEXT PRIMARY KEY,
  worker_id TEXT NOT NULL CHECK (length(worker_id) > 0),
  rule_id TEXT NOT NULL CHECK (length(rule_id) > 0),
  severity TEXT NOT NULL CHECK (severity IN ('INFO', 'WARNING', 'CRITICAL')),
  state TEXT NOT NULL CHECK (state IN ('ACTIVE', 'RESOLVED')),
  fired_at TEXT NOT NULL,
  resolved_at TEXT,
  last_observed_at TEXT NOT NULL,
  current_value TEXT,
  message TEXT NOT NULL CHECK (length(message) > 0)
);

CREATE INDEX IF NOT EXISTS idx_alert_events_worker_status_time
  ON alert_events(worker_id, severity, fired_at DESC);

CREATE INDEX IF NOT EXISTS idx_alert_events_active
  ON alert_events(state, severity, fired_at DESC)
  WHERE state = 'ACTIVE';

CREATE INDEX IF NOT EXISTS idx_alert_events_severity_rule
  ON alert_events(severity, rule_id, fired_at DESC);
