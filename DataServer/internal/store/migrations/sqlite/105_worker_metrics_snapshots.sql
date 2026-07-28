-- 105_worker_metrics_snapshots.sql
--
-- Step 13/15 fleet-operator surface. New analytics rollup table
-- holding the 13-metric fleet telemetry snapshot per worker per
-- period. Distinct from worker_metric_samples (migration 094,
-- per-sample high-throughput counters) — this table holds the
-- AGGREGATIONS the operator dashboard reads at >1Hz on the
-- /api/v1/admin/workers/{id}/metrics endpoints.
--
-- Aggregation strategy: each row is one (worker_id, snapshotted_at)
-- pair. The fleet-side scheduler (registered in bootstrap_composition.go
-- via the existing supervisor pattern) computes the 13 metrics
-- from:
--   - worker_metric_samples (availability_percent)
--   - fleet_operations (jobs_succeeded, jobs_failed, queue_ms avg)
--   - smoke_runs (render_ms avg/p95, last_smoke_status)
--   - deployment_records (restarts, rollback_count, current_image_digest)
--   - worker_events (disconnects — approximate)
--
-- Windows:
--   - availability_percent / disconnects: 24h sliding window
--   - failure_rate: lifetime cumulative
--   - jobs_succeeded / jobs_failed / restarts / rollback_count:
--     lifetime cumulative
--   - queue_ms / render_ms: trailing-100 window
--   - current_image_digest / last_smoke_status: latest row snapshot
--
-- download_ms_avg is RESERVED for Step 14+ which will add phase-
-- level columns to smoke_runs. Until then, the column defaults
-- to 0 and the dashboard treats it as "undefined" — this matches
-- the design Q2 decision documented in the Step 13/15 thinker call.
--
-- Retention: pruneWorkerMetricsSnapshots drops rows older than 30
-- days (Q4: KEEP-WITHIN-WINDOW). Trailing-100 windows on render /
-- queue stay accurate because those derive from the FULL tables,
-- not the snapshots table — pruning is a dashboard-history cap.
--
-- Insertion order is by (worker_id, snapshotted_at DESC). The
-- dashboard's "latest snapshot per worker" query uses
--   SELECT * FROM worker_metrics_snapshots
--    WHERE snapshotted_at = (SELECT MAX(snapshotted_at) ...)
-- so the snapshotted_at index drives the lookup.

CREATE TABLE IF NOT EXISTS worker_metrics_snapshots (
  snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
  worker_id TEXT NOT NULL CHECK (length(worker_id) > 0),
  snapshotted_at TEXT NOT NULL,

  -- 24h sliding-window aggregates
  availability_percent REAL,           -- NULL when no samples in window
  disconnects INTEGER NOT NULL DEFAULT 0 CHECK (disconnects >= 0),

  -- Lifetime cumulative
  jobs_succeeded INTEGER NOT NULL DEFAULT 0 CHECK (jobs_succeeded >= 0),
  jobs_failed INTEGER NOT NULL DEFAULT 0 CHECK (jobs_failed >= 0),
  failure_rate REAL,                   -- NULL when total == 0; 0..100
  restarts INTEGER NOT NULL DEFAULT 0 CHECK (restarts >= 0),
  rollback_count INTEGER NOT NULL DEFAULT 0 CHECK (rollback_count >= 0),
  current_image_digest TEXT,           -- NULL when no SUCCEEDED deploy
  last_smoke_status TEXT,              -- PENDING | SUCCEEDED | FAILED | NULL

  -- Trailing-N window aggregates (rendering / queueing timing)
  queue_ms_avg INTEGER NOT NULL DEFAULT 0 CHECK (queue_ms_avg >= 0),
  render_ms_avg INTEGER NOT NULL DEFAULT 0 CHECK (render_ms_avg >= 0),
  render_ms_p95 INTEGER NOT NULL DEFAULT 0 CHECK (render_ms_p95 >= 0),

  -- RESERVED for Step 14+ (phase-level columns on smoke_runs). Until
  -- then, the aggregator writes 0 and the dashboard renders "—".
  download_ms_avg INTEGER NOT NULL DEFAULT 0 CHECK (download_ms_avg >= 0)
);

CREATE INDEX IF NOT EXISTS idx_worker_metrics_snapshots_worker
  ON worker_metrics_snapshots(worker_id, snapshotted_at DESC);

CREATE INDEX IF NOT EXISTS idx_worker_metrics_snapshots_at
  ON worker_metrics_snapshots(snapshotted_at DESC);

CREATE INDEX IF NOT EXISTS idx_worker_metrics_snapshots_status
  ON worker_metrics_snapshots(last_smoke_status, snapshotted_at DESC);
