package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// store_worker_metrics_snapshots.go owns the
// worker_metrics_snapshots table (migration 105) — one row per
// (worker_id, snapshotted_at) holding the 13-metric fleet
// telemetry snapshot that the Step 13/15 scheduler writes every
// 5 minutes and the dashboard reads at >1Hz.
//
// Distinct from worker_metric_samples (migration 094, raw per-
// sample counters) and the per-table aggregate views derived
// in-flight by the aggregator (internal/fleet/worker_metrics_
// aggregator.go). This table holds the PERSISTED snapshot that
// the dashboard's per-worker /metrics and aggregated /metrics
// endpoints read; the aggregator writes into it, the handler
// reads from it.
//
// Retention: pruneWorkerMetricsSnapshots drops rows older than
// 30 days (Q4 KEEP-WITHIN-WINDOW). Trailing-N window aggregates
// (queue/render) derive from the FULL source tables, not the
// snapshot table — pruning is a dashboard-history cap only.

// ErrWorkerMetricsSnapshotNotFound is returned by
// GetLatestWorkerMetricsForWorker when no row exists for the
// worker yet. Maps to a 404 in the per-worker handler; the
// aggregated handler logs+skips instead.
var ErrWorkerMetricsSnapshotNotFound = errors.New("no metrics snapshots for worker")

// WorkerMetricsSnapshot mirrors one row in worker_metrics_snapshots.
// All time fields are RFC3339 in SQL; Go-side conversion is at the
// repository boundary.
//
// Nullable fields (AvailabilityPercent, FailureRate,
// CurrentImageDigest, LastSmokeStatus) use sql.Null* so semantically-
// NULL ("no data in this window") preserves through Scan and the
// dashboard can distinguish "never had a sample" from "had 0".
type WorkerMetricsSnapshot struct {
	SnapshotID          int64
	WorkerID            string
	SnapshottedAt       time.Time
	AvailabilityPercent sql.NullFloat64
	Disconnects         int64
	JobsSucceeded       int64
	JobsFailed          int64
	FailureRate         sql.NullFloat64
	Restarts            int64
	RollbackCount       int64
	CurrentImageDigest  sql.NullString
	LastSmokeStatus     sql.NullString
	QueueMsAvg          int64
	RenderMsAvg         int64
	RenderMsP95         int64
	DownloadMsAvg       int64
}

// CreateWorkerMetricsSnapshotsTableIfNotExists is the test/dev
// bootstrap. Production uses migration 105. Idempotent.
func (s *SQLiteStore) CreateWorkerMetricsSnapshotsTableIfNotExists() error {
	ddl := `
CREATE TABLE IF NOT EXISTS worker_metrics_snapshots (
  snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
  worker_id TEXT NOT NULL CHECK (length(worker_id) > 0),
  snapshotted_at TEXT NOT NULL,
  availability_percent REAL,
  disconnects INTEGER NOT NULL DEFAULT 0,
  jobs_succeeded INTEGER NOT NULL DEFAULT 0,
  jobs_failed INTEGER NOT NULL DEFAULT 0,
  failure_rate REAL,
  restarts INTEGER NOT NULL DEFAULT 0,
  rollback_count INTEGER NOT NULL DEFAULT 0,
  current_image_digest TEXT,
  last_smoke_status TEXT,
  queue_ms_avg INTEGER NOT NULL DEFAULT 0,
  render_ms_avg INTEGER NOT NULL DEFAULT 0,
  render_ms_p95 INTEGER NOT NULL DEFAULT 0,
  download_ms_avg INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_worker_metrics_snapshots_worker
  ON worker_metrics_snapshots(worker_id, snapshotted_at DESC);
CREATE INDEX IF NOT EXISTS idx_worker_metrics_snapshots_at
  ON worker_metrics_snapshots(snapshotted_at DESC);
`
	_, err := s.db.Exec(ddl)
	return err
}

// InsertWorkerMetricsSnapshot persists a new analytics row. The
// scheduler calls this once per (worker_id, now) tick; the
// (worker_id, snapshotted_at) tuple is unique-by-construction
// because the scheduler stamps a fresh snapshotted_at each run.
//
// Nullable fields are mapped to SQL NULL via the Nullable* helpers
// from the wider store package (defined in store_nullable.go or a
// peer file). When the row carries `Valid=false`, the column is
// written as SQL NULL; the dashboard reads back a Null* field
// distinguishable from 0 / "".
func (s *SQLiteStore) InsertWorkerMetricsSnapshot(ctx context.Context, snap WorkerMetricsSnapshot) error {
	if snap.WorkerID == "" {
		return errors.New("InsertWorkerMetricsSnapshot: WorkerID empty")
	}
	avail := nullableFloat(snap.AvailabilityPercent)
	failRate := nullableFloat(snap.FailureRate)
	digest := nullableString(snap.CurrentImageDigest)
	lastSmoke := nullableString(snap.LastSmokeStatus)
	_, err := s.db.ExecContext(ctx, `
INSERT INTO worker_metrics_snapshots
  (worker_id, snapshotted_at, availability_percent, disconnects,
   jobs_succeeded, jobs_failed, failure_rate,
   restarts, rollback_count, current_image_digest, last_smoke_status,
   queue_ms_avg, render_ms_avg, render_ms_p95, download_ms_avg)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		snap.WorkerID,
		snap.SnapshottedAt.UTC().Format(time.RFC3339),
		avail,
		snap.Disconnects,
		snap.JobsSucceeded,
		snap.JobsFailed,
		failRate,
		snap.Restarts,
		snap.RollbackCount,
		digest,
		lastSmoke,
		snap.QueueMsAvg,
		snap.RenderMsAvg,
		snap.RenderMsP95,
		snap.DownloadMsAvg,
	)
	return err
}

// GetLatestWorkerMetricsForWorker returns the row with the highest
// snapshotted_at for the worker. Returns
// ErrWorkerMetricsSnapshotNotFound when no rows exist.
func (s *SQLiteStore) GetLatestWorkerMetricsForWorker(ctx context.Context, workerID string) (WorkerMetricsSnapshot, error) {
	if workerID == "" {
		return WorkerMetricsSnapshot{}, ErrWorkerMetricsSnapshotNotFound
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT snapshot_id, worker_id, snapshotted_at, availability_percent,
       disconnects, jobs_succeeded, jobs_failed, failure_rate,
       restarts, rollback_count, current_image_digest, last_smoke_status,
       queue_ms_avg, render_ms_avg, render_ms_p95, download_ms_avg
FROM worker_metrics_snapshots
WHERE worker_id = ?
ORDER BY snapshotted_at DESC, snapshot_id DESC
LIMIT 1`, workerID)
	if err != nil {
		return WorkerMetricsSnapshot{}, fmt.Errorf("get latest worker metrics: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return WorkerMetricsSnapshot{}, fmt.Errorf("get latest worker metrics: %w", err)
		}
		return WorkerMetricsSnapshot{}, ErrWorkerMetricsSnapshotNotFound
	}
	return scanWorkerMetricsSnapshot(rows)
}

// ListLatestWorkerMetrics returns the LATEST snapshot per worker_id
// across the fleet. The query is structured as a self-join on
// snapshotted_at to avoid the alternative "scan full table, pick max
// in Go" approach — the SQL-only aggregation is essential to keep
// the response under the per-package test-budget threshold and the
// dashboard's >1Hz poll rate.
//
// limit <= 0 means "no cap". The caller (handler) clamps to a sane
// upper bound (default 1000) before calling.
func (s *SQLiteStore) ListLatestWorkerMetrics(ctx context.Context, limit int) ([]WorkerMetricsSnapshot, error) {
	q := `
SELECT snapshot_id, worker_id, snapshotted_at, availability_percent,
       disconnects, jobs_succeeded, jobs_failed, failure_rate,
       restarts, rollback_count, current_image_digest, last_smoke_status,
       queue_ms_avg, render_ms_avg, render_ms_p95, download_ms_avg
FROM worker_metrics_snapshots AS wms
WHERE snapshot_id IN (
  SELECT MAX(snapshot_id) FROM worker_metrics_snapshots
   GROUP BY worker_id
)`
	args := []interface{}{}
	if limit > 0 {
		q += ` LIMIT ?`
		args = append(args, limit)
	}
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("list latest worker metrics: %w", err)
	}
	defer rows.Close()
	out := make([]WorkerMetricsSnapshot, 0, 8)
	for rows.Next() {
		snap, err := scanWorkerMetricsSnapshot(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, snap)
	}
	return out, rows.Err()
}

// scanWorkerMetricsSnapshot reads one row into a WorkerMetricsSnapshot.
// Tolerates NULL on availability_percent / failure_rate /
// current_image_digest / last_smoke_status (semantic "no data" —
// the dashboard distinguishes this from 0).
func scanWorkerMetricsSnapshot(rows *sql.Rows) (WorkerMetricsSnapshot, error) {
	var (
		snap          WorkerMetricsSnapshot
		snapshottedAt string
	)
	if err := rows.Scan(
		&snap.SnapshotID, &snap.WorkerID, &snapshottedAt,
		&snap.AvailabilityPercent,
		&snap.Disconnects, &snap.JobsSucceeded, &snap.JobsFailed,
		&snap.FailureRate,
		&snap.Restarts, &snap.RollbackCount,
		&snap.CurrentImageDigest, &snap.LastSmokeStatus,
		&snap.QueueMsAvg, &snap.RenderMsAvg, &snap.RenderMsP95,
		&snap.DownloadMsAvg,
	); err != nil {
		return snap, fmt.Errorf("scan worker metrics snapshot: %w", err)
	}
	parsed, err := parsePersistedWorkerTimestamp(snapshottedAt, "worker_metrics_snapshots.snapshotted_at")
	if err != nil {
		return snap, err
	}
	snap.SnapshottedAt = parsed
	return snap, nil
}

// PruneWorkerMetricsSnapshots deletes snapshot rows older than
// `days`. days <= 0 opts out of retention (no DELETE pass). Called
// by the metrics-snapshot-supervisor's same-tick hygiene alongside
// the existing pruneWorkerMetricSamples / pruneWorkerEvents helpers
// in store_worker_metrics.go.
func (s *SQLiteStore) PruneWorkerMetricsSnapshots(ctx context.Context, days int) error {
	if days <= 0 {
		return nil
	}
	if _, err := s.db.ExecContext(ctx,
		fmt.Sprintf(`DELETE FROM worker_metrics_snapshots WHERE snapshotted_at < DATETIME('now','-%d days')`, days),
	); err != nil {
		return fmt.Errorf("prune worker metrics snapshots: %w", err)
	}
	return nil
}

// nullableFloat returns the float64 when Valid, otherwise SQL NULL.
// Keeps the Insert helper free of branches + mirrors the existing
// pattern in store_deployment_records.go (boolToIntSQLite) and
// store_worker_metrics.go (nullableMetric).
func nullableFloat(n sql.NullFloat64) any {
	if n.Valid {
		return n.Float64
	}
	return nil
}

// nullableString returns the string when Valid, otherwise SQL NULL.
func nullableString(n sql.NullString) any {
	if n.Valid {
		return n.String
	}
	return nil
}
