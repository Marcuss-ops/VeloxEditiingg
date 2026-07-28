package fleet

// worker_metrics_aggregator.go — pure SQL aggregator producing the
// 13-metric fleet telemetry snapshot the dashboard reads at
// /api/v1/admin/workers/{id}/metrics and /api/v1/admin/workers/metrics.
//
// The aggregator is the boundary between the high-throughput
// operational tables (worker_metric_samples, fleet_operations,
// smoke_runs, deployment_records, worker_events) and the
// persistent analytics table (worker_metrics_snapshots, migration
// 105). It executes 8 SQL queries per worker — each is bounded
// by an index on its source table, so the 5-minute scheduler
// tick completes inside a few hundred ms even on a 4-worker
// fleet.
//
// Design decisions locked from the Step 13/15 thinker call:
//   - 24h sliding window for availability_percent / disconnects / failure_rate
//   - lifetime cumulative for jobs_succeeded / jobs_failed / restarts / rollback_count
//   - trailing-100 window for queue_ms / render_ms averages + p95
//   - download_ms_avg RESERVED for Step 14+ (returns 0)
//   - SQL-only aggregation (no in-Go aggregation)
//
// The aggregator depends on a narrow AggregatorDataSource
// interface that the production *store.SQLiteStore satisfies
// via the methods exposed in store_worker_metrics_snapshots.go
// (snapshot write) plus the existing source-table readers
// (smoke_runs, deployment_records, fleet_operations,
// worker_metric_samples). Tests inject an in-memory stub.

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"velox-server/internal/store"
)

// AggregatorDataSource is the narrow consumer-side interface the
// aggregator depends on. Production wires *store.SQLiteStore;
// tests inject an in-memory stub via the same shape.
type AggregatorDataSource interface {
	WorkerIDs(ctx context.Context) ([]string, error)
	InsertWorkerMetricsSnapshot(ctx context.Context, snap store.WorkerMetricsSnapshot) error
}

// ComputeAndPersistSnapshot runs the full aggregator for every
// known worker, persists one row per worker, and returns a
// summary count. Called by the bootstrap-composition
// metrics-snapshot-supervisor every 5 minutes.
//
// Errors are aggregated; the first per-worker error short-
// circuits the rest of the run so a partial fleet does not get
// snapshots while a downstream worker fails silently. The
// supervisor runner logs the count + first error for the
// operator dashboard.
func ComputeAndPersistSnapshot(ctx context.Context, ds AggregatorDataSource, db *sql.DB, now time.Time) (int, error) {
	workerIDs, err := ds.WorkerIDs(ctx)
	if err != nil {
		return 0, fmt.Errorf("aggregator: listing workers: %w", err)
	}
	persisted := 0
	for _, workerID := range workerIDs {
		snap, err := ComputeForWorker(ctx, db, workerID, now)
		if err != nil {
			return persisted, fmt.Errorf("aggregator: worker=%s: %w", workerID, err)
		}
		if err := ds.InsertWorkerMetricsSnapshot(ctx, snap); err != nil {
			return persisted, fmt.Errorf("aggregator: insert worker=%s: %w", workerID, err)
		}
		persisted++
	}
	return persisted, nil
}

// ComputeForWorker runs the 8 aggregation queries for one worker
// and returns the resulting snapshot row. Exposed (not just
// ComputeAndPersistSnapshot) so the aggregated handler can call
// it on-demand when the persisted snapshot is older than the
// freshness window — operator dashboards tolerate a stale read
// at most until the next scheduler tick.
func ComputeForWorker(ctx context.Context, db *sql.DB, workerID string, now time.Time) (store.WorkerMetricsSnapshot, error) {
	availPct, err := computeAvailabilityPct(ctx, db, workerID, now)
	if err != nil {
		return store.WorkerMetricsSnapshot{}, fmt.Errorf("availability: %w", err)
	}
	disconnects, err := computeDisconnects(ctx, db, workerID, now)
	if err != nil {
		return store.WorkerMetricsSnapshot{}, fmt.Errorf("disconnects: %w", err)
	}
	jobsOK, jobsFail, failureRate, err := computeJobsAndFailure(ctx, db, workerID)
	if err != nil {
		return store.WorkerMetricsSnapshot{}, fmt.Errorf("jobs: %w", err)
	}
	queueMsAvg, err := computeQueueMsAvg(ctx, db, workerID)
	if err != nil {
		return store.WorkerMetricsSnapshot{}, fmt.Errorf("queue_ms: %w", err)
	}
	renderAvg, renderP95, err := computeRenderMs(ctx, db, workerID)
	if err != nil {
		return store.WorkerMetricsSnapshot{}, fmt.Errorf("render_ms: %w", err)
	}
	restarts, rollbackCount, err := computeRestartsAndRollback(ctx, db, workerID)
	if err != nil {
		return store.WorkerMetricsSnapshot{}, fmt.Errorf("restarts/rollback: %w", err)
	}
	curImg, err := computeCurrentImageDigest(ctx, db, workerID)
	if err != nil {
		return store.WorkerMetricsSnapshot{}, fmt.Errorf("current_image_digest: %w", err)
	}
	lastSmoke, err := computeLastSmokeStatus(ctx, db, workerID)
	if err != nil {
		return store.WorkerMetricsSnapshot{}, fmt.Errorf("last_smoke_status: %w", err)
	}
	return store.WorkerMetricsSnapshot{
		WorkerID:            workerID,
		SnapshottedAt:       now,
		AvailabilityPercent: availPct,
		Disconnects:         disconnects,
		JobsSucceeded:       jobsOK,
		JobsFailed:          jobsFail,
		FailureRate:         failureRate,
		Restarts:            restarts,
		RollbackCount:       rollbackCount,
		CurrentImageDigest:  curImg,
		LastSmokeStatus:     lastSmoke,
		QueueMsAvg:          queueMsAvg,
		RenderMsAvg:         renderAvg,
		RenderMsP95:         renderP95,
		DownloadMsAvg:       0, // RESERVED Step 14+: phase-level columns
	}, nil
}

// computeAvailabilityPct counts CONNECTED samples over the 24h
// window. Returns Null when zero samples (avoids 0.0 ambiguity).
func computeAvailabilityPct(ctx context.Context, db *sql.DB, workerID string, now time.Time) (sql.NullFloat64, error) {
	cutoff := now.UTC().Add(-24 * time.Hour).Format(time.RFC3339)
	var pct sql.NullFloat64
	err := db.QueryRowContext(ctx, `
SELECT (SUM(CASE WHEN connection_status='CONNECTED' THEN 1 ELSE 0 END) * 100.0 / COUNT(*))
FROM worker_metric_samples
WHERE worker_id = ? AND sampled_at >= ?`, workerID, cutoff).Scan(&pct)
	return pct, err
}

// computeDisconnects counts DISCONNECTED samples over the 24h
// window. Per Q2: 24h sliding. Returns 0 (not null) when zero
// samples — disconnects is a counter, zero IS meaningful.
func computeDisconnects(ctx context.Context, db *sql.DB, workerID string, now time.Time) (int64, error) {
	cutoff := now.UTC().Add(-24 * time.Hour).Format(time.RFC3339)
	var n int64
	err := db.QueryRowContext(ctx, `
SELECT COUNT(*) FROM worker_metric_samples
WHERE worker_id = ? AND sampled_at >= ? AND connection_status='DISCONNECTED'`,
		workerID, cutoff).Scan(&n)
	return n, err
}

// computeJobsAndFailure returns lifetime cumulative counts from
// fleet_operations (kind != 'smoke' — smoke counts are surfaced
// separately via smoke_runs). failureRate is NULL when total == 0.
func computeJobsAndFailure(ctx context.Context, db *sql.DB, workerID string) (int64, int64, sql.NullFloat64, error) {
	var ok, fail int64
	err := db.QueryRowContext(ctx, `
SELECT
  COALESCE(SUM(CASE WHEN status='SUCCEEDED' THEN 1 ELSE 0 END), 0),
  COALESCE(SUM(CASE WHEN status='FAILED'    THEN 1 ELSE 0 END), 0)
FROM fleet_operations
WHERE worker_id = ? AND op != 'smoke'`, workerID).Scan(&ok, &fail)
	if err != nil {
		return 0, 0, sql.NullFloat64{}, err
	}
	var rate sql.NullFloat64
	if total := ok + fail; total > 0 {
		rate = sql.NullFloat64{Float64: float64(fail) * 100.0 / float64(total), Valid: true}
	}
	return ok, fail, rate, nil
}

// computeQueueMsAvg returns the average queue→start duration
// across the trailing-100 fleet_operations rows for the worker.
// Uses SQLite julianday arithmetic to get milliseconds between
// RFC3339 strings: (julianday(end) - julianday(start)) * 86400000.
func computeQueueMsAvg(ctx context.Context, db *sql.DB, workerID string) (int64, error) {
	var avg sql.NullFloat64
	err := db.QueryRowContext(ctx, `
SELECT AVG((julianday(started_at) - julianday(queued_at)) * 86400000.0)
FROM (
  SELECT started_at, queued_at FROM fleet_operations
   WHERE worker_id = ? AND started_at IS NOT NULL AND queued_at IS NOT NULL
   ORDER BY queued_at DESC LIMIT 100
)`, workerID).Scan(&avg)
	if !avg.Valid {
		return 0, err
	}
	return int64(avg.Float64), err
}

// computeRenderMs returns avg + p95 of smoke_runs.duration_ms
// across the trailing-100 SUCCEEDED rows. p95 workaround for
// SQLite's missing PERCENTILE_CONT: ORDER BY duration_ms ASC then
// LIMIT 1 OFFSET (count*95/100).
func computeRenderMs(ctx context.Context, db *sql.DB, workerID string) (int64, int64, error) {
	var avg sql.NullFloat64
	err := db.QueryRowContext(ctx, `
SELECT AVG(duration_ms) FROM (
  SELECT duration_ms FROM smoke_runs
   WHERE worker_id = ? AND status='SUCCEEDED'
   ORDER BY started_at DESC LIMIT 100
)`, workerID).Scan(&avg)
	if err != nil {
		return 0, 0, err
	}
	avgMs := int64(0)
	if avg.Valid {
		avgMs = int64(avg.Float64)
	}
	// p95 — order ascending, skip first 95% of rows.
	var p95 sql.NullInt64
	err = db.QueryRowContext(ctx, `
SELECT duration_ms FROM smoke_runs
 WHERE worker_id = ? AND status='SUCCEEDED'
 ORDER BY duration_ms ASC
 LIMIT 1 OFFSET (
   SELECT CAST(COUNT(*) * 95 / 100 AS INTEGER)
     FROM smoke_runs
    WHERE worker_id = ? AND status='SUCCEEDED'
 )`, workerID, workerID).Scan(&p95)
	if err != nil && err != sql.ErrNoRows {
		return avgMs, 0, err
	}
	p95Ms := int64(0)
	if p95.Valid {
		p95Ms = p95.Int64
	}
	return avgMs, p95Ms, nil
}

// computeRestartsAndRollback returns lifetime counts from
// deployment_records: restarts = ROLLED_BACK rows; rollback_count
// = is_rollback=1 rows regardless of status.
func computeRestartsAndRollback(ctx context.Context, db *sql.DB, workerID string) (int64, int64, error) {
	var restarts, rollbacks int64
	err := db.QueryRowContext(ctx, `
SELECT
  COALESCE(SUM(CASE WHEN status='ROLLED_BACK' THEN 1 ELSE 0 END), 0),
  COALESCE(SUM(CASE WHEN is_rollback=1      THEN 1 ELSE 0 END), 0)
FROM deployment_records
WHERE worker_id = ?`, workerID).Scan(&restarts, &rollbacks)
	if err != nil {
		return 0, 0, err
	}
	return restarts, rollbacks, nil
}

// computeCurrentImageDigest returns the most recent SUCCEEDED
// deployment's target_digest for the worker. NULL when no
// SUCCEEDED deploy exists.
func computeCurrentImageDigest(ctx context.Context, db *sql.DB, workerID string) (sql.NullString, error) {
	var dgst sql.NullString
	err := db.QueryRowContext(ctx, `
	SELECT target_digest FROM deployment_records
 WHERE worker_id = ? AND status='SUCCEEDED'
 ORDER BY finished_at DESC LIMIT 1`, workerID).Scan(&dgst)
	if err == sql.ErrNoRows {
		return sql.NullString{}, nil
	}
	return dgst, err
}

// computeLastSmokeStatus returns the most recent smoke_runs row's
// status for the worker. NULL when the worker has never been
// smoke-tested.
func computeLastSmokeStatus(ctx context.Context, db *sql.DB, workerID string) (sql.NullString, error) {
	var st sql.NullString
	err := db.QueryRowContext(ctx, `
SELECT status FROM smoke_runs
 WHERE worker_id = ?
 ORDER BY finished_at DESC LIMIT 1`, workerID).Scan(&st)
	if err == sql.ErrNoRows {
		return sql.NullString{}, nil
	}
	return st, err
}

// SQLiteWorkerIDs is the production AggregatorDataSource
// implementation backed by the workers registry table on the
// same SQLite handle. Returns distinct worker_ids from
// worker_metric_samples as a portable signal of "ever-registered
// workers" — using a separate registry table (Phase 2+ worker
// table from migration 020_worker_control_plane.sql) instead
// would lose the convenience of having one source of truth for
// fleet telemetry computation; the registry table is the source
// of authoritative worker_id set.
type SQLiteWorkerIDs struct {
	DB *sql.DB
}

func (s SQLiteWorkerIDs) WorkerIDs(ctx context.Context) ([]string, error) {
	// Authoritative source: the workers control-plane table
	// (migration 020). If empty, fall back to worker_metric_samples
	// so the aggregator self-bootstraps when telemetry is the
	// only signal of a worker's existence (e.g., a worker that
	// has been heart-beating but never registered via V2).
	rows, err := s.DB.QueryContext(ctx, `SELECT worker_id FROM workers`)
	if err != nil {
		return nil, fmt.Errorf("list worker ids from workers table: %w", err)
	}
	defer rows.Close()
	out := make([]string, 0, 8)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan worker id: %w", err)
		}
		out = append(out, id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(out) > 0 {
		return out, nil
	}
	// Bootstrap fallback.
	rows2, err := s.DB.QueryContext(ctx,
		`SELECT DISTINCT worker_id FROM worker_metric_samples ORDER BY worker_id`)
	if err != nil {
		return nil, fmt.Errorf("list worker ids from samples: %w", err)
	}
	defer rows2.Close()
	for rows2.Next() {
		var id string
		if err := rows2.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows2.Err()
}

// WorkerMetricsAggregatorDataSource is the production binding
// used in bootstrap_composition.go. It extends the Aggregator
// DataSource interface to include store write/read helpers used
// by the snapshot scheduler.
type WorkerMetricsAggregatorDataSource struct {
	Store interface {
		InsertWorkerMetricsSnapshot(ctx context.Context, snap store.WorkerMetricsSnapshot) error
		ListLatestWorkerMetrics(ctx context.Context, limit int) ([]store.WorkerMetricsSnapshot, error)
		GetLatestWorkerMetricsForWorker(ctx context.Context, workerID string) (store.WorkerMetricsSnapshot, error)
	}
	WorkerIDsFn func(ctx context.Context) ([]string, error)
}

// WorkerIDs returns the authoritative set of worker_ids the
// aggregator ticks on each call. Delegates to WorkerIDsFn; the
// function pointer keeps the surface narrow (no SQLite dependency
// leaked into the interface).
func (s WorkerMetricsAggregatorDataSource) WorkerIDs(ctx context.Context) ([]string, error) {
	if s.WorkerIDsFn == nil {
		return nil, fmt.Errorf("WorkerMetricsAggregatorDataSource: WorkerIDsFn nil")
	}
	return s.WorkerIDsFn(ctx)
}

// InsertWorkerMetricsSnapshot persists one snapshot row.
func (s WorkerMetricsAggregatorDataSource) InsertWorkerMetricsSnapshot(ctx context.Context, snap store.WorkerMetricsSnapshot) error {
	if s.Store == nil {
		return fmt.Errorf("WorkerMetricsAggregatorDataSource: Store nil")
	}
	return s.Store.InsertWorkerMetricsSnapshot(ctx, snap)
}
