package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// ComputeWorkerMetricsSnapshot computes the canonical fleet metrics snapshot
// for one worker. All source-table SQL for this projection belongs to store;
// callers receive only the typed WorkerMetricsSnapshot value.
func (s *SQLiteStore) ComputeWorkerMetricsSnapshot(ctx context.Context, workerID string, now time.Time) (WorkerMetricsSnapshot, error) {
	if s == nil || s.db == nil {
		return WorkerMetricsSnapshot{}, fmt.Errorf("worker metrics aggregation: store not initialized")
	}
	avail, err := workerAvailabilityPct(ctx, s.db, workerID, now)
	if err != nil {
		return WorkerMetricsSnapshot{}, fmt.Errorf("availability: %w", err)
	}
	disconnects, err := workerDisconnects(ctx, s.db, workerID, now)
	if err != nil {
		return WorkerMetricsSnapshot{}, fmt.Errorf("disconnects: %w", err)
	}
	jobsOK, jobsFailed, failureRate, err := workerJobsAndFailure(ctx, s.db, workerID)
	if err != nil {
		return WorkerMetricsSnapshot{}, fmt.Errorf("jobs: %w", err)
	}
	queueAvg, err := workerQueueMSAvg(ctx, s.db, workerID)
	if err != nil {
		return WorkerMetricsSnapshot{}, fmt.Errorf("queue_ms: %w", err)
	}
	renderAvg, renderP95, err := workerRenderMS(ctx, s.db, workerID)
	if err != nil {
		return WorkerMetricsSnapshot{}, fmt.Errorf("render_ms: %w", err)
	}
	restarts, rollbacks, err := workerRestartsAndRollback(ctx, s.db, workerID)
	if err != nil {
		return WorkerMetricsSnapshot{}, fmt.Errorf("restarts/rollback: %w", err)
	}
	digest, err := workerCurrentImageDigest(ctx, s.db, workerID)
	if err != nil {
		return WorkerMetricsSnapshot{}, fmt.Errorf("current_image_digest: %w", err)
	}
	smoke, err := workerLastSmokeStatus(ctx, s.db, workerID)
	if err != nil {
		return WorkerMetricsSnapshot{}, fmt.Errorf("last_smoke_status: %w", err)
	}
	return WorkerMetricsSnapshot{
		WorkerID: workerID, SnapshottedAt: now,
		AvailabilityPercent: avail, Disconnects: disconnects,
		JobsSucceeded: jobsOK, JobsFailed: jobsFailed, FailureRate: failureRate,
		Restarts: restarts, RollbackCount: rollbacks,
		CurrentImageDigest: digest, LastSmokeStatus: smoke,
		QueueMsAvg: queueAvg, RenderMsAvg: renderAvg, RenderMsP95: renderP95,
		DownloadMsAvg: 0,
	}, nil
}

// WorkerIDs returns the authoritative worker set, falling back to telemetry
// samples when the control-plane table is empty during bootstrap.
func (s *SQLiteStore) WorkerIDs(ctx context.Context) ([]string, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("worker metrics aggregation: store not initialized")
	}
	rows, err := s.db.QueryContext(ctx, `SELECT worker_id FROM workers`)
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
	rows, err = s.db.QueryContext(ctx, `SELECT DISTINCT worker_id FROM worker_metric_samples ORDER BY worker_id`)
	if err != nil {
		return nil, fmt.Errorf("list worker ids from samples: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

func workerAvailabilityPct(ctx context.Context, db *sql.DB, workerID string, now time.Time) (sql.NullFloat64, error) {
	cutoff := now.UTC().Add(-24 * time.Hour).Format(time.RFC3339)
	var pct sql.NullFloat64
	err := db.QueryRowContext(ctx, `SELECT (SUM(CASE WHEN connection_status='CONNECTED' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) FROM worker_metric_samples WHERE worker_id = ? AND sampled_at >= ?`, workerID, cutoff).Scan(&pct)
	return pct, err
}

func workerDisconnects(ctx context.Context, db *sql.DB, workerID string, now time.Time) (int64, error) {
	cutoff := now.UTC().Add(-24 * time.Hour).Format(time.RFC3339)
	var count int64
	err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM worker_metric_samples WHERE worker_id = ? AND sampled_at >= ? AND connection_status='DISCONNECTED'`, workerID, cutoff).Scan(&count)
	return count, err
}

func workerJobsAndFailure(ctx context.Context, db *sql.DB, workerID string) (int64, int64, sql.NullFloat64, error) {
	var succeeded, failed int64
	err := db.QueryRowContext(ctx, `SELECT COALESCE(SUM(CASE WHEN status='SUCCEEDED' THEN 1 ELSE 0 END), 0), COALESCE(SUM(CASE WHEN status='FAILED' THEN 1 ELSE 0 END), 0) FROM fleet_operations WHERE worker_id = ? AND op != 'smoke'`, workerID).Scan(&succeeded, &failed)
	if err != nil {
		return 0, 0, sql.NullFloat64{}, err
	}
	var rate sql.NullFloat64
	if total := succeeded + failed; total > 0 {
		rate = sql.NullFloat64{Float64: float64(failed) * 100 / float64(total), Valid: true}
	}
	return succeeded, failed, rate, nil
}

func workerQueueMSAvg(ctx context.Context, db *sql.DB, workerID string) (int64, error) {
	var avg sql.NullFloat64
	err := db.QueryRowContext(ctx, `SELECT AVG((julianday(started_at) - julianday(queued_at)) * 86400000.0) FROM (SELECT started_at, queued_at FROM fleet_operations WHERE worker_id = ? AND started_at IS NOT NULL AND queued_at IS NOT NULL ORDER BY queued_at DESC LIMIT 100)`, workerID).Scan(&avg)
	if !avg.Valid {
		return 0, err
	}
	return int64(avg.Float64), err
}

func workerRenderMS(ctx context.Context, db *sql.DB, workerID string) (int64, int64, error) {
	var avg sql.NullFloat64
	if err := db.QueryRowContext(ctx, `SELECT AVG(duration_ms) FROM (SELECT duration_ms FROM smoke_runs WHERE worker_id = ? AND status='SUCCEEDED' ORDER BY started_at DESC LIMIT 100)`, workerID).Scan(&avg); err != nil {
		return 0, 0, err
	}
	avgMS := int64(0)
	if avg.Valid {
		avgMS = int64(avg.Float64)
	}
	var p95 sql.NullInt64
	err := db.QueryRowContext(ctx, `SELECT duration_ms FROM smoke_runs WHERE worker_id = ? AND status='SUCCEEDED' ORDER BY duration_ms ASC LIMIT 1 OFFSET (SELECT CAST(COUNT(*) * 95 / 100 AS INTEGER) FROM smoke_runs WHERE worker_id = ? AND status='SUCCEEDED')`, workerID, workerID).Scan(&p95)
	if err != nil && err != sql.ErrNoRows {
		return avgMS, 0, err
	}
	if !p95.Valid {
		return avgMS, 0, nil
	}
	return avgMS, p95.Int64, nil
}

func workerRestartsAndRollback(ctx context.Context, db *sql.DB, workerID string) (int64, int64, error) {
	var restarts, rollbacks int64
	err := db.QueryRowContext(ctx, `SELECT COALESCE(SUM(CASE WHEN status='ROLLED_BACK' THEN 1 ELSE 0 END), 0), COALESCE(SUM(CASE WHEN is_rollback=1 THEN 1 ELSE 0 END), 0) FROM deployment_records WHERE worker_id = ?`, workerID).Scan(&restarts, &rollbacks)
	return restarts, rollbacks, err
}

func workerCurrentImageDigest(ctx context.Context, db *sql.DB, workerID string) (sql.NullString, error) {
	var digest sql.NullString
	err := db.QueryRowContext(ctx, `SELECT target_digest FROM deployment_records WHERE worker_id = ? AND status='SUCCEEDED' ORDER BY finished_at DESC LIMIT 1`, workerID).Scan(&digest)
	if err == sql.ErrNoRows {
		return sql.NullString{}, nil
	}
	return digest, err
}

func workerLastSmokeStatus(ctx context.Context, db *sql.DB, workerID string) (sql.NullString, error) {
	var status sql.NullString
	err := db.QueryRowContext(ctx, `SELECT status FROM smoke_runs WHERE worker_id = ? ORDER BY finished_at DESC LIMIT 1`, workerID).Scan(&status)
	if err == sql.ErrNoRows {
		return sql.NullString{}, nil
	}
	return status, err
}
