package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// store_worker_metrics.go owns the worker_metric_samples table:
// throttled sample insertion (1 min idle / 15 s busy) and 7-day
// retention prune. Both helpers receive *sql.Tx from the caller and
// never open their own transaction.

func maybeInsertWorkerMetric(ctx context.Context, tx *sql.Tx, m map[string]any, workerID, sessionID string, changed bool, now string) error {
	var last string
	_ = tx.QueryRowContext(ctx, `SELECT sampled_at FROM worker_metric_samples WHERE worker_id=? ORDER BY sampled_at DESC LIMIT 1`, workerID).Scan(&last)
	lastAt, _ := time.Parse(time.RFC3339Nano, last)
	active := workerActiveTaskCount(m, func(key string) any {
		metrics, _ := m["metrics"].(map[string]any)
		return metrics[key]
	})
	interval := time.Minute
	if active > 0 || changed {
		interval = 15 * time.Second
	}
	if !changed && !lastAt.IsZero() && time.Since(lastAt) < interval {
		return nil
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO worker_metric_samples
		(worker_id,session_id,sampled_at,connection_status,active_tasks,task_slots,cpu_utilization_ratio,memory_used_bytes,disk_free_bytes)
		VALUES (?,?,?,?,?,?,?,?,?)`, workerID, sessionID, now, asString(m["status"]), active,
		int64OrDefault(m["task_slots"], 1), floatValue(m["cpu_utilization_ratio"]), int64Value(m["memory_used_bytes"]), int64Value(m["disk_free_bytes"]))
	return err
}

// pruneWorkerMetricSamples retains the 7-day retention window for
// worker_metric_samples. Pure SQL-side retention: SQLite's
// DATETIME('now','-7 days') is the wallclock, no Go-side parameter
// is needed or honoured.
func pruneWorkerMetricSamples(ctx context.Context, tx *sql.Tx) error {
	if _, err := tx.ExecContext(ctx, `DELETE FROM worker_metric_samples WHERE sampled_at < DATETIME('now','-7 days')`); err != nil {
		return fmt.Errorf("prune worker metric samples: %w", err)
	}
	return nil
}
