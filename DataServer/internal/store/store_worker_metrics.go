package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// store_worker_metrics.go owns the worker_metric_samples table:
// throttled sample insertion (1 min idle / 15 s busy) and the
// retention prune for both worker_metric_samples and
// worker_events. All helpers receive *sql.Tx from the caller and
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

// pruneWorkerMetricSamples retains the configured retention window
// for worker_metric_samples. The window is configurable via
// WorkersConfig (caller passes the days from the Store's stored
// retention config).
//
// Behavior:
//   - days <= 0     → opt-out: skip the DELETE pass entirely so a
//     deployment that explicitly disabled retention
//     never accidentally deletes its audit table.
//   - days  > 0     → DELETE rows with sampled_at < DATETIME('now',
//     '-<days> days').
//
// days is interpolated into the SQL via fmt.Sprintf. Safe because
// days is an int validated by config.intFromEnv(... , default, 1)
// so the lower bound is enforced at the config layer.
func pruneWorkerMetricSamples(ctx context.Context, tx *sql.Tx, days int) error {
	if days <= 0 {
		return nil
	}
	if _, err := tx.ExecContext(ctx,
		fmt.Sprintf(`DELETE FROM worker_metric_samples WHERE sampled_at < DATETIME('now','-%d days')`, days),
	); err != nil {
		return fmt.Errorf("prune worker metric samples: %w", err)
	}
	return nil
}

// pruneWorkerEvents retains the configured retention window for
// the worker_events audit ledger. The window is configurable via
// WorkersConfig.WorkersRetention config (caller passes the days).
//
// Behavior mirrors pruneWorkerMetricSamples:
//   - days <= 0     → opt-out: skip the DELETE pass.
//   - days  > 0     → DELETE rows with created_at < DATETIME('now',
//     '-<days> days').
//
// Workers config the audit trail of state transitions, partition
// detection, and runtime disappearance events. 30 days is the
// default; production deployments with stricter audit requirements
// can extend it via VELOX_RETENTION_WORKER_EVENTS_DAYS.
func pruneWorkerEvents(ctx context.Context, tx *sql.Tx, days int) error {
	if days <= 0 {
		return nil
	}
	if _, err := tx.ExecContext(ctx,
		fmt.Sprintf(`DELETE FROM worker_events WHERE created_at < DATETIME('now','-%d days')`, days),
	); err != nil {
		return fmt.Errorf("prune worker events: %w", err)
	}
	return nil
}
