package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// store_worker_metrics.go owns the worker_metric_samples table:
// throttled sample insertion (1 min idle / 15 s busy), the
// retention prune for both worker_metric_samples and
// worker_events, and the read query used by the operator-facing
// HTTP endpoint.
//
// All write helpers (maybeInsertWorkerMetric + the two prune
// helpers) receive *sql.Tx from the caller and never open their
// own transaction. The read query (ListWorkerMetrics) takes
// *sql.DB directly because it is called from the HTTP handler
// path which has no caller-supplied tx.

// WorkerMetricSampleRow is the read-side row shape returned by
// ListWorkerMetrics. Fields with no NOT NULL constraint at the
// schema level (load_average, process_rss_bytes, network_rx_bytes,
// network_tx_bytes) are typed as sql.Null* so the SQL NULL is
// preserved through the scan and the handler can decide whether
// to surface the field as 0 / omitted / explicit null.
//
// Mirrors the worker_metric_samples schema (migration 094):
//
//	id                       INTEGER PRIMARY KEY AUTOINCREMENT
//	worker_id                TEXT NOT NULL
//	session_id               TEXT
//	sampled_at               TEXT NOT NULL
//	connection_status        TEXT NOT NULL
//	active_tasks             INTEGER NOT NULL
//	task_slots               INTEGER NOT NULL
//	cpu_utilization_ratio    REAL NOT NULL
//	memory_used_bytes        INTEGER NOT NULL
//	disk_free_bytes          INTEGER NOT NULL
//	load_average             REAL                 (nullable)
//	process_rss_bytes        INTEGER              (nullable)
//	network_rx_bytes         INTEGER              (nullable)
//	network_tx_bytes         INTEGER              (nullable)
type WorkerMetricSampleRow struct {
	ID                  int64
	WorkerID            string
	SessionID           sql.NullString
	SampledAt           string
	ConnectionStatus    string
	ActiveTasks         int64
	TaskSlots           int64
	CPUUtilizationRatio float64
	MemoryUsedBytes     int64
	DiskFreeBytes       int64
	LoadAverage         sql.NullFloat64
	ProcessRSSBytes     sql.NullInt64
	NetworkRxBytes      sql.NullInt64
	NetworkTxBytes      sql.NullInt64
}

// ListWorkerMetrics returns up to `limit` metric samples for
// `workerID`, newest first (ORDER BY sampled_at DESC).
//
// Parameters:
//
//	workerID  — exact match on the worker_id column. Empty string
//	            returns an empty slice (defense against a Gin handler
//	            reading the :worker_id param as empty).
//	since     — optional RFC3339 lower bound on sampled_at. Pass ""
//	            to disable the lower-bound filter (returns the most
//	            recent `limit` rows regardless of age). The handler
//	            layer is responsible for the "last 24h" default.
//	limit     — caller-supplied upper bound. The handler clamps to
//	            the [1, 1000] range before calling here; this
//	            function still enforces a sane floor (limit < 1
//	            returns the default 100) so a misuse from non-handler
//	            callers does not accidentally pull the whole table.
//
// The function NEVER mutates state. It is safe to call concurrently
// from multiple Gin handlers. Read consistency is per-row (no
// transaction wrapping) because each row is independently meaningful
// for time-series visualization; a cross-row snapshot is not part
// of the contract.
func ListWorkerMetrics(ctx context.Context, db *sql.DB, workerID, since string, limit int) ([]WorkerMetricSampleRow, error) {
	if workerID == "" {
		return []WorkerMetricSampleRow{}, nil
	}
	if limit < 1 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}
	args := []interface{}{workerID}
	q := `SELECT id, worker_id, session_id, sampled_at, connection_status,
		       active_tasks, task_slots, cpu_utilization_ratio,
		       memory_used_bytes, disk_free_bytes,
		       load_average, process_rss_bytes, network_rx_bytes, network_tx_bytes
		  FROM worker_metric_samples
		 WHERE worker_id = ?`
	if strings.TrimSpace(since) != "" {
		q += ` AND sampled_at >= ?`
		args = append(args, since)
	}
	q += ` ORDER BY sampled_at DESC, id DESC LIMIT ?`
	args = append(args, limit)
	rows, err := db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("list worker metrics: %w", err)
	}
	defer rows.Close()
	out := make([]WorkerMetricSampleRow, 0, limit)
	for rows.Next() {
		var r WorkerMetricSampleRow
		if err := rows.Scan(
			&r.ID, &r.WorkerID, &r.SessionID, &r.SampledAt, &r.ConnectionStatus,
			&r.ActiveTasks, &r.TaskSlots, &r.CPUUtilizationRatio,
			&r.MemoryUsedBytes, &r.DiskFreeBytes,
			&r.LoadAverage, &r.ProcessRSSBytes, &r.NetworkRxBytes, &r.NetworkTxBytes,
		); err != nil {
			return nil, fmt.Errorf("list worker metrics: scan: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list worker metrics: rows: %w", err)
	}
	return out, nil
}

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
