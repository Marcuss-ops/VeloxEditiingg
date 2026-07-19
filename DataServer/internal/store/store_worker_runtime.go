package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

// PersistWorkerHeartbeat writes the structured projections associated with a
// heartbeat in one SQLite transaction. workers.raw_json remains a compatibility
// snapshot; runtime rows are volatile and are reconciled from active_jobs.
func (s *SQLiteStore) PersistWorkerHeartbeat(ctx context.Context, raw []byte, sessionID string) error {
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		return err
	}
	workerID := asString(m["worker_id"])
	if workerID == "" {
		return fmt.Errorf("persist worker heartbeat: missing worker_id")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var oldStatus, oldJob string
	_ = tx.QueryRowContext(ctx, `SELECT status,current_job FROM workers WHERE worker_id=?`, workerID).Scan(&oldStatus, &oldJob)
	if err := s.upsertWorkerExec(tx, m, raw, now); err != nil {
		return fmt.Errorf("persist worker snapshot: %w", err)
	}
	if sessionID != "" {
		if _, err := tx.ExecContext(ctx, `UPDATE worker_sessions
			SET last_seen=?, last_seen_at=?, status='ACTIVE', revoked=0
			WHERE session_id=?`, now, now, sessionID); err != nil {
			return fmt.Errorf("touch worker session: %w", err)
		}
	}

	active := activeTasksFromSnapshot(m)
	if err := reconcileWorkerRuntime(ctx, tx, workerID, sessionID, active, now); err != nil {
		return err
	}
	if err := maybeInsertWorkerMetric(ctx, tx, m, workerID, sessionID, oldStatus != asString(m["status"]), now); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM worker_metric_samples WHERE sampled_at < DATETIME('now','-7 days')`); err != nil {
		return fmt.Errorf("prune worker metric samples: %w", err)
	}
	if oldStatus != "" && (oldStatus != asString(m["status"]) || oldJob != asString(m["current_job"])) {
		details, _ := json.Marshal(map[string]any{"old_status": oldStatus, "new_status": m["status"], "old_job": oldJob, "new_job": m["current_job"]})
		if _, err := tx.ExecContext(ctx, `INSERT INTO worker_events
			(event_id,worker_id,session_id,event_type,severity,details_json,created_at)
			VALUES (?,?,?,?,?,?,?)`, uuid.NewString(), workerID, sessionID,
			"WORKER_STATE_CHANGED", "INFO", string(details), now); err != nil {
			return fmt.Errorf("append worker event: %w", err)
		}
	}
	return tx.Commit()
}

// DeleteWorkerTaskRuntime removes the volatile runtime projection after the
// canonical TaskResult transaction has closed the attempt. The task/attempt
// tables remain the durable history; this table is only the live view.
func (s *SQLiteStore) DeleteWorkerTaskRuntime(taskID, attemptID string) error {
	if taskID == "" {
		return nil
	}
	if attemptID == "" {
		_, err := s.db.Exec(`DELETE FROM worker_task_runtime WHERE task_id=?`, taskID)
		return err
	}
	_, err := s.db.Exec(`DELETE FROM worker_task_runtime WHERE task_id=? AND attempt_id=?`, taskID, attemptID)
	return err
}

func activeTasksFromSnapshot(m map[string]any) []map[string]any {
	metrics, _ := m["metrics"].(map[string]any)
	if metrics == nil {
		return nil
	}
	value, _ := metrics["active_jobs"]
	items, _ := value.([]any)
	result := make([]map[string]any, 0, len(items))
	for _, item := range items {
		if task, ok := item.(map[string]any); ok && asString(task["job_id"]) != "" {
			result = append(result, task)
		}
	}
	return result
}

func reconcileWorkerRuntime(ctx context.Context, tx *sql.Tx, workerID, sessionID string, active []map[string]any, now string) error {
	seen := make(map[string]bool, len(active))
	for _, task := range active {
		taskID := asString(task["task_id"])
		if taskID == "" {
			// Older workers advertised only job_id; do not manufacture a
			// runtime identity that could be confused with task_attempts.
			continue
		}
		seen[taskID] = true
		// Heartbeats can race with TaskResult delivery. Never recreate a
		// volatile runtime row from a late heartbeat after the canonical
		// attempt has already reached a terminal state.
		var attemptStatus string
		attemptErr := tx.QueryRowContext(ctx, `SELECT status FROM task_attempts WHERE id=?`, asString(task["attempt_id"])).Scan(&attemptStatus)
		if attemptErr == nil && attemptStatus != "LEASED" && attemptStatus != "RUNNING" {
			delete(seen, taskID)
			if _, err := tx.ExecContext(ctx, `DELETE FROM worker_task_runtime WHERE task_id=? AND attempt_id=?`, taskID, asString(task["attempt_id"])); err != nil {
				return err
			}
			continue
		}
		_, err := tx.ExecContext(ctx, `INSERT INTO worker_task_runtime
			(task_id,job_id,attempt_id,attempt_number,worker_id,session_id,lease_id,
			executor_id,executor_version,runtime_status,progress_percent,progress_stage,
			current_scene,total_scenes,started_at,last_progress_at,updated_at)
			VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
			ON CONFLICT(task_id) DO UPDATE SET
			job_id=excluded.job_id, attempt_id=excluded.attempt_id, attempt_number=excluded.attempt_number,
			worker_id=excluded.worker_id, session_id=excluded.session_id, lease_id=excluded.lease_id,
			executor_id=excluded.executor_id, executor_version=excluded.executor_version,
			runtime_status=excluded.runtime_status, progress_percent=excluded.progress_percent,
			progress_stage=excluded.progress_stage, current_scene=excluded.current_scene,
			total_scenes=excluded.total_scenes, started_at=COALESCE(worker_task_runtime.started_at, excluded.started_at),
			last_progress_at=excluded.last_progress_at, updated_at=excluded.updated_at,
			missing_heartbeats=0`,
			taskID, asString(task["job_id"]), asString(task["attempt_id"]), int64OrDefault(task["attempt"], 1),
			workerID, sessionID, asString(task["lease_id"]), asString(task["job_type"]), 0,
			defaultString(task["status"], "RUNNING"), clampPercent(int64Value(task["progress_percent"])),
			asString(task["progress_stage"]), int64Value(task["progress_scene"]), int64Value(task["progress_total"]),
			defaultString(task["started_at"], now), now, now)
		if err != nil {
			return fmt.Errorf("upsert worker task runtime %s: %w", taskID, err)
		}
	}
	if len(seen) == 0 {
		if _, err := tx.ExecContext(ctx, `UPDATE worker_task_runtime
			SET missing_heartbeats=missing_heartbeats+1, updated_at=? WHERE worker_id=?`, now, workerID); err != nil {
			return err
		}
	} else {
		placeholders := strings.TrimRight(strings.Repeat("?,", len(seen)), ",")
		args := []any{now, workerID}
		for taskID := range seen {
			args = append(args, taskID)
		}
		if _, err := tx.ExecContext(ctx, `UPDATE worker_task_runtime
			SET missing_heartbeats=missing_heartbeats+1, updated_at=?
			WHERE worker_id=? AND task_id NOT IN (`+placeholders+`)`, args...); err != nil {
			return err
		}
	}
	rows, err := tx.QueryContext(ctx, `SELECT task_id,job_id,attempt_id FROM worker_task_runtime
		WHERE worker_id=? AND missing_heartbeats>=2`, workerID)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var taskID, jobID, attemptID string
		if err := rows.Scan(&taskID, &jobID, &attemptID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO worker_events
			(event_id,worker_id,job_id,task_id,attempt_id,event_type,severity,reason_code,created_at)
			VALUES (?,?,?,?,?,?,?,?,?)`, uuid.NewString(), workerID, jobID, taskID, attemptID,
			"TASK_RUNTIME_DISAPPEARED", "WARN", "heartbeat_missing", now); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `DELETE FROM worker_task_runtime WHERE worker_id=? AND missing_heartbeats>=2`, workerID)
	return err
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

func clampPercent(v int64) int64 {
	if v < 0 {
		return 0
	}
	if v > 100 {
		return 100
	}
	return v
}
func defaultString(v any, fallback string) string {
	if s := asString(v); s != "" {
		return s
	}
	return fallback
}
func boolInt(v any) int {
	b, _ := v.(bool)
	if b {
		return 1
	}
	return 0
}
func int64Value(v any) int64 {
	switch x := v.(type) {
	case int:
		return int64(x)
	case int32:
		return int64(x)
	case int64:
		return x
	case float64:
		return int64(x)
	case float32:
		return int64(x)
	}
	return 0
}
func int64OrDefault(v any, fallback int64) int64 {
	if n := int64Value(v); n != 0 {
		return n
	}
	return fallback
}
func floatValue(v any) float64 {
	switch x := v.(type) {
	case float64:
		return x
	case float32:
		return float64(x)
	case int:
		return float64(x)
	case int64:
		return float64(x)
	}
	return 0
}

func floatOrMetric(primary, fallback any) float64 {
	if primary != nil {
		if value := floatValue(primary); value != 0 {
			return value
		}
	}
	return floatValue(fallback)
}
