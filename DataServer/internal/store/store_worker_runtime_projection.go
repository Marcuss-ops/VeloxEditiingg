package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// store_worker_runtime_projection.go owns the worker_task_runtime
// table: delete-after-commit, snapshot-to-active-task derivation, and
// the reconciliation loop that bumps missing_heartbeats and emits
// TASK_RUNTIME_DISAPPEARED events for stale rows.
//
// reconcileWorkerRuntime *receives* a *sql.Tx from the caller; it does
// not open transactions, never BeginTx, never commits. This is the
// single-writer contract honoured across the runtime/metrics/events
// helpers.
//
// The missing_heartbeats >= 2 counter is a discrete heartbeat-miss
// bound for runtime-task cleanup: a runtime row is deleted and the
// TASK_RUNTIME_DISAPPEARED event is emitted after missing 2 heartbeats.
// This is INDEPENDENT of the canonical heartbeat-staleness threshold
// (DefaultStaleThreshold in store_worker_heartbeat.go, 150s = 2.5x
// the producer 60s idle heartbeat). The two thresholds serve
// different purposes: missing_heartbeats is the low-level "did the
// worker still advertise this task?" trip-wire for a single runtime
// row; DefaultStaleThreshold governs the operator-visible
// worker-level connection_state transitions
// (CONNECTED -> STALE -> PARTITIONED -> DISCONNECTED). Touching one
// does not collapse the other.

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

// activeTasksFromSnapshot materialises the worker's active_jobs list into
// the slice shape reconcileWorkerRuntime consumes. Filters out items with
// no job_id so the reconciler never inserts an orphan runtime row.
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
		if err := appendTaskRuntimeDisappearedEvent(ctx, tx, workerID, jobID, taskID, attemptID, now); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `DELETE FROM worker_task_runtime WHERE worker_id=? AND missing_heartbeats>=2`, workerID)
	return err
}
