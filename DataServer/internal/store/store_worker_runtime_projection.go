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

// workerTaskRuntimeUpsert is the minimal canonical identity/projection payload
// written by lease acceptance. Progress fields remain at their defaults until
// the worker publishes its first detailed heartbeat; identity fields are
// available immediately because this helper runs in the AcceptTaskAtomic tx.
// Heartbeat reconciliation keeps its richer progress upsert in the same file
// because it must preserve the latest counters while accepting identity edges.
type workerTaskRuntimeUpsert struct {
	TaskID          string
	JobID           string
	AttemptID       string
	AttemptNumber   int
	WorkerID        string
	SessionID       string
	LeaseID         string
	ExecutorID      string
	ExecutorVersion int
	RuntimeStatus   string
	StartedAt       string
	LastProgressAt  string
	UpdatedAt       string
}

// upsertWorkerTaskRuntimeTx creates the live Attempt read model without
// opening a second transaction. It is deliberately reusable by the lease
// acceptance path and heartbeat reconciliation, so the admin projection has
// one canonical storage row and one identity source.
func upsertWorkerTaskRuntimeTx(ctx context.Context, tx *sql.Tx, runtime workerTaskRuntimeUpsert) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO worker_task_runtime
		(task_id,job_id,attempt_id,attempt_number,worker_id,session_id,lease_id,
		executor_id,executor_version,runtime_status,started_at,last_progress_at,updated_at)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT(task_id) DO UPDATE SET
		job_id=excluded.job_id, attempt_id=excluded.attempt_id,
		attempt_number=excluded.attempt_number, worker_id=excluded.worker_id,
		session_id=excluded.session_id, lease_id=excluded.lease_id,
		executor_id=excluded.executor_id, executor_version=excluded.executor_version,
		runtime_status=excluded.runtime_status,
		started_at=CASE WHEN worker_task_runtime.attempt_id <> excluded.attempt_id
			THEN excluded.started_at ELSE COALESCE(worker_task_runtime.started_at, excluded.started_at) END,
		progress_percent=CASE WHEN worker_task_runtime.attempt_id <> excluded.attempt_id THEN 0 ELSE worker_task_runtime.progress_percent END,
		progress_stage=CASE WHEN worker_task_runtime.attempt_id <> excluded.attempt_id THEN NULL ELSE worker_task_runtime.progress_stage END,
		current_scene=CASE WHEN worker_task_runtime.attempt_id <> excluded.attempt_id THEN 0 ELSE worker_task_runtime.current_scene END,
		total_scenes=CASE WHEN worker_task_runtime.attempt_id <> excluded.attempt_id THEN 0 ELSE worker_task_runtime.total_scenes END,
		current_segment=CASE WHEN worker_task_runtime.attempt_id <> excluded.attempt_id THEN 0 ELSE worker_task_runtime.current_segment END,
		total_segments=CASE WHEN worker_task_runtime.attempt_id <> excluded.attempt_id THEN 0 ELSE worker_task_runtime.total_segments END,
		frames_encoded=CASE WHEN worker_task_runtime.attempt_id <> excluded.attempt_id THEN 0 ELSE worker_task_runtime.frames_encoded END,
		frames_decoded=CASE WHEN worker_task_runtime.attempt_id <> excluded.attempt_id THEN 0 ELSE worker_task_runtime.frames_decoded END,
		frames_composited=CASE WHEN worker_task_runtime.attempt_id <> excluded.attempt_id THEN 0 ELSE worker_task_runtime.frames_composited END,
		ffmpeg_speed_x=CASE WHEN worker_task_runtime.attempt_id <> excluded.attempt_id THEN 0 ELSE worker_task_runtime.ffmpeg_speed_x END,
		elapsed_ms=CASE WHEN worker_task_runtime.attempt_id <> excluded.attempt_id THEN 0 ELSE worker_task_runtime.elapsed_ms END,
		cumulative_metrics_json=CASE WHEN worker_task_runtime.attempt_id <> excluded.attempt_id THEN '{}' ELSE worker_task_runtime.cumulative_metrics_json END,
		last_progress_at=CASE WHEN worker_task_runtime.attempt_id <> excluded.attempt_id
			THEN NULL ELSE worker_task_runtime.last_progress_at END,
		cancel_requested_at=CASE WHEN worker_task_runtime.attempt_id <> excluded.attempt_id
			THEN NULL ELSE worker_task_runtime.cancel_requested_at END,
		updated_at=excluded.updated_at, missing_heartbeats=0`,
		runtime.TaskID, runtime.JobID, runtime.AttemptID, runtime.AttemptNumber,
		runtime.WorkerID, runtime.SessionID, runtime.LeaseID, runtime.ExecutorID,
		runtime.ExecutorVersion, runtime.RuntimeStatus, runtime.StartedAt,
		runtime.LastProgressAt, runtime.UpdatedAt)
	return err
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
			current_scene,total_scenes,current_segment,total_segments,frames_encoded,
			frames_decoded,frames_composited,ffmpeg_speed_x,elapsed_ms,cumulative_metrics_json,
			started_at,last_progress_at,updated_at)
			VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
			ON CONFLICT(task_id) DO UPDATE SET
			job_id=excluded.job_id, attempt_id=excluded.attempt_id, attempt_number=excluded.attempt_number,
			worker_id=excluded.worker_id, session_id=excluded.session_id, lease_id=excluded.lease_id,
			executor_id=excluded.executor_id, executor_version=excluded.executor_version,
			runtime_status=excluded.runtime_status, progress_percent=excluded.progress_percent,
			progress_stage=excluded.progress_stage, current_scene=excluded.current_scene,
			total_scenes=excluded.total_scenes, current_segment=excluded.current_segment,
			total_segments=excluded.total_segments, frames_encoded=excluded.frames_encoded,
			frames_decoded=excluded.frames_decoded, frames_composited=excluded.frames_composited,
			ffmpeg_speed_x=excluded.ffmpeg_speed_x, elapsed_ms=excluded.elapsed_ms,
			cumulative_metrics_json=excluded.cumulative_metrics_json,
			started_at=COALESCE(worker_task_runtime.started_at, excluded.started_at),
			last_progress_at=excluded.last_progress_at, updated_at=excluded.updated_at,
			missing_heartbeats=0`,
			taskID, asString(task["job_id"]), asString(task["attempt_id"]), int64OrDefault(task["attempt"], 1),
			workerID, sessionID, asString(task["lease_id"]), asString(task["job_type"]), 0,
			defaultString(task["status"], "RUNNING"), clampPercent(int64Value(task["progress_percent"])),
			defaultString(task["progress_phase"], defaultString(task["progress_stage"], "")),
			int64Value(task["progress_scene"]), int64Value(task["progress_total"]),
			int64Value(task["progress_segment"]), int64Value(task["progress_total_segments"]),
			int64Value(task["frames_encoded"]), int64Value(task["frames_decoded"]),
			int64Value(task["frames_composited"]), floatValue(task["ffmpeg_speed_x"]),
			int64Value(task["elapsed_ms"]), jsonString(task["progress_metrics"]),
			defaultString(task["started_at"], now), defaultString(task["last_progress_at"], now), now)
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
		if err := appendTaskRuntimeDisappearedEvent(ctx, tx, workerID, jobID, taskID, attemptID, connectionStateChangeReasonHeartbeatMissing, now); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `DELETE FROM worker_task_runtime WHERE worker_id=? AND missing_heartbeats>=2`, workerID)
	return err
}

// bulkEmitTaskRuntimeDisappearedOnPartition is the per-task fan-out
// that mirrors a worker-level PARTITIONED_SUSPECTED transition onto
// every active worker_task_runtime row owned by the suspected worker.
// Called from PersistWorkerHeartbeat step 8.5 (after
// detectAndPersistPartitionTransition has flipped connection_state to
// PARTITIONED_SUSPECTED). Single-writer tx contract: receives a
// *sql.Tx from the caller, never opens its own.
//
// Idempotency: rows already in PARTITIONED_SUSPECTED or PARTITIONED
// are skipped on both the SELECT and the subsequent status flip, so
// this helper can be called multiple times for the same worker
// without emitting duplicate events. ReconcileWorkerPartitions is the
// separate path that writes the bare PARTITIONED state and is not
// affected by this fan-out (it iterates workers, not runtime rows).
//
// Returns the number of runtime rows emitted (== the number of
// runtime_status rows flipped).
func bulkEmitTaskRuntimeDisappearedOnPartition(ctx context.Context, tx *sql.Tx, workerID, now string) (int, error) {
	if workerID == "" {
		return 0, nil
	}
	rows, err := tx.QueryContext(ctx, `SELECT task_id, job_id, attempt_id
		FROM worker_task_runtime
		WHERE worker_id = ?
		  AND runtime_status NOT IN ('PARTITIONED_SUSPECTED','PARTITIONED')`,
		workerID)
	if err != nil {
		return 0, fmt.Errorf("query runtimes for partition emission: %w", err)
	}
	defer rows.Close()

	type runtimeIdentity struct {
		tID string
		jID string
		aID string
	}
	var identities []runtimeIdentity
	for rows.Next() {
		var id runtimeIdentity
		if err := rows.Scan(&id.tID, &id.jID, &id.aID); err != nil {
			return 0, fmt.Errorf("scan runtime for partition emission: %w", err)
		}
		identities = append(identities, id)
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("iterate runtimes for partition emission: %w", err)
	}

	for _, id := range identities {
		if err := appendTaskRuntimeDisappearedEvent(ctx, tx, workerID, id.jID, id.tID, id.aID, connectionStateChangeReasonPartitionTimeoutTask, now); err != nil {
			return 0, fmt.Errorf("append task disappeared (partition): %w", err)
		}
	}
	if len(identities) > 0 {
		if _, err := tx.ExecContext(ctx, `UPDATE worker_task_runtime
			SET runtime_status = ?, updated_at = ?
			WHERE worker_id = ?
			  AND runtime_status NOT IN ('PARTITIONED_SUSPECTED','PARTITIONED')`,
			connectionStatePartitionedSuspected, now, workerID); err != nil {
			return 0, fmt.Errorf("flip runtime status to partitioned_suspected: %w", err)
		}
	}
	return len(identities), nil
}
