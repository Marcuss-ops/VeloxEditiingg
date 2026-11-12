package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"velox-shared/taskcontract"
)

// ScheduleRuntimeFinalizationRetry records a bounded exponential delay in the
// durable task spec. The assembly gate remains closed during RETRY_WAIT.
func (r *SQLiteTaskRepository) ScheduleRuntimeFinalizationRetry(ctx context.Context, taskID, message string) (bool, error) {
	if r == nil || r.store == nil || r.store.db == nil || strings.TrimSpace(taskID) == "" {
		return false, errors.New("runtime finalization retry: initialized store and task ID are required")
	}
	tx, err := r.store.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("runtime finalization retry begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	res, err := tx.ExecContext(ctx, `UPDATE tasks SET revision=revision WHERE task_id=? AND status IN ('PENDING','READY')
		AND COALESCE(worker_id,'')='' AND EXISTS (SELECT 1 FROM jobs WHERE jobs.job_id=tasks.job_id AND UPPER(jobs.status) NOT IN ('SUCCEEDED','FAILED','CANCELLED','CANCELED'))`, taskID)
	if err != nil {
		return false, fmt.Errorf("runtime finalization retry lock: %w", err)
	}
	count, err := res.RowsAffected()
	if err != nil || count == 0 {
		return false, err
	}
	var jobID, executor, raw string
	var version int
	if err := tx.QueryRowContext(ctx, `SELECT t.job_id,t.executor_id,s.spec_version,s.payload_json FROM tasks t JOIN task_specs s ON s.task_id=t.task_id WHERE t.task_id=?`, taskID).Scan(&jobID, &executor, &version, &raw); err != nil {
		return false, fmt.Errorf("runtime finalization retry read: %w", err)
	}
	var payload map[string]interface{}
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		return false, fmt.Errorf("runtime finalization retry decode: %w", err)
	}
	if payload["runtime_finalize_status"] != "RUNNING" {
		return false, nil
	}
	var previous float64
	if value, exists := payload["runtime_finalize_retry_count"]; exists {
		var ok bool
		previous, ok = value.(float64)
		if !ok {
			return false, errors.New("runtime finalization retry: invalid retry count")
		}
	}
	if previous < 0 || previous != float64(int(previous)) {
		return false, errors.New("runtime finalization retry: invalid retry count")
	}
	const maxRetries = 3
	if previous >= maxRetries {
		return false, nil
	}
	countRetry := int(previous) + 1
	now := time.Now().UTC()
	message = strings.TrimSpace(message)
	if len(message) > 2048 {
		message = message[:2048]
	}
	payload["runtime_finalize_status"] = "RETRY_WAIT"
	payload["runtime_finalize_error"] = message
	payload["runtime_finalize_retry_count"] = countRetry
	payload["runtime_finalize_retry_at"] = now.Add(2 * time.Second * time.Duration(1<<int(previous))).Format(time.RFC3339Nano)
	payload["runtime_assets_pending"] = true
	encoded, err := json.Marshal(payload)
	if err != nil {
		return false, fmt.Errorf("runtime finalization retry encode: %w", err)
	}
	hash, err := (&taskcontract.TaskSpec{Version: version, JobID: jobID, ExecutorID: executor, Payload: payload}).SpecHash()
	if err != nil {
		return false, fmt.Errorf("runtime finalization retry hash: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE task_specs SET payload_json=?,spec_hash=? WHERE task_id=?`, string(encoded), hash, taskID); err != nil {
		return false, fmt.Errorf("runtime finalization retry spec: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE tasks SET updated_at=? WHERE task_id=?`, now.Format(time.RFC3339Nano), taskID); err != nil {
		return false, fmt.Errorf("runtime finalization retry touch: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("runtime finalization retry commit: %w", err)
	}
	return true, nil
}
