package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
)

// store_worker_events.go owns the worker_events INSERT helpers.
// Today only two event types are emitted from the heartbeat path —
// WORKER_STATE_CHANGED (state machine transitions) and
// TASK_RUNTIME_DISAPPEARED (reconciler-detected stale rows). Both
// helpers receive *sql.Tx from the caller and never open their own.

func appendWorkerStateChangedEvent(ctx context.Context, tx *sql.Tx, workerID, sessionID, oldStatus, newStatus, oldJob, newJob, now string) error {
	details, _ := json.Marshal(map[string]any{
		"old_status": oldStatus,
		"new_status": newStatus,
		"old_job":    oldJob,
		"new_job":    newJob,
	})
	if _, err := tx.ExecContext(ctx, `INSERT INTO worker_events
		(event_id,worker_id,session_id,event_type,severity,details_json,created_at)
		VALUES (?,?,?,?,?,?,?)`, uuid.NewString(), workerID, sessionID,
		"WORKER_STATE_CHANGED", "INFO", string(details), now); err != nil {
		return fmt.Errorf("append worker event: %w", err)
	}
	return nil
}

func appendTaskRuntimeDisappearedEvent(ctx context.Context, tx *sql.Tx, workerID, jobID, taskID, attemptID, now string) error {
	if _, err := tx.ExecContext(ctx, `INSERT INTO worker_events
		(event_id,worker_id,job_id,task_id,attempt_id,event_type,severity,reason_code,created_at)
		VALUES (?,?,?,?,?,?,?,?,?)`, uuid.NewString(), workerID, jobID, taskID, attemptID,
		"TASK_RUNTIME_DISAPPEARED", "WARN", "heartbeat_missing", now); err != nil {
		return err
	}
	return nil
}
