package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

type masterExecutionEvent struct {
	EventID                              string
	AttemptID, JobID, TaskID, WorkerID   string
	WorkerSessionID, SnapshotID, LeaseID string
	ExecutorID                           string
	ExecutorVersion                      int
	Scope, Component, Action, Phase      string
	StartedAt, CompletedAt               time.Time
	Status, MetadataJSON                 string
}

// persistMasterExecutionEventTx appends a master-owned event in the same
// transaction as the state transition it describes.
func persistMasterExecutionEventTx(ctx context.Context, tx *sql.Tx, event masterExecutionEvent) error {
	if event.AttemptID == "" || event.TaskID == "" || event.Component == "" || event.Action == "" {
		return fmt.Errorf("master execution event requires attempt, task, component and action")
	}
	if event.JobID == "" || event.WorkerID == "" || event.ExecutorID == "" {
		var jobID, taskID, workerID, sessionID, snapshotID, leaseID, executorID string
		var executorVersion int
		err := tx.QueryRowContext(ctx, `
			SELECT a.job_id, a.task_id, a.worker_id, COALESCE(a.worker_session_id, ''),
			       COALESCE(a.worker_snapshot_id, ''), COALESCE(a.lease_id, ''),
			       COALESCE(t.executor_id, ''), COALESCE(t.executor_version, 0)
			  FROM task_attempts a JOIN tasks t ON t.task_id = a.task_id
			 WHERE a.id = ?`, event.AttemptID).Scan(
			&jobID, &taskID, &workerID, &sessionID, &snapshotID, &leaseID,
			&executorID, &executorVersion)
		if err != nil {
			return fmt.Errorf("master execution event identity: %w", err)
		}
		event.JobID, event.TaskID, event.WorkerID = jobID, taskID, workerID
		event.WorkerSessionID, event.SnapshotID, event.LeaseID = sessionID, snapshotID, leaseID
		event.ExecutorID, event.ExecutorVersion = executorID, executorVersion
	}
	if event.Status == "" {
		event.Status = "ok"
	}
	if event.MetadataJSON == "" {
		event.MetadataJSON = "{}"
	}
	started, completed := "", ""
	if !event.StartedAt.IsZero() {
		started = event.StartedAt.UTC().Format(time.RFC3339Nano)
	}
	if !event.CompletedAt.IsZero() {
		completed = event.CompletedAt.UTC().Format(time.RFC3339Nano)
	}
	duration := int64(0)
	if !event.StartedAt.IsZero() && !event.CompletedAt.IsZero() && event.CompletedAt.After(event.StartedAt) {
		duration = event.CompletedAt.Sub(event.StartedAt).Milliseconds()
	}
	var eventIndex int64
	if err := tx.QueryRowContext(ctx, `
		SELECT COALESCE(MAX(event_index), -1) + 1
		  FROM task_execution_events
		 WHERE attempt_id = ? AND origin = 'master'`, event.AttemptID).Scan(&eventIndex); err != nil {
		if strings.Contains(err.Error(), "no such table: task_execution_events") {
			return nil
		}
		return fmt.Errorf("master execution event index: %w", err)
	}
	eventID := event.EventID
	if eventID == "" {
		eventID = fmt.Sprintf("master-%s-%d", event.AttemptID, eventIndex)
	}
	var existing int
	if err := tx.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM task_execution_events WHERE event_id = ?`, eventID,
	).Scan(&existing); err != nil {
		return fmt.Errorf("master execution event lookup %s: %w", eventID, err)
	}
	if existing > 0 {
		return nil
	}
	_, err := tx.ExecContext(ctx, `
		INSERT INTO task_execution_events (
			event_id, attempt_id, job_id, task_id, worker_id,
			worker_session_id, worker_snapshot_id, lease_id,
			executor_id, executor_version, event_index, origin, scope,
			event_type, event_name, component, action, phase, status,
			started_at, completed_at, duration_ms, metadata_json, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'master', ?, 'completed', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(event_id) DO NOTHING`,
		eventID, event.AttemptID, event.JobID, event.TaskID, event.WorkerID,
		event.WorkerSessionID, event.SnapshotID, event.LeaseID,
		event.ExecutorID, event.ExecutorVersion, eventIndex, event.Scope,
		event.Action, event.Component, event.Action, event.Phase, event.Status,
		started, completed, duration, event.MetadataJSON, time.Now().UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		return fmt.Errorf("master execution event %s.%s: %w", event.Component, event.Action, err)
	}
	return nil
}
