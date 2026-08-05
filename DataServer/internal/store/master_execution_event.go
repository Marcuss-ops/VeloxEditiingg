package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	sharedtelemetry "velox-shared/telemetry"
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
	TelemetrySchemaVersion               int32
}

// persistMasterExecutionEventTx appends a master-owned event in the same
// transaction as the state transition it describes. Telemetry is best-effort:
// an invalid event is quarantined and never aborts the state transition.
func persistMasterExecutionEventTx(ctx context.Context, tx *sql.Tx, event masterExecutionEvent) error {
	if event.AttemptID == "" || event.TaskID == "" || event.Component == "" || event.Action == "" {
		return fmt.Errorf("master execution event requires attempt, task, component and action")
	}
	if event.TelemetrySchemaVersion == 0 {
		event.TelemetrySchemaVersion = sharedtelemetry.SchemaVersion
	}
	if err := sharedtelemetry.Catalog.Validate(sharedtelemetry.TelemetryEventSpec{
		Origin:        sharedtelemetry.OriginMaster,
		Scope:         event.Scope,
		Component:     event.Component,
		Action:        event.Action,
		SchemaVersion: event.TelemetrySchemaVersion,
	}); err != nil {
		quarantineMasterTelemetryEvent(ctx, tx, event, err)
		return nil
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
		 WHERE attempt_id = ? AND origin = ?`, event.AttemptID, sharedtelemetry.OriginMaster).Scan(&eventIndex); err != nil {
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
			started_at, completed_at, duration_ms, metadata_json, created_at,
			telemetry_schema_version
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'completed', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(event_id) DO NOTHING`,
		eventID, event.AttemptID, event.JobID, event.TaskID, event.WorkerID,
		event.WorkerSessionID, event.SnapshotID, event.LeaseID,
		event.ExecutorID, event.ExecutorVersion, eventIndex, sharedtelemetry.OriginMaster, event.Scope,
		event.Action, event.Component, event.Action, event.Phase, event.Status,
		started, completed, duration, event.MetadataJSON, time.Now().UTC().Format(time.RFC3339Nano),
		event.TelemetrySchemaVersion,
	)
	if err != nil {
		return fmt.Errorf("master execution event %s.%s: %w", event.Component, event.Action, err)
	}
	return nil
}

// quarantineMasterTelemetryEvent keeps invalid master-owned events out of the
// execution timeline without allowing telemetry to abort the state transition.
// The quarantine write is deliberately best-effort because older stores may
// not have migration 130 yet.
func quarantineMasterTelemetryEvent(ctx context.Context, tx *sql.Tx, event masterExecutionEvent, reason error) {
	eventID := event.EventID
	if eventID == "" {
		eventID = fmt.Sprintf("master-invalid-%s-%s-%s", event.AttemptID, event.Component, event.Action)
	}
	eventJSON, marshalErr := json.Marshal(map[string]any{
		"origin":         sharedtelemetry.OriginMaster,
		"scope":          event.Scope,
		"component":      event.Component,
		"action":         event.Action,
		"schema_version": event.TelemetrySchemaVersion,
	})
	if marshalErr != nil {
		log.Printf("[TELEMETRY_QUARANTINE] master event attempt=%s component=%s action=%s reason=%v json=%v", event.AttemptID, event.Component, event.Action, reason, marshalErr)
		return
	}
	telemetryQuarantinedEvents.Add(1)
	_, err := tx.ExecContext(ctx, `
		INSERT INTO telemetry_event_quarantine (
			attempt_id, event_id, origin, scope, component, action,
			schema_version, reason, event_json, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(attempt_id, event_id) DO NOTHING`,
		event.AttemptID, eventID, sharedtelemetry.OriginMaster, event.Scope,
		event.Component, event.Action, event.TelemetrySchemaVersion, reason.Error(),
		string(eventJSON), time.Now().UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		log.Printf("[TELEMETRY_QUARANTINE] master event attempt=%s component=%s action=%s reason=%v write=%v", event.AttemptID, event.Component, event.Action, reason, err)
	}
}
