package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"velox-server/internal/audittrail"
)

func (s *SQLiteStore) AppendAuditEvent(ctx context.Context, event audittrail.Event) error {
	return s.insertAuditEvent(ctx, event, false)
}

// AppendAuditEventIdempotent appends an audit event unless the deterministic
// event ID already exists. It never updates or deletes an existing event, so
// retries of an operator action remain append-only and produce one record.
func (s *SQLiteStore) AppendAuditEventIdempotent(ctx context.Context, event audittrail.Event) error {
	return s.insertAuditEvent(ctx, event, true)
}

func (s *SQLiteStore) insertAuditEvent(ctx context.Context, event audittrail.Event, ignoreExisting bool) error {
	if event.ID == "" {
		event.ID = uuid.NewString()
	}
	if event.OccurredAt.IsZero() {
		event.OccurredAt = time.Now().UTC()
	}
	if event.MetadataJSON == "" {
		event.MetadataJSON = "{}"
	}
	verb := "INSERT"
	if ignoreExisting {
		verb = "INSERT OR IGNORE"
	}
	_, err := s.db.ExecContext(ctx, verb+` INTO audit_events
		(id, occurred_at, actor_type, actor_id, action, resource_type, resource_id, request_id, trace_id, before_hash, after_hash, metadata_json)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, event.ID, event.OccurredAt.UTC().Format(time.RFC3339Nano), event.ActorType, event.ActorID, event.Action, event.ResourceType, event.ResourceID, event.RequestID, event.TraceID, event.BeforeHash, event.AfterHash, audittrail.RedactMetadata(event.MetadataJSON))
	return err
}

func (s *SQLiteStore) ListAuditEvents(ctx context.Context, resourceID string, limit int) ([]audittrail.Event, error) {
	if limit <= 0 || limit > 1000 {
		limit = 1000
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id, occurred_at, actor_type, actor_id, action, resource_type, resource_id, request_id, trace_id, before_hash, after_hash, metadata_json FROM audit_events WHERE resource_id = ? ORDER BY occurred_at ASC, id ASC LIMIT ?`, resourceID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []audittrail.Event
	for rows.Next() {
		var event audittrail.Event
		var occurred string
		if err := rows.Scan(&event.ID, &occurred, &event.ActorType, &event.ActorID, &event.Action, &event.ResourceType, &event.ResourceID, &event.RequestID, &event.TraceID, &event.BeforeHash, &event.AfterHash, &event.MetadataJSON); err != nil {
			return nil, err
		}
		event.OccurredAt, _ = time.Parse(time.RFC3339Nano, occurred)
		out = append(out, event)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(out) > 0 {
		return out, nil
	}

	// Normal task-result ingestion records the detailed, append-only
	// execution timeline in task_execution_events. Older lifecycle code did
	// not mirror those rows into audit_events, which made the audit endpoint
	// incorrectly return null/empty for successful jobs. Project the detailed
	// timeline as a read-only audit view when no explicit operator audit rows
	// exist; this preserves the audit_events source for operator actions while
	// making execution history observable and replayable.
	timelineRows, err := s.db.QueryContext(ctx, `
		SELECT event_id, COALESCE(NULLIF(completed_at, ''), NULLIF(started_at, ''), created_at),
		       origin, worker_id, event_name, event_type, action, job_id, metadata_json
		FROM task_execution_events
		WHERE job_id = ? OR task_id = ? OR attempt_id = ?
		ORDER BY COALESCE(NULLIF(completed_at, ''), NULLIF(started_at, ''), created_at) ASC,
		         event_index ASC, id ASC
		LIMIT ?`, resourceID, resourceID, resourceID, limit)
	if err != nil {
		return nil, err
	}
	defer timelineRows.Close()
	for timelineRows.Next() {
		var eventID, occurred, actorType, actorID, eventName, eventType, action, jobID, metadata string
		if err := timelineRows.Scan(&eventID, &occurred, &actorType, &actorID, &eventName, &eventType, &action, &jobID, &metadata); err != nil {
			return nil, err
		}
		if eventID == "" {
			eventID = uuid.NewString()
		}
		if occurred == "" {
			occurred = time.Now().UTC().Format(time.RFC3339Nano)
		}
		eventTime, parseErr := time.Parse(time.RFC3339Nano, occurred)
		if parseErr != nil {
			eventTime, _ = time.Parse(time.RFC3339, occurred)
		}
		if metadata == "" {
			metadata = "{}"
		}
		if !json.Valid([]byte(metadata)) {
			metadata = "{}"
		}
		if action == "" {
			action = eventName
		}
		if action == "" {
			action = eventType
		}
		out = append(out, audittrail.Event{
			ID: eventID, OccurredAt: eventTime, ActorType: actorType, ActorID: actorID,
			Action: action, ResourceType: "job", ResourceID: jobID, MetadataJSON: metadata,
		})
	}
	if err := timelineRows.Err(); err != nil && err != sql.ErrNoRows {
		return nil, err
	}
	return out, nil
}

var _ audittrail.Repository = (*SQLiteStore)(nil)
