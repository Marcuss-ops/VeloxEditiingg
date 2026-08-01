package store

import (
	"context"
	"github.com/google/uuid"
	"time"
	"velox-server/internal/audittrail"
)

func (s *SQLiteStore) AppendAuditEvent(ctx context.Context, event audittrail.Event) error {
	if event.ID == "" {
		event.ID = uuid.NewString()
	}
	if event.OccurredAt.IsZero() {
		event.OccurredAt = time.Now().UTC()
	}
	if event.MetadataJSON == "" {
		event.MetadataJSON = "{}"
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO audit_events
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
	return out, rows.Err()
}

var _ audittrail.Repository = (*SQLiteStore)(nil)
