// Package audittrail defines the append-only operator audit contract.
package audittrail

import (
	"context"
	"time"
	"velox-server/internal/credentials"
)

type Event struct {
	ID           string    `json:"id"`
	OccurredAt   time.Time `json:"occurred_at"`
	ActorType    string    `json:"actor_type,omitempty"`
	ActorID      string    `json:"actor_id,omitempty"`
	Action       string    `json:"action,omitempty"`
	ResourceType string    `json:"resource_type,omitempty"`
	ResourceID   string    `json:"resource_id,omitempty"`
	RequestID    string    `json:"request_id,omitempty"`
	TraceID      string    `json:"trace_id,omitempty"`
	BeforeHash   string    `json:"before_hash,omitempty"`
	AfterHash    string    `json:"after_hash,omitempty"`
	MetadataJSON string    `json:"metadata_json,omitempty"`
}

type Repository interface {
	AppendAuditEvent(context.Context, Event) error
	ListAuditEvents(context.Context, string, int) ([]Event, error)
}

func RedactMetadata(raw string) string { return credentials.JSON(raw) }
