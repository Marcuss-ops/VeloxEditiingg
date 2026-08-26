// Package audittrail owns the append-only operator/security audit-trail
// contract for the Velox server.
//
// Event is the durable record of an actor's action or an auditable lifecycle
// transition on a resource. Repository is the persistence boundary consumed
// by services; its SQLite implementation lives in internal/store. Metadata is
// redacted through RedactMetadata before persistence.
//
// This package does not inspect filesystem layout, validate migrations, or
// decide whether the data layer has one source of truth. Those checks belong
// to internal/audit. Structured audit events must not be replaced with
// free-form log lines, and a second audit-event model must not be introduced.
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
