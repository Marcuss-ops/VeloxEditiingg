// Package deliverystore / types.go
//
// Typed views of the delivery_destinations, job_deliveries, and lease
// projections. These are the canonical shapes; internal/store re-exports them
// as type aliases for its existing call sites.
package deliverystore

import (
	"time"

	"velox-server/internal/deliverycontract"
)

// DeliveryDestination is the typed view of a delivery_destinations row.
//
// Opaque-mode (Residuo 2 of the YouTube→Social closure): the legacy
// YouTube-specific fields `AccountID`, `ChannelID`, and `Language` have
// been removed from the typed struct because Velox no longer owns those
// concepts. `ExternalDestinationID` (canonical, opaque to Velox) is the
// only identifier routed to the social_repo; the social_repo resolves
// account + channel + language server-side from this opaque reference.
//
// The column `external_destination_id` was added by migration 091
// (Residuo 2 — DROPPED the legacy account_id / channel_id / language
// columns) and renamed from `social_destination_id` by migration 092
// (Residuo 4 — canonical-rename of the opaque-mode identifier).
type DeliveryDestination struct {
	DestinationID         string `json:"destination_id"`
	Provider              string `json:"provider"`
	ExternalDestinationID string `json:"external_destination_id,omitempty"`
	FolderID              string `json:"folder_id,omitempty"`
	Name                  string `json:"name"`
	Enabled               bool   `json:"enabled"`
	ConfigurationJSON     string `json:"configuration_json"`
	CreatedAt             string `json:"created_at"`
	UpdatedAt             string `json:"updated_at"`
}

// JobDelivery is the per-(artifact, destination) join row.
type JobDelivery struct {
	DeliveryID       string                          `json:"delivery_id"`
	ArtifactID       string                          `json:"artifact_id"`
	PublicationID    string                          `json:"publication_id,omitempty"`
	DestinationID    string                          `json:"destination_id"`
	Status           deliverycontract.DeliveryStatus `json:"status"`
	IdempotencyKey   string                          `json:"idempotency_key,omitempty"`
	RemoteID         string                          `json:"remote_id,omitempty"`
	RemoteURL        string                          `json:"remote_url,omitempty"`
	CreatedAt        string                          `json:"created_at"`
	UpdatedAt        string                          `json:"updated_at"`
	QueuedAt         string                          `json:"queued_at,omitempty"`
	StartedAt        string                          `json:"started_at,omitempty"`
	LockedBy         string                          `json:"locked_by,omitempty"`
	LeaseID          string                          `json:"lease_id,omitempty"`
	LeaseExpiresAt   string                          `json:"lease_expires_at,omitempty"`
	NextAttemptAt    string                          `json:"next_attempt_at,omitempty"`
	AttemptCount     int                             `json:"attempt_count"`
	MaxAttempts      int                             `json:"max_attempts"`
	LastError        string                          `json:"last_error_code,omitempty"`
	LastErrorMessage string                          `json:"last_error_message,omitempty"`
	CompletedAt      string                          `json:"completed_at,omitempty"`
}

// DeliveryLease is the typed return from ClaimDeliveries. Every field is
// populated by the atomic UPDATE+RETURNING and is required by the runner
// to dispatch, renew, and complete the delivery.
type DeliveryLease struct {
	DeliveryID    string
	RunnerID      string
	LeaseID       string
	LeaseExpires  time.Time
	AttemptNumber int
	// MaxAttempts is the per-delivery retry budget stamped from
	// job_deliveries.max_attempts (which itself comes from
	// job_delivery_plans.retry_budget at INSERT time). The DeliveryRunner
	// reads this at claim time and overrides its runner-wide MaxAttempts
	// on a per-delivery basis. 0 = explicit no-retry budget.
	MaxAttempts   int
	Provider      string
	ConfigJSON    string
	ArtifactID    string
	PublicationID string
	DestinationID string
	QueuedAt      time.Time
}

// DeliveryDestinationStatus is the 3-state verdict returned by
// BatchDeliveryDestinationsStatus. It exists so the handler-layer
// pre-flight can distinguish the two failure modes that §0.3.4
// item 4 of the runbook splits (Velox-side enabled=false vs.
// an upstream InstaEdit-side verdict).
type DeliveryDestinationStatus int

const (
	// DeliveryDestinationNotFound indicates the destination_id is
	// not present in delivery_destinations. The producer picked
	// (or fabricated) an unknown id.
	DeliveryDestinationNotFound DeliveryDestinationStatus = iota
	// DeliveryDestinationDisabled indicates the row exists but
	// enabled = 0.
	DeliveryDestinationDisabled
	// DeliveryDestinationEnabled indicates the row exists and
	// enabled = 1.
	DeliveryDestinationEnabled
)

// String returns the canonical wire-format name for use in JSON
// envelopes. Stable contract: the lowercase strings are the
// accepted values for `details[].status` in the §0.3.4 split
// response.
func (s DeliveryDestinationStatus) String() string {
	switch s {
	case DeliveryDestinationNotFound:
		return "not_found"
	case DeliveryDestinationDisabled:
		return "disabled"
	case DeliveryDestinationEnabled:
		return "enabled"
	default:
		return "unknown"
	}
}
