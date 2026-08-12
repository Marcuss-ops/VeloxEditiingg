// Package store / store_deliveries.go
//
// Types + insert/list for the delivery split introduced by
// migration 022_split_deliveries.sql:
//
//   - delivery_destinations (reusable, per-provider configuration)
//   - job_deliveries        (per-artifact × per-destination, the new home
//     of what used to be delivery_targets)
//   - delivery_attempts     (one row per attempt; keyed by string delivery_id)
//
// Migration 031_delivery_leases.sql adds durable lease + retry columns to
// job_deliveries (locked_by, lease_id, lease_expires_at, next_attempt_at,
// attempt_count, max_attempts, last_error_code, last_error_message,
// completed_at). Status set changes from PENDING/CLAIMED/SUCCEEDED/FAILED
// to PENDING/RUNNING/RETRY_WAIT/SUCCEEDED/FAILED/BLOCKED_AUTH/CANCELLED.
//
// The destination CRUD lives in store_delivery_destinations.go and the
// job-delivery CRUD in store_job_deliveries.go.
package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
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
//
// Residuo 5 (this commit): the deprecated ABI-safe back-compat alias
// for the opaque identifier has been removed entirely. The only opaque
// identifier in the typed struct is `ExternalDestinationID`.
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
	DeliveryID       string `json:"delivery_id"`
	ArtifactID       string `json:"artifact_id"`
	DestinationID    string `json:"destination_id"`
	Status           string `json:"status"`
	IdempotencyKey   string `json:"idempotency_key,omitempty"`
	RemoteID         string `json:"remote_id,omitempty"`
	RemoteURL        string `json:"remote_url,omitempty"`
	CreatedAt        string `json:"created_at"`
	UpdatedAt        string `json:"updated_at"`
	QueuedAt         string `json:"queued_at,omitempty"`
	StartedAt        string `json:"started_at,omitempty"`
	LockedBy         string `json:"locked_by,omitempty"`
	LeaseID          string `json:"lease_id,omitempty"`
	LeaseExpiresAt   string `json:"lease_expires_at,omitempty"`
	NextAttemptAt    string `json:"next_attempt_at,omitempty"`
	AttemptCount     int    `json:"attempt_count"`
	MaxAttempts      int    `json:"max_attempts"`
	LastError        string `json:"last_error_code,omitempty"`
	LastErrorMessage string `json:"last_error_message,omitempty"`
	CompletedAt      string `json:"completed_at,omitempty"`
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
	// job_delivery_plans.retry_budget at INSERT time — see
	// internal/completion.coordinator.insertJobDeliveriesIdempotent).
	// The DeliveryRunner reads this at claim time and overrides
	// its runner-wide MaxAttempts on a per-delivery basis. 0 =
	// explicit no-retry budget.
	MaxAttempts   int
	Provider      string
	ConfigJSON    string
	ArtifactID    string
	DestinationID string
	QueuedAt      time.Time
}

// GetDeliveryPlanMetadata returns the immutable per-destination metadata
// snapshot associated with an artifact's job delivery plan. Missing metadata
// is represented as an empty JSON object so providers can safely apply their
// defaults.
func (s *SQLiteStore) GetDeliveryPlanMetadata(ctx context.Context, artifactID, destinationID string) (string, error) {
	s.observeDBOperation(false)
	if strings.TrimSpace(artifactID) == "" || strings.TrimSpace(destinationID) == "" {
		return "", fmt.Errorf("store: GetDeliveryPlanMetadata: artifact_id and destination_id are required")
	}
	var metadata string
	err := s.db.QueryRowContext(ctx, `
		SELECT COALESCE(jdp.metadata_json, '{}')
		FROM job_delivery_plans jdp
		JOIN artifacts a ON a.job_id = jdp.job_id
		WHERE a.id = ? AND jdp.destination_id = ? AND jdp.enabled = 1`, artifactID, destinationID).Scan(&metadata)
	if errors.Is(err, sql.ErrNoRows) {
		return "{}", nil
	}
	if err != nil {
		return "", fmt.Errorf("store: GetDeliveryPlanMetadata: %w", err)
	}
	if strings.TrimSpace(metadata) == "" {
		return "{}", nil
	}
	return metadata, nil
}

// ErrDeliveryNoRow is returned when the lookup misses.
var ErrDeliveryNoRow = errors.New("store: delivery row not found")
