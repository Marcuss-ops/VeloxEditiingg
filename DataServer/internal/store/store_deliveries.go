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
	// "use runner default" (back-compat with rows stamped before
	// Phase 5.5).
	MaxAttempts   int
	Provider      string
	ConfigJSON    string
	ArtifactID    string
	DestinationID string
}

// GetDeliveryPlanMetadata returns the immutable per-destination metadata
// snapshot associated with an artifact's job delivery plan. Missing metadata
// is represented as an empty JSON object so providers can safely apply their
// defaults.
func (s *SQLiteStore) GetDeliveryPlanMetadata(ctx context.Context, artifactID, destinationID string) (string, error) {
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

// ── Destination CRUD ─────────────────────────────────────────────────────────

// InsertDeliveryDestination persists a delivery destination (idempotent
// on destination_id via INSERT OR IGNORE so retries are safe).
func (s *SQLiteStore) InsertDeliveryDestination(dest *DeliveryDestination) error {
	if dest.DestinationID == "" || dest.Provider == "" {
		return fmt.Errorf("store: InsertDeliveryDestination: missing required fields")
	}
	now := time.Now().UTC().Format(time.RFC3339)
	if dest.CreatedAt == "" {
		dest.CreatedAt = now
	}
	if dest.UpdatedAt == "" {
		dest.UpdatedAt = now
	}
	if dest.ConfigurationJSON == "" {
		dest.ConfigurationJSON = "{}"
	}
	enabled := 0
	if dest.Enabled {
		enabled = 1
	}
	_, err := s.db.ExecContext(context.Background(),
		`INSERT OR IGNORE INTO delivery_destinations
	 (destination_id, provider, external_destination_id, folder_id, name,
	  enabled, configuration_json, created_at, updated_at)
	 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		dest.DestinationID, dest.Provider,
		nullIfEmpty(dest.ExternalDestinationID),
		nullIfEmpty(dest.FolderID),
		dest.Name, enabled, dest.ConfigurationJSON,
		dest.CreatedAt, dest.UpdatedAt,
	)
	return err
}

// ListDeliveryDestinations returns all enabled destinations, optionally
// filtered by provider. Returns at most `limit` rows (zero means default).
//
// Opaque-mode SQL (Residuo 2 of YouTube → Social closure + migration 091):
// the legacy account_id / channel_id / language columns have been dropped
// from the delivery_destinations table. ExternalDestinationID is the
// canonical opaque reference (renamed from social_destination_id by
// migration 092, Residuo 4).
func (s *SQLiteStore) ListDeliveryDestinations(provider string, limit int) ([]DeliveryDestination, error) {
	if limit <= 0 {
		limit = 200
	}
	query := `SELECT destination_id, provider, COALESCE(external_destination_id, ''),
	                 COALESCE(folder_id, ''),
	                 COALESCE(name, ''),
	                 enabled, COALESCE(configuration_json, ''),
	                 created_at, updated_at
	          FROM delivery_destinations`
	args := []interface{}{}
	if provider != "" {
		query += ` WHERE provider = ? AND enabled = 1`
		args = append(args, provider)
	} else {
		query += ` WHERE enabled = 1`
	}
	query += ` ORDER BY created_at ASC LIMIT ?`
	args = append(args, limit)

	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DeliveryDestination
	for rows.Next() {
		var d DeliveryDestination
		var enabledInt int
		if err := rows.Scan(&d.DestinationID, &d.Provider, &d.ExternalDestinationID,
			&d.FolderID,
			&d.Name,
			&enabledInt, &d.ConfigurationJSON,
			&d.CreatedAt, &d.UpdatedAt); err != nil {
			continue
		}
		d.Enabled = enabledInt != 0
		out = append(out, d)
	}
	return out, rows.Err()
}

// GetDeliveryDestination returns a single destination by id, or
// ErrDeliveryNoRow when missing (sql.ErrNoRows is normalized).
//
// Opaque-mode SQL (Residuo 2 of YouTube → Social closure + migration 091):
// the legacy account_id / channel_id / language columns have been dropped
// from the delivery_destinations table. ExternalDestinationID is the
// canonical opaque reference (renamed from social_destination_id by
// migration 092, Residuo 4).
func (s *SQLiteStore) GetDeliveryDestination(ctx context.Context, destID string) (*DeliveryDestination, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT destination_id, provider, COALESCE(external_destination_id, ''),
		        COALESCE(folder_id, ''),
		        COALESCE(name, ''),
		        enabled, COALESCE(configuration_json, ''),
		        created_at, updated_at
		 FROM delivery_destinations WHERE destination_id = ?`, destID)
	var d DeliveryDestination
	var enabledInt int
	err := row.Scan(&d.DestinationID, &d.Provider, &d.ExternalDestinationID,
		&d.FolderID,
		&d.Name,
		&enabledInt, &d.ConfigurationJSON,
		&d.CreatedAt, &d.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, ErrDeliveryNoRow
	}
	if err != nil {
		return nil, err
	}
	d.Enabled = enabledInt != 0
	return &d, nil
}

// ── Job Delivery CRUD ────────────────────────────────────────────────────────

// InsertJobDelivery persists a new per-(artifact, destination) row.
func (s *SQLiteStore) InsertJobDelivery(jobD *JobDelivery) error {
	if jobD.DeliveryID == "" || jobD.ArtifactID == "" || jobD.DestinationID == "" {
		return fmt.Errorf("store: InsertJobDelivery: missing required fields")
	}
	now := time.Now().UTC().Format(time.RFC3339)
	if jobD.CreatedAt == "" {
		jobD.CreatedAt = now
	}
	if jobD.UpdatedAt == "" {
		jobD.UpdatedAt = now
	}
	if jobD.Status == "" {
		jobD.Status = "PENDING"
	}
	if jobD.MaxAttempts == 0 {
		jobD.MaxAttempts = 5
	}
	_, err := s.db.Exec(
		`INSERT OR IGNORE INTO job_deliveries
		 (delivery_id, artifact_id, destination_id, status,
		  idempotency_key, remote_id, remote_url, created_at, updated_at,
		  locked_by, lease_id, lease_expires_at, next_attempt_at,
		  attempt_count, max_attempts, last_error_code, last_error_message, completed_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		jobD.DeliveryID, jobD.ArtifactID, jobD.DestinationID,
		jobD.Status, nullIfEmpty(jobD.IdempotencyKey),
		nullIfEmpty(jobD.RemoteID), nullIfEmpty(jobD.RemoteURL),
		jobD.CreatedAt, jobD.UpdatedAt,
		nullIfEmpty(jobD.LockedBy), nullIfEmpty(jobD.LeaseID),
		nullIfEmpty(jobD.LeaseExpiresAt), nullIfEmpty(jobD.NextAttemptAt),
		jobD.AttemptCount, jobD.MaxAttempts,
		nullIfEmpty(jobD.LastError), nullIfEmpty(jobD.LastErrorMessage),
		nullIfEmpty(jobD.CompletedAt),
	)
	return err
}

// ListJobDeliveriesByJob returns all deliveries for a job's READY artifacts.
func (s *SQLiteStore) ListJobDeliveriesByJob(jobID string) ([]JobDelivery, error) {
	rows, err := s.db.Query(
		`SELECT jd.delivery_id, jd.artifact_id, jd.destination_id,
		        jd.status,
		        COALESCE(jd.idempotency_key,''), COALESCE(jd.remote_id,''),
		        COALESCE(jd.remote_url,''),
		        jd.created_at, jd.updated_at
		 FROM job_deliveries jd
		 JOIN artifacts a ON a.id = jd.artifact_id
		 WHERE a.job_id = ?
		 ORDER BY jd.id ASC`, jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []JobDelivery
	for rows.Next() {
		var jd JobDelivery
		if err := rows.Scan(&jd.DeliveryID, &jd.ArtifactID, &jd.DestinationID,
			&jd.Status, &jd.IdempotencyKey, &jd.RemoteID,
			&jd.RemoteURL, &jd.CreatedAt, &jd.UpdatedAt); err != nil {
			continue
		}
		out = append(out, jd)
	}
	return out, rows.Err()
}

// GetJobDelivery retrieves a single job_delivery by ID.
func (s *SQLiteStore) GetJobDelivery(ctx context.Context, deliveryID string) (*JobDelivery, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT delivery_id, artifact_id, destination_id,
		        status,
		        COALESCE(idempotency_key,''), COALESCE(remote_id,''),
		        COALESCE(remote_url,''),
		        created_at, updated_at, COALESCE(completed_at, ''),
		        COALESCE(next_attempt_at, ''), COALESCE(last_error_code, ''),
		        COALESCE(last_error_message, '')
		 FROM job_deliveries WHERE delivery_id = ?`, deliveryID)
	var jd JobDelivery
	var idempotencyKey, remoteID, remoteURL string
	err := row.Scan(&jd.DeliveryID, &jd.ArtifactID, &jd.DestinationID,
		&jd.Status, &idempotencyKey, &remoteID,
		&remoteURL, &jd.CreatedAt, &jd.UpdatedAt, &jd.CompletedAt,
		&jd.NextAttemptAt, &jd.LastError, &jd.LastErrorMessage)
	if err != nil {
		return nil, err
	}
	jd.IdempotencyKey = idempotencyKey
	jd.RemoteID = remoteID
	jd.RemoteURL = remoteURL
	return &jd, nil
}
