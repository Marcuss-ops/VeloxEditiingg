// Package store / store_deliveries.go
//
// Types + insert/list/claim for the delivery split introduced by
// migration 022_split_deliveries.sql:
//
//   * delivery_destinations (reusable, per-provider configuration)
//   * job_deliveries        (per-artifact × per-destination, the new home
//                            of what used to be delivery_targets)
//   * delivery_attempts     (one row per attempt; keyed by string delivery_id)
//
// Migration 031_delivery_leases.sql adds durable lease + retry columns to
// job_deliveries (locked_by, lease_id, lease_expires_at, next_attempt_at,
// attempt_count, max_attempts, last_error_code, last_error_message,
// completed_at). Status set changes from PENDING/CLAIMED/SUCCEEDED/FAILED
// to PENDING/RUNNING/RETRY_WAIT/SUCCEEDED/FAILED/BLOCKED_AUTH/CANCELLED.
//
// The typed methods (ClaimDeliveries, RenewDeliveryLease, MarkDeliverySucceeded,
// MarkDeliveryRetry, MarkDeliveryFailed, MarkDeliveryBlockedAuth) replace the
// legacy untyped map-based claim path.
package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// DeliveryDestination is the typed view of a delivery_destinations row.
type DeliveryDestination struct {
	DestinationID     string `json:"destination_id"`
	Provider          string `json:"provider"`
	AccountID         string `json:"account_id,omitempty"`
	FolderID          string `json:"folder_id,omitempty"`
	ChannelID         string `json:"channel_id,omitempty"`
	Language          string `json:"language,omitempty"`
	Name              string `json:"name"`
	Enabled           bool   `json:"enabled"`
	ConfigurationJSON string `json:"configuration_json"`
	CreatedAt         string `json:"created_at"`
	UpdatedAt         string `json:"updated_at"`
}

// JobDelivery is the per-(artifact, destination) join row.
type JobDelivery struct {
	DeliveryID              string `json:"delivery_id"`
	ArtifactID              string `json:"artifact_id"`
	DestinationID           string `json:"destination_id"`
	LegacyDeliveryTargetID  int64  `json:"legacy_delivery_target_id,omitempty"`
	Status                  string `json:"status"`
	IdempotencyKey          string `json:"idempotency_key,omitempty"`
	RemoteID                string `json:"remote_id,omitempty"`
	RemoteURL               string `json:"remote_url,omitempty"`
	CreatedAt               string `json:"created_at"`
	UpdatedAt               string `json:"updated_at"`
	LockedBy                string `json:"locked_by,omitempty"`
	LeaseID                 string `json:"lease_id,omitempty"`
	LeaseExpiresAt          string `json:"lease_expires_at,omitempty"`
	NextAttemptAt           string `json:"next_attempt_at,omitempty"`
	AttemptCount            int    `json:"attempt_count"`
	MaxAttempts             int    `json:"max_attempts"`
	LastError               string `json:"last_error_code,omitempty"`
	LastErrorMessage        string `json:"last_error_message,omitempty"`
	CompletedAt             string `json:"completed_at,omitempty"`
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
	Provider      string
	ConfigJSON    string
	ArtifactID    string
	DestinationID string
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
		 (destination_id, provider, account_id, folder_id, channel_id, language, name,
		  enabled, configuration_json, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		dest.DestinationID, dest.Provider,
		nullIfEmpty(dest.AccountID), nullIfEmpty(dest.FolderID),
		nullIfEmpty(dest.ChannelID), nullIfEmpty(dest.Language),
		dest.Name, enabled, dest.ConfigurationJSON,
		dest.CreatedAt, dest.UpdatedAt,
	)
	return err
}

// ListDeliveryDestinations returns all enabled destinations, optionally
// filtered by provider. Returns at most `limit` rows (zero means default).
func (s *SQLiteStore) ListDeliveryDestinations(provider string, limit int) ([]DeliveryDestination, error) {
	if limit <= 0 {
		limit = 200
	}
	query := `SELECT destination_id, provider, COALESCE(account_id,''), COALESCE(folder_id,''),
	                 COALESCE(channel_id,''), COALESCE(language,''), COALESCE(name,''),
	                 enabled, COALESCE(configuration_json,''),
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
		if err := rows.Scan(&d.DestinationID, &d.Provider, &d.AccountID, &d.FolderID,
			&d.ChannelID, &d.Language, &d.Name,
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
func (s *SQLiteStore) GetDeliveryDestination(ctx context.Context, destID string) (*DeliveryDestination, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT destination_id, provider, COALESCE(account_id,''), COALESCE(folder_id,''),
		        COALESCE(channel_id,''), COALESCE(language,''), COALESCE(name,''),
		        enabled, COALESCE(configuration_json,''),
		        created_at, updated_at
		 FROM delivery_destinations WHERE destination_id = ?`, destID)
	var d DeliveryDestination
	var enabledInt int
	err := row.Scan(&d.DestinationID, &d.Provider, &d.AccountID, &d.FolderID,
		&d.ChannelID, &d.Language, &d.Name,
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
	legacyID := interface{}(nil)
	if jobD.LegacyDeliveryTargetID != 0 {
		legacyID = jobD.LegacyDeliveryTargetID
	}
	_, err := s.db.Exec(
		`INSERT OR IGNORE INTO job_deliveries
		 (delivery_id, artifact_id, destination_id, legacy_delivery_target_id, status,
		  idempotency_key, remote_id, remote_url, created_at, updated_at,
		  locked_by, lease_id, lease_expires_at, next_attempt_at,
		  attempt_count, max_attempts, last_error_code, last_error_message, completed_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		jobD.DeliveryID, jobD.ArtifactID, jobD.DestinationID, legacyID,
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
		        COALESCE(jd.legacy_delivery_target_id, 0), jd.status,
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
		var legacyID sql.NullInt64
		if err := rows.Scan(&jd.DeliveryID, &jd.ArtifactID, &jd.DestinationID,
			&legacyID, &jd.Status, &jd.IdempotencyKey, &jd.RemoteID,
			&jd.RemoteURL, &jd.CreatedAt, &jd.UpdatedAt); err != nil {
			continue
		}
		if legacyID.Valid {
			jd.LegacyDeliveryTargetID = legacyID.Int64
		}
		out = append(out, jd)
	}
	return out, rows.Err()
}

// GetJobDelivery retrieves a single job_delivery by ID.
func (s *SQLiteStore) GetJobDelivery(ctx context.Context, deliveryID string) (*JobDelivery, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT delivery_id, artifact_id, destination_id,
		        COALESCE(legacy_delivery_target_id, 0), status,
		        COALESCE(idempotency_key,''), COALESCE(remote_id,''),
		        COALESCE(remote_url,''),
		        created_at, updated_at
		 FROM job_deliveries WHERE delivery_id = ?`, deliveryID)
	var jd JobDelivery
	var legacyID interface{}
	var idempotencyKey, remoteID, remoteURL string
	err := row.Scan(&jd.DeliveryID, &jd.ArtifactID, &jd.DestinationID,
		&legacyID, &jd.Status, &idempotencyKey, &remoteID,
		&remoteURL, &jd.CreatedAt, &jd.UpdatedAt)
	if err != nil {
		return nil, err
	}
	jd.IdempotencyKey = idempotencyKey
	jd.RemoteID = remoteID
	jd.RemoteURL = remoteURL
	if legacyID, ok := legacyID.(int64); ok {
		jd.LegacyDeliveryTargetID = legacyID
	}
	return &jd, nil
}

// ── Typed lease methods (PR4e) ───────────────────────────────────────────────

// ClaimDeliveries atomically claims up to `batch` claimable deliveries for a
// runner. It matches:
//   - PENDING / RETRY_WAIT with next_attempt_at IS NULL OR <= now
//   - RUNNING with lease_expires_at < now (zombie reclaim)
//
// Each claim sets status=RUNNING, locked_by=runnerID, lease_id=uuid,
// lease_expires_at=now+lease, attempt_count++, and inserts a
// delivery_attempts audit row — all inside a single tx.
//
// Returns typed DeliveryLease values for the runner to dispatch.
func (s *SQLiteStore) ClaimDeliveries(ctx context.Context, runnerID string, lease time.Duration, batch int) ([]DeliveryLease, error) {
	if batch <= 0 {
		batch = 1
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	now := time.Now().UTC()
	leaseExpires := now.Add(lease)
	leaseExpiresISO := leaseExpires.Format(time.RFC3339)
	nowISO := now.Format(time.RFC3339)
	leaseID := fmt.Sprintf("dl_%s_%d", runnerID, now.UnixNano())

	// Atomic claim: flip status='RUNNING' on up to `batch` claimable rows
	// in one UPDATE+RETURNING. The subquery matches:
	//   1. PENDING/RETRY_WAIT where next_attempt_at is NULL or in the past
	//   2. RUNNING where the lease has expired (zombie reclaim)
	rows, err := tx.QueryContext(ctx,
		`UPDATE job_deliveries
		 SET status = 'RUNNING',
		     locked_by = ?,
		     lease_id = ?,
		     lease_expires_at = ?,
		     next_attempt_at = NULL,
		     attempt_count = attempt_count + 1,
		     updated_at = ?
		 WHERE delivery_id IN (
		   SELECT jd.delivery_id FROM job_deliveries jd
		     JOIN delivery_destinations dd ON dd.destination_id = jd.destination_id
		     JOIN artifacts a ON a.id = jd.artifact_id
		   WHERE (
		         (jd.status IN ('PENDING', 'RETRY_WAIT')
		          AND (jd.next_attempt_at IS NULL OR jd.next_attempt_at <= ?))
		         OR
		         (jd.status = 'RUNNING'
		          AND jd.lease_expires_at IS NOT NULL
		          AND jd.lease_expires_at < ?)
		       )
		     AND dd.enabled = 1
		     AND a.status = 'READY'
		     AND a.verified_at IS NOT NULL
		   ORDER BY jd.created_at ASC
		   LIMIT ?
		 )
		 RETURNING delivery_id, artifact_id, destination_id, attempt_count`,
		runnerID, leaseID, leaseExpiresISO, nowISO,
		nowISO, nowISO, batch,
	)
	if err != nil {
		return nil, fmt.Errorf("ClaimDeliveries: UPDATE+RETURNING: %w", err)
	}

	type claimedRow struct {
		deliveryID, artifactID, destID string
		attemptCount                   int
	}
	var claimed []claimedRow
	for rows.Next() {
		var c claimedRow
		if err := rows.Scan(&c.deliveryID, &c.artifactID, &c.destID, &c.attemptCount); err != nil {
			continue
		}
		claimed = append(claimed, c)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(claimed) == 0 {
		_ = tx.Commit()
		return nil, nil
	}

	// Hydrate provider/config for each claimed row and insert audit rows.
	out := make([]DeliveryLease, 0, len(claimed))
	for _, c := range claimed {
		var provider, configJSON string
		err := tx.QueryRowContext(ctx,
			`SELECT dd.provider, COALESCE(dd.configuration_json, '')
			 FROM delivery_destinations dd WHERE dd.destination_id = ?`,
			c.destID,
		).Scan(&provider, &configJSON)
		if err != nil {
			return nil, fmt.Errorf("ClaimDeliveries: hydrate destination: %w", err)
		}

		// Insert a delivery_attempts row tracking this claim.
		_, err = tx.ExecContext(ctx,
			`INSERT INTO delivery_attempts
			 (delivery_target_id, attempt_number, status, result,
			  started_at, completed_at, error_message, worker_id, delivery_id)
			 VALUES (0, ?, 'in_progress', '{}', ?, NULL, NULL, ?, ?)`,
			c.attemptCount, nowISO, nullIfEmpty(runnerID), c.deliveryID,
		)
		if err != nil {
			return nil, fmt.Errorf("ClaimDeliveries: attempts INSERT: %w", err)
		}

		out = append(out, DeliveryLease{
			DeliveryID:    c.deliveryID,
			RunnerID:      runnerID,
			LeaseID:       leaseID,
			LeaseExpires:  leaseExpires,
			AttemptNumber: c.attemptCount,
			Provider:      provider,
			ConfigJSON:    configJSON,
			ArtifactID:    c.artifactID,
			DestinationID: c.destID,
		})
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return out, nil
}

// RenewDeliveryLease extends the lease on a RUNNING delivery. The CAS guard
// verifies (delivery_id, status=RUNNING, locked_by, lease_id) to prevent
// stale renewals. Returns ErrTransitionConflict if the guard fails.
func (s *SQLiteStore) RenewDeliveryLease(ctx context.Context, deliveryID, runnerID, leaseID string, newExpiry time.Time) error {
	if deliveryID == "" || runnerID == "" || leaseID == "" {
		return fmt.Errorf("store: RenewDeliveryLease: missing required fields")
	}
	iso := newExpiry.UTC().Format(time.RFC3339)
	now := time.Now().UTC().Format(time.RFC3339)
	result, err := s.db.ExecContext(ctx,
		`UPDATE job_deliveries
		 SET lease_expires_at = ?, updated_at = ?
		 WHERE delivery_id = ?
		   AND status = 'RUNNING'
		   AND locked_by = ?
		   AND lease_id = ?`,
		iso, now, deliveryID, runnerID, leaseID,
	)
	if err != nil {
		return err
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		return ErrTransitionConflict
	}
	return nil
}

// MarkDeliverySucceeded moves a RUNNING delivery to SUCCEEDED with CAS guard.
// Stamps completed_at and optionally remote_id/remote_url.
func (s *SQLiteStore) MarkDeliverySucceeded(ctx context.Context, deliveryID, runnerID, leaseID, remoteID, remoteURL string) error {
	if deliveryID == "" || runnerID == "" || leaseID == "" {
		return fmt.Errorf("store: MarkDeliverySucceeded: missing required fields")
	}
	now := time.Now().UTC().Format(time.RFC3339)
	result, err := s.db.ExecContext(ctx,
		`UPDATE job_deliveries
		 SET status = 'SUCCEEDED',
		     remote_id = COALESCE(NULLIF(?, ''), remote_id),
		     remote_url = COALESCE(NULLIF(?, ''), remote_url),
		     completed_at = ?,
		     updated_at = ?
		 WHERE delivery_id = ?
		   AND status = 'RUNNING'
		   AND locked_by = ?
		   AND lease_id = ?`,
		remoteID, remoteURL, now, now, deliveryID, runnerID, leaseID,
	)
	if err != nil {
		return err
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		return ErrTransitionConflict
	}

	// Close the latest delivery_attempt.
	_, _ = s.db.ExecContext(ctx,
		`UPDATE delivery_attempts
		 SET status = 'SUCCESS', completed_at = ?
		 WHERE delivery_id = ?
		   AND id = (SELECT MAX(id) FROM delivery_attempts WHERE delivery_id = ?)`,
		now, deliveryID, deliveryID,
	)
	return nil
}

// MarkDeliveryRetry moves a RUNNING delivery to RETRY_WAIT with the next
// attempt scheduled after a backoff delay. Sets last_error_code and
// last_error_message for diagnostics.
func (s *SQLiteStore) MarkDeliveryRetry(ctx context.Context, deliveryID, runnerID, leaseID, errorCode, errorMsg string, nextAttemptAt time.Time) error {
	if deliveryID == "" || runnerID == "" || leaseID == "" {
		return fmt.Errorf("store: MarkDeliveryRetry: missing required fields")
	}
	now := time.Now().UTC().Format(time.RFC3339)
	nextISO := nextAttemptAt.UTC().Format(time.RFC3339)
	result, err := s.db.ExecContext(ctx,
		`UPDATE job_deliveries
		 SET status = 'RETRY_WAIT',
		     locked_by = NULL,
		     lease_id = NULL,
		     lease_expires_at = NULL,
		     next_attempt_at = ?,
		     last_error_code = ?,
		     last_error_message = ?,
		     updated_at = ?
		 WHERE delivery_id = ?
		   AND status = 'RUNNING'
		   AND locked_by = ?
		   AND lease_id = ?`,
		nextISO, nullIfEmpty(errorCode), nullIfEmpty(errorMsg), now,
		deliveryID, runnerID, leaseID,
	)
	if err != nil {
		return err
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		return ErrTransitionConflict
	}

	// Close the latest delivery_attempt with error.
	_, _ = s.db.ExecContext(ctx,
		`UPDATE delivery_attempts
		 SET status = 'RETRY_WAIT', completed_at = ?, error_message = ?
		 WHERE delivery_id = ?
		   AND id = (SELECT MAX(id) FROM delivery_attempts WHERE delivery_id = ?)`,
		now, nullIfEmpty(errorMsg), deliveryID, deliveryID,
	)
	return nil
}

// MarkDeliveryFailed moves a RUNNING delivery to FAILED (permanent failure).
// No further retry attempts will be scheduled.
func (s *SQLiteStore) MarkDeliveryFailed(ctx context.Context, deliveryID, runnerID, leaseID, errorCode, errorMsg string) error {
	if deliveryID == "" || runnerID == "" || leaseID == "" {
		return fmt.Errorf("store: MarkDeliveryFailed: missing required fields")
	}
	now := time.Now().UTC().Format(time.RFC3339)
	result, err := s.db.ExecContext(ctx,
		`UPDATE job_deliveries
		 SET status = 'FAILED',
		     locked_by = NULL,
		     lease_id = NULL,
		     lease_expires_at = NULL,
		     last_error_code = ?,
		     last_error_message = ?,
		     completed_at = ?,
		     updated_at = ?
		 WHERE delivery_id = ?
		   AND status = 'RUNNING'
		   AND locked_by = ?
		   AND lease_id = ?`,
		nullIfEmpty(errorCode), nullIfEmpty(errorMsg), now, now,
		deliveryID, runnerID, leaseID,
	)
	if err != nil {
		return err
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		return ErrTransitionConflict
	}

	// Close the latest delivery_attempt with error.
	_, _ = s.db.ExecContext(ctx,
		`UPDATE delivery_attempts
		 SET status = 'FAILED', completed_at = ?, error_message = ?
		 WHERE delivery_id = ?
		   AND id = (SELECT MAX(id) FROM delivery_attempts WHERE delivery_id = ?)`,
		now, nullIfEmpty(errorMsg), deliveryID, deliveryID,
	)
	return nil
}

// MarkDeliveryBlockedAuth moves a RUNNING delivery to BLOCKED_AUTH when the
// provider returns an authentication/authorization error that will not be
// resolved by retrying. The delivery stays blocked until operator intervention
// re-enables the destination credentials.
func (s *SQLiteStore) MarkDeliveryBlockedAuth(ctx context.Context, deliveryID, runnerID, leaseID, errorCode, errorMsg string) error {
	if deliveryID == "" || runnerID == "" || leaseID == "" {
		return fmt.Errorf("store: MarkDeliveryBlockedAuth: missing required fields")
	}
	now := time.Now().UTC().Format(time.RFC3339)
	result, err := s.db.ExecContext(ctx,
		`UPDATE job_deliveries
		 SET status = 'BLOCKED_AUTH',
		     locked_by = NULL,
		     lease_id = NULL,
		     lease_expires_at = NULL,
		     last_error_code = ?,
		     last_error_message = ?,
		     completed_at = ?,
		     updated_at = ?
		 WHERE delivery_id = ?
		   AND status = 'RUNNING'
		   AND locked_by = ?
		   AND lease_id = ?`,
		nullIfEmpty(errorCode), nullIfEmpty(errorMsg), now, now,
		deliveryID, runnerID, leaseID,
	)
	if err != nil {
		return err
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		return ErrTransitionConflict
	}

	// Close the latest delivery_attempt with error.
	_, _ = s.db.ExecContext(ctx,
		`UPDATE delivery_attempts
		 SET status = 'BLOCKED_AUTH', completed_at = ?, error_message = ?
		 WHERE delivery_id = ?
		   AND id = (SELECT MAX(id) FROM delivery_attempts WHERE delivery_id = ?)`,
		now, nullIfEmpty(errorMsg), deliveryID, deliveryID,
	)
	return nil
}

// ── Legacy methods (kept for backward compat during migration) ───────────────

// ClaimPendingDeliveries is the legacy untyped claim path. Deprecated: use
// ClaimDeliveries instead for typed DeliveryLease returns.
func (s *SQLiteStore) ClaimPendingDeliveries(ctx context.Context, runnerID string, lease time.Duration, batch int) ([]map[string]interface{}, error) {
	if batch <= 0 {
		batch = 1
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	now := time.Now().UTC()
	leaseAt := now.Add(lease).Format(time.RFC3339)
	nowISO := now.Format(time.RFC3339)
	leaseID := fmt.Sprintf("dl_%s_%d", runnerID, now.UnixNano())

	rows, err := tx.QueryContext(ctx,
		`UPDATE job_deliveries
		 SET status = 'RUNNING',
		     locked_by = ?,
		     lease_id = ?,
		     lease_expires_at = ?,
		     next_attempt_at = NULL,
		     attempt_count = attempt_count + 1,
		     updated_at = ?
		 WHERE delivery_id IN (
		   SELECT jd.delivery_id FROM job_deliveries jd
		     JOIN delivery_destinations dd ON dd.destination_id = jd.destination_id
		     JOIN artifacts a ON a.id = jd.artifact_id
		   WHERE jd.status IN ('PENDING', 'RETRY_WAIT')
		     AND (jd.next_attempt_at IS NULL OR jd.next_attempt_at <= ?)
		     AND dd.enabled = 1
		     AND a.status = 'READY'
		     AND a.verified_at IS NOT NULL
		   ORDER BY jd.created_at ASC
		   LIMIT ?
		 )
		 RETURNING delivery_id, artifact_id, destination_id`,
		runnerID, leaseID, leaseAt, nowISO,
		nowISO, batch,
	)
	if err != nil {
		return nil, fmt.Errorf("ClaimPendingDeliveries: UPDATE+RETURNING: %w", err)
	}

	type claimedRow struct {
		deliveryID, artifactID, destID string
	}
	var claimed []claimedRow
	for rows.Next() {
		var c claimedRow
		if err := rows.Scan(&c.deliveryID, &c.artifactID, &c.destID); err != nil {
			continue
		}
		claimed = append(claimed, c)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(claimed) == 0 {
		_ = tx.Commit()
		return nil, nil
	}

	out := make([]map[string]interface{}, 0, len(claimed))
	for _, c := range claimed {
		var provider, configJSON string
		err := tx.QueryRowContext(ctx,
			`SELECT dd.provider, COALESCE(dd.configuration_json, '')
			 FROM delivery_destinations dd WHERE dd.destination_id = ?`,
			c.destID,
		).Scan(&provider, &configJSON)
		if err != nil {
			return nil, fmt.Errorf("ClaimPendingDeliveries: hydrate destination: %w", err)
		}
		var nextAttempt int64
		row := tx.QueryRowContext(ctx,
			`SELECT COALESCE(MAX(attempt_number), 0) FROM delivery_attempts WHERE delivery_id = ?`,
			c.deliveryID,
		)
		_ = row.Scan(&nextAttempt)
		nextAttempt++

		res, err := tx.ExecContext(ctx,
			`INSERT INTO delivery_attempts
			 (delivery_target_id, attempt_number, status, result,
			  started_at, completed_at, error_message, worker_id, delivery_id)
			 VALUES (0, ?, 'in_progress', '{}', ?, NULL, NULL, ?, ?)`,
			nextAttempt, nowISO, nullIfEmpty(runnerID), c.deliveryID,
		)
		if err != nil {
			return nil, fmt.Errorf("ClaimPendingDeliveries: attempts INSERT: %w", err)
		}
		attemptID, _ := res.LastInsertId()

		out = append(out, map[string]interface{}{
			"delivery_id":      c.deliveryID,
			"artifact_id":      c.artifactID,
			"destination_id":   c.destID,
			"provider":         provider,
			"configuration":    configJSON,
			"attempt_id":       attemptID,
			"attempt_number":   nextAttempt,
			"lease_expires_at": leaseAt,
			"runner_id":        runnerID,
			"lease_id":         leaseID,
		})
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return out, nil
}

// UpdateDeliveryAttempt finalizes the in-flight attempt.
func (s *SQLiteStore) UpdateDeliveryAttempt(ctx context.Context, deliveryID, status, resultURL, errMsg string) error {
	if deliveryID == "" || status == "" {
		return fmt.Errorf("store: UpdateDeliveryAttempt: missing required fields")
	}
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := s.db.ExecContext(ctx,
		`UPDATE delivery_attempts
		 SET status = ?, result = COALESCE(NULLIF(?, ''), result),
		     completed_at = ?, error_message = ?
		 WHERE delivery_id = ?
		   AND id = (SELECT MAX(id) FROM delivery_attempts WHERE delivery_id = ?)`,
		status, resultURL, now, nullIfEmpty(errMsg), deliveryID, deliveryID,
	)
	return err
}

// UpdateJobDeliveryStatus moves a job_deliveries row to a new status.
func (s *SQLiteStore) UpdateJobDeliveryStatus(ctx context.Context, deliveryID, status string) error {
	if deliveryID == "" || status == "" {
		return fmt.Errorf("store: UpdateJobDeliveryStatus: missing required fields")
	}
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := s.db.ExecContext(ctx,
		`UPDATE job_deliveries SET status = ?, updated_at = ? WHERE delivery_id = ?`,
		status, now, deliveryID,
	)
	return err
}

// UpdateJobDeliveryStatusWithBackoff is the retry-aware variant.
func (s *SQLiteStore) UpdateJobDeliveryStatusWithBackoff(ctx context.Context, deliveryID, status string, nextAttemptAt time.Time) error {
	if deliveryID == "" || status == "" {
		return fmt.Errorf("store: UpdateJobDeliveryStatusWithBackoff: missing required fields")
	}
	iso := nextAttemptAt.UTC().Format(time.RFC3339)
	_, err := s.db.ExecContext(ctx,
		`UPDATE job_deliveries
		 SET status = ?, updated_at = ?
		 WHERE delivery_id = ?`,
		status, iso, deliveryID,
	)
	return err
}

// UpdateJobDeliveryRemote stamps remote_id/remote_url on a SUCCEEDED job_delivery.
func (s *SQLiteStore) UpdateJobDeliveryRemote(ctx context.Context, deliveryID, remoteID, remoteURL string) error {
	if deliveryID == "" {
		return fmt.Errorf("store: UpdateJobDeliveryRemote: missing deliveryID")
	}
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := s.db.ExecContext(ctx,
		`UPDATE job_deliveries
		 SET remote_id = COALESCE(NULLIF(?, ''), remote_id),
		     remote_url = COALESCE(NULLIF(?, ''), remote_url),
		     status = CASE WHEN ? IN ('SUCCESS', 'SUCCEEDED') THEN 'SUCCEEDED' ELSE status END,
		     updated_at = ?
		 WHERE delivery_id = ?`,
		remoteID, remoteURL, remoteURL, now, deliveryID,
	)
	return err
}

func nullIfEmpty(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}
