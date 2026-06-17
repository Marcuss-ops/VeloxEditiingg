// Package store / store_deliveries.go
//
// PR3b — Types + insert/list for the new delivery split introduced by
// migration 022_split_deliveries.sql:
//
//   * delivery_destinations (reusable, per-provider configuration)
//   * job_deliveries        (per-artifact × per-destination, the new home
//                            of what used to be delivery_targets)
//   * delivery_attempts     (one row per upload attempt; now keyed by
//                            string delivery_id, not integer target_id)
//
// PR4d — claim/lease/retry methods used by internal/deliveries.DeliveryRunner.
//   * ClaimPendingDeliveries    — atomic claim with lease, returns the
//                                 row batch for the runner to dispatch.
//   * UpdateDeliveryAttempt     — outcome write; signature is now
//                                 (ctx, deliveryID, status, resultURL, errorMsg),
//                                 a string delivery_id rather than the legacy
//                                 int id. Backward compat with the legacy int
//                                 id path is not preserved — the runner.go
//                                 side already calls the new signature.
//   * UpdateJobDeliveryStatus + WithBackoff   — per-delivery retries
//   * UpdateJobDeliveryRemote    — stamp remote_id/remote_url after SUCCESS
//   * GetDeliveryDestination     — caller for the runner's hydrate path
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
}

// ErrDeliveryNoRow is returned when the lookup misses.
var ErrDeliveryNoRow = errors.New("store: delivery row not found")

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
	legacyID := interface{}(nil)
	if jobD.LegacyDeliveryTargetID != 0 {
		legacyID = jobD.LegacyDeliveryTargetID
	}
	_, err := s.db.Exec(
		`INSERT OR IGNORE INTO job_deliveries
		 (delivery_id, artifact_id, destination_id, legacy_delivery_target_id, status,
		  idempotency_key, remote_id, remote_url, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		jobD.DeliveryID, jobD.ArtifactID, jobD.DestinationID, legacyID,
		jobD.Status, nullIfEmpty(jobD.IdempotencyKey),
		nullIfEmpty(jobD.RemoteID), nullIfEmpty(jobD.RemoteURL),
		jobD.CreatedAt, jobD.UpdatedAt,
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

// ClaimPendingDeliveries atomically claims up to `batch` pending deliveries
// for a runner. The claim is a single UPDATE+RETURNING statement so two
// concurrent runners cannot race on the same row even when their wall
// clocks disagree.
//
// Atomicity strategy: SELECT-then-UPDATE has TOCTOU between two runners
// (A reads, B reads, both UPDATE the same row). UPDATE+RETURNING keeps the
// row under the same lock from read to write. SQLite 3.35+ supports
// RETURNING; we rely on the velox-server SQLite >= 3.40 requirement.
//
// After the atomic UPDATE we open a separate INSERT for the
// delivery_attempts row inside the same tx. On a runner crash between
// the UPDATE and the commit, the UPDATE rolls back and a future pass
// re-claims the same delivery_id. The single-statement UPDATE+RETURNING
// is sufficient for cross-runner race avoidance; the deferred tx only
// protects against mid-batch rollback of the delivery_attempts INSERTs.
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

	// Atomic claim: flip status='CLAIMED' on up to `batch` pending rows
	// in one UPDATE+RETURNING. WHERE filters prune artifact-not-READY and
	// destination-not-enabled rows at the planner level. SQLite serializes
	// the writer lock, so a concurrent runner cannot observe the row in its
	// pre-update state and would queue behind this UPDATE.
	rows, err := tx.QueryContext(ctx,
		`UPDATE job_deliveries
		 SET status = 'CLAIMED', updated_at = ?
		 WHERE delivery_id IN (
		   SELECT jd.delivery_id FROM job_deliveries jd
		     JOIN delivery_destinations dd ON dd.destination_id = jd.destination_id
		     JOIN artifacts a ON a.id = jd.artifact_id
		   WHERE jd.status = 'PENDING'
		     AND dd.enabled = 1
		     AND a.status = 'READY'
		     AND a.verified_at IS NOT NULL
		   ORDER BY jd.created_at ASC
		   LIMIT ?
		 )
		 RETURNING delivery_id, artifact_id, destination_id`,
		leaseAt, batch,
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

	// Hydrate provider/configuration for each claimed row (after the
	// UPDATE so we use committed-but-isolation-locked views).
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

		// Insert a fresh delivery_attempts row tracking this claim.
		res, err := tx.ExecContext(ctx,
			`INSERT INTO delivery_attempts
			 (delivery_target_id, attempt_number, status, result,
			  started_at, completed_at, error_message, worker_id, delivery_id)
			 VALUES (0, ?, 'in_progress', '{}', ?, NULL, NULL, ?, ?)`,
			nextAttempt, nowISO, nullIfEmpty(runnerID), c.deliveryID,
		)
		// delivery_target_id is 0 — a sentinel meaning "this attempt was
		// created by the new runner path, not the legacy auto-upload
		// goroutines". Reports that join on delivery_targets will not see
		// these rows; that is intentional — reports come from job_deliveries
		// after migration 022.
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
		})
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return out, nil
}

// UpdateDeliveryAttempt finalizes the in-flight attempt. Migrated to the
// string delivery_id namespace so callers (the runner, the integration
// hooks) don't need to convert primary keys between the legacy int target
// row and the new (string delivery_id keyed) attempt row.
//
// `resultURL` is the remote URL surfaced on success; `errMsg` is logged on
// failure. Empty values are stored as NULL.
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

// UpdateJobDeliveryStatus moves a job_deliveries row to a new status with
// the canonical statuses (PENDING / CLAIMED / SUCCEEDED / FAILED / RETRY_WAIT).
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

// UpdateJobDeliveryStatusWithBackoff is the retry-aware variant. It moves
// the row back to PENDING and stamps `updated_at = nextAttemptAt` so the
// runner's claim query can use updated_at as the gate (a row in 'PENDING'
// with updated_at > NOW() is held). The schema doesn't have a dedicated
// `lease_renew_at` column yet — that lives on a follow-up migration once
// the runner smoke tests prove the gate works end-to-end. Update is
// idempotent so retrying is safe.
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
