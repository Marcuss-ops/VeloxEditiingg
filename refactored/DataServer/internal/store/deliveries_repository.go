package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// ErrNotImplemented is returned by stub repository implementations.
var ErrNotImplemented = errors.New("postgres: not implemented in scaffolding (spec §5 — implement via driver/pgx before use)")

// DeliveryResult is the outcome of a single delivery attempt.
type DeliveryResult struct {
	Success     bool   `json:"success"`
	URL         string `json:"url,omitempty"`
	VideoID     string `json:"video_id,omitempty"`
	WebViewLink string `json:"web_view_link,omitempty"`
	FolderLink  string `json:"folder_link,omitempty"`
}

// DeliveryFailure describes why a delivery attempt failed — stored to drive retries.
type DeliveryFailure struct {
	ErrorCode string `json:"error_code"`
	ErrorMsg  string `json:"error_message"`
}

// DeliveryRepository narrows persistence for delivery targets/attempts.
//
// Per spec §5, atomicity (claim / complete-with-attempt-record / fail) belongs
// inside single repo methods. Callers never see Begin/Commit.
type DeliveryRepository interface {
	// CreateDeliveriesForArtifact resolves DeliveryTargets for an artifact's job.
	// Idempotent: if delivery_targets already exist for the job, returns nil.
	CreateDeliveriesForArtifact(ctx context.Context, artifactID string) error
	// ClaimNextDelivery atomically marks the next pending target as 'uploading'
	// and inserts an uploading delivery_attempts row for audit. Returns nil on empty queue.
	ClaimNextDelivery(ctx context.Context, workerID string) (*DeliveryTarget, error)
	// CompleteDelivery records success and closes the latest in-progress attempt.
	CompleteDelivery(ctx context.Context, targetID int, result DeliveryResult) error
	// FailDelivery records failure and closes the latest in-progress attempt.
	FailDelivery(ctx context.Context, targetID int, failure DeliveryFailure) error
}

// SQLiteDeliveryRepository implements DeliveryRepository against *SQLiteStore.
type SQLiteDeliveryRepository struct {
	store *SQLiteStore
}

// NewSQLiteDeliveryRepository wraps a SQLiteStore as a DeliveryRepository.
func NewSQLiteDeliveryRepository(store *SQLiteStore) *SQLiteDeliveryRepository {
	return &SQLiteDeliveryRepository{store: store}
}

// CreateDeliveriesForArtifact is idempotent — it ensures delivery_targets exist
// for the artifact's job but never duplicates existing rows. The narrow repo
// intentionally does NOT carry config JSON: full config resolution is the
// DeliveryService's responsibility, layered on top.
func (r *SQLiteDeliveryRepository) CreateDeliveriesForArtifact(ctx context.Context, artifactID string) error {
	if artifactID == "" {
		return fmt.Errorf("artifactID is required")
	}
	var jobID string
	err := r.store.db.QueryRowContext(ctx,
		`SELECT job_id FROM artifacts WHERE id=?`, artifactID).Scan(&jobID)
	if err == sql.ErrNoRows {
		return fmt.Errorf("artifact %s not found", artifactID)
	}
	if err != nil {
		return err
	}

	var existing int
	if err := r.store.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM delivery_targets WHERE job_id=?`, jobID).Scan(&existing); err != nil {
		return err
	}
	if existing > 0 {
		return nil
	}

	now := time.Now().UTC().Format(time.RFC3339)
	_, err = r.store.db.ExecContext(ctx,
		`INSERT INTO delivery_targets (job_id, target_type, status, config, result, created_at, updated_at, attempt_count)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		jobID, "video", "pending", "{}", "{}", now, now, 0,
	)
	return err
}

// ClaimNextDelivery atomically picks the oldest pending target and marks it as
// 'uploading' with an audit row in delivery_attempts. Returns (nil, nil) if
// nothing is claimable.
func (r *SQLiteDeliveryRepository) ClaimNextDelivery(ctx context.Context, workerID string) (*DeliveryTarget, error) {
	tx, err := r.store.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	row := tx.QueryRowContext(ctx,
		`SELECT id, job_id, target_type, status, config, result,
		        created_at, updated_at, attempt_count, COALESCE(last_attempt_at,'')
		 FROM delivery_targets
		 WHERE status IN ('pending','scheduled','needs_resolution')
		 ORDER BY updated_at ASC, id ASC
		 LIMIT 1`)
	var t DeliveryTarget
	err = row.Scan(&t.ID, &t.JobID, &t.TargetType, &t.Status,
		&t.Config, &t.Result, &t.CreatedAt, &t.UpdatedAt,
		&t.AttemptCount, &t.LastAttemptAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	now := time.Now().UTC().Format(time.RFC3339)
	if _, err := tx.ExecContext(ctx,
		`UPDATE delivery_targets SET status='uploading', updated_at=?, last_attempt_at=? WHERE id=?`,
		now, now, t.ID); err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO delivery_attempts (delivery_target_id, attempt_number, status, result, started_at, worker_id)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		t.ID, t.AttemptCount+1, "uploading", "{}", now, workerID); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	t.Status = "uploading"
	t.LastAttemptAt = now
	t.AttemptCount = t.AttemptCount + 1
	return &t, nil
}

// CompleteDelivery atomically updates the target's status + result and closes
// the matching last in-progress attempt with the same result.
func (r *SQLiteDeliveryRepository) CompleteDelivery(ctx context.Context, targetID int, result DeliveryResult) error {
	resultJSON, _ := json.Marshal(result)
	now := time.Now().UTC().Format(time.RFC3339)
	tx, err := r.store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx,
		`UPDATE delivery_targets SET status='completed', result=?, updated_at=?, attempt_count=attempt_count+1, last_attempt_at=?
		 WHERE id=?`,
		string(resultJSON), now, now, targetID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE delivery_attempts SET status='completed', result=?, completed_at=?
		 WHERE id=(SELECT id FROM delivery_attempts WHERE delivery_target_id=? AND status='uploading' ORDER BY id DESC LIMIT 1)`,
		string(resultJSON), now, targetID); err != nil {
		return err
	}
	return tx.Commit()
}

// FailDelivery atomically updates the target's status + result with the failure,
// and closes the latest in-progress attempt with error_message populated.
func (r *SQLiteDeliveryRepository) FailDelivery(ctx context.Context, targetID int, failure DeliveryFailure) error {
	resultJSON := []byte("{}")
	if b, err := json.Marshal(map[string]interface{}{
		"success":       false,
		"error_code":    failure.ErrorCode,
		"error_message": failure.ErrorMsg,
	}); err == nil {
		resultJSON = b
	}
	now := time.Now().UTC().Format(time.RFC3339)
	tx, err := r.store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx,
		`UPDATE delivery_targets SET status='failed', result=?, updated_at=?, attempt_count=attempt_count+1, last_attempt_at=?
		 WHERE id=?`,
		string(resultJSON), now, now, targetID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE delivery_attempts SET status='failed', result=?, completed_at=?, error_message=?
		 WHERE id=(SELECT id FROM delivery_attempts WHERE delivery_target_id=? AND status='uploading' ORDER BY id DESC LIMIT 1)`,
		string(resultJSON), now, failure.ErrorMsg, targetID); err != nil {
		return err
	}
	return tx.Commit()
}

// Compile-time check that SQLiteDeliveryRepository satisfies DeliveryRepository.
var _ DeliveryRepository = (*SQLiteDeliveryRepository)(nil)
