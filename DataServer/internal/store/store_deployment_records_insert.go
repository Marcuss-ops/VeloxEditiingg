package store

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// store_deployment_records_insert.go owns the deployment_records write
// paths that CREATE rows: the PENDING insert (InsertDeploymentRecord) and
// the terminal baseline insert (InsertBaselineDeploymentRecord). Terminal
// transitions of existing rows live in store_deployment_records_transition.go.

// InsertDeploymentRecord persists a new deployment record. Status
// MUST be DeployStatusPending at insert time (terminal statuses
// fail the FK + CHECK constraints downstream and confuse the
// dashboard's "in-flight deploys" query).
func (s *SQLiteStore) InsertDeploymentRecord(ctx context.Context, r DeploymentRecord) error {
	if r.Status != DeployStatusPending {
		return fmt.Errorf("InsertDeploymentRecord: initial status must be PENDING, got %q", r.Status)
	}
	if r.DeploymentID == "" {
		return errors.New("InsertDeploymentRecord: DeploymentID empty")
	}
	if r.WorkerID == "" {
		return errors.New("InsertDeploymentRecord: WorkerID empty")
	}
	// Defence-in-depth: SQL CHECK (length > 0) catches empty digests
	// at the DB layer too, but rejecting eagerly here gives the
	// caller a clearer error and avoids the surprise of an SQL
	// CHECK violation if a future caller calls BEFORE running
	// ValidateImageRef upstream.
	if r.PreviousDigest == "" {
		return errors.New("InsertDeploymentRecord: PreviousDigest empty")
	}
	if r.TargetDigest == "" {
		return errors.New("InsertDeploymentRecord: TargetDigest empty")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `
INSERT INTO deployment_records
	  (deployment_id, worker_id, previous_digest, target_digest, started_at, finished_at, status, error_code, error_message, applied_by, is_rollback)
VALUES (?, ?, ?, ?, ?, NULL, ?, '', '', ?, ?)`,
		r.DeploymentID, r.WorkerID, r.PreviousDigest, r.TargetDigest,
		r.StartedAt.UTC().Format(time.RFC3339),
		r.Status, r.AppliedBy, boolToIntSQLite(r.IsRollback),
	)
	if err != nil {
		return err
	}
	if err := upsertDeploymentStateFromRecord(ctx, tx, r, r.StartedAt, false); err != nil {
		return err
	}
	return tx.Commit()
}

// InsertBaselineDeploymentRecord writes an already-verified runtime state as
// a terminal ledger row. It is intentionally separate from
// InsertDeploymentRecord: ordinary rollouts must always begin PENDING, while
// a bootstrap baseline must never expose a fake in-flight deployment or run
// any worker mutation.
//
// An empty PreviousDigest is persisted as SQL NULL. NULL means rollback
// provenance is unavailable; it is never replaced with the current digest.
func (s *SQLiteStore) InsertBaselineDeploymentRecord(ctx context.Context, r DeploymentRecord) error {
	if r.Status != DeployStatusSucceeded {
		return fmt.Errorf("InsertBaselineDeploymentRecord: status must be SUCCEEDED, got %q", r.Status)
	}
	if r.DeploymentID == "" {
		return errors.New("InsertBaselineDeploymentRecord: DeploymentID empty")
	}
	if r.WorkerID == "" {
		return errors.New("InsertBaselineDeploymentRecord: WorkerID empty")
	}
	if r.TargetDigest == "" {
		return errors.New("InsertBaselineDeploymentRecord: TargetDigest empty")
	}
	finishedAt := r.FinishedAt
	if finishedAt == nil {
		finishedAt = &r.StartedAt
	}
	var previous interface{}
	if r.PreviousDigest != "" {
		previous = r.PreviousDigest
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `
INSERT INTO deployment_records
	  (deployment_id, worker_id, previous_digest, target_digest, started_at, finished_at, status, error_code, error_message, applied_by, is_rollback)
VALUES (?, ?, ?, ?, ?, ?, ?, '', '', ?, 0)`,
		r.DeploymentID, r.WorkerID, previous, r.TargetDigest,
		r.StartedAt.UTC().Format(time.RFC3339), finishedAt.UTC().Format(time.RFC3339),
		r.Status, r.AppliedBy,
	)
	if err != nil {
		return err
	}
	if err := upsertDeploymentStateFromRecord(ctx, tx, r, finishedAt.UTC(), true); err != nil {
		return err
	}
	return tx.Commit()
}

// boolToIntSQLite returns 1 if b is true, 0 otherwise — the
// canonical encoding for SQLite's INTEGER-as-BOOLEAN dialect.
func boolToIntSQLite(b bool) int {
	if b {
		return 1
	}
	return 0
}
