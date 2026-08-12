package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// WorkerDeploymentState is the durable operator read model. The fields are
// intentionally independent: desired_digest describes intent, running_digest
// comes only from an authenticated heartbeat, and last_successful_digest is
// never replaced by a newer failed operation.
type WorkerDeploymentState struct {
	WorkerID             string    `json:"worker_id"`
	DesiredDigest        string    `json:"desired_digest"`
	RunningDigest        string    `json:"running_digest"`
	LastSuccessfulDigest string    `json:"last_successful_digest"`
	LastOperationID      string    `json:"last_operation_id"`
	LastOperationKind    string    `json:"last_operation_kind"`
	LastOperationStatus  string    `json:"last_operation_status"`
	LastOperationError   string    `json:"last_operation_error,omitempty"`
	UpdatedAt            time.Time `json:"updated_at"`
}

var ErrWorkerDeploymentStateNotFound = errors.New("worker deployment state not found")

type deploymentStateExecer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

type deploymentStateQuerier interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

// CreateWorkerDeploymentStateTableIfNotExists is the test/dev bootstrap
// counterpart of migration 151. It is also called by the deployment-record
// fixture because those tests intentionally create only the tables they use.
//
// running_digest is deliberately nullable: it is an OBSERVED value written
// only by an authenticated heartbeat, and stays NULL until the first such
// observation. A deployment record (control-plane intent) must never
// fabricate it.
func (s *SQLiteStore) CreateWorkerDeploymentStateTableIfNotExists() error {
	if s == nil || s.db == nil {
		return errors.New("worker deployment state: store not initialized")
	}
	_, err := s.db.Exec(`
CREATE TABLE IF NOT EXISTS worker_deployment_state (
    worker_id               TEXT PRIMARY KEY,
    desired_digest          TEXT NOT NULL DEFAULT '',
    running_digest          TEXT,
    last_successful_digest  TEXT NOT NULL DEFAULT '',
    last_operation_id       TEXT NOT NULL DEFAULT '',
    last_operation_kind     TEXT NOT NULL DEFAULT '',
    last_operation_status   TEXT NOT NULL DEFAULT '',
    last_operation_error    TEXT NOT NULL DEFAULT '',
    updated_at              TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_worker_deployment_state_status
    ON worker_deployment_state(last_operation_status, updated_at DESC);`)
	return err
}

// GetWorkerDeploymentState returns the persisted projection. Absence is
// distinct from an existing worker with empty fields.
func (s *SQLiteStore) GetWorkerDeploymentState(ctx context.Context, workerID string) (*WorkerDeploymentState, error) {
	if s == nil || s.db == nil {
		return nil, errors.New("worker deployment state: store not initialized")
	}
	var state WorkerDeploymentState
	var updatedAt string
	err := s.db.QueryRowContext(ctx, `
SELECT worker_id, desired_digest, COALESCE(running_digest, ''),
       last_successful_digest,
       last_operation_id, last_operation_kind, last_operation_status,
       last_operation_error, updated_at
  FROM worker_deployment_state
 WHERE worker_id = ?`, workerID).Scan(
		&state.WorkerID, &state.DesiredDigest, &state.RunningDigest,
		&state.LastSuccessfulDigest, &state.LastOperationID,
		&state.LastOperationKind, &state.LastOperationStatus,
		&state.LastOperationError, &updatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrWorkerDeploymentStateNotFound
	}
	if err != nil {
		return nil, err
	}
	state.UpdatedAt, err = parsePersistedWorkerTimestamp(updatedAt, "worker_deployment_state.updated_at")
	if err != nil {
		return nil, err
	}
	return &state, nil
}

func deploymentStateKind(isRollback bool) string {
	if isRollback {
		return "rollback"
	}
	return "update"
}

func deploymentStateSuccessful(status string) bool {
	return status == DeployStatusSucceeded || status == DeployStatusRolledBack
}

// upsertDeploymentStateFromRecord updates intent and operation history while
// preserving last_successful_digest on PENDING/FAILED transitions.
func upsertDeploymentStateFromRecord(ctx context.Context, exec deploymentStateExecer, r DeploymentRecord, updatedAt time.Time) error {
	if r.WorkerID == "" || r.TargetDigest == "" {
		return fmt.Errorf("worker deployment state: incomplete deployment record")
	}
	lastSuccessful := ""
	if deploymentStateSuccessful(r.Status) {
		lastSuccessful = r.TargetDigest
	}
	_, err := exec.ExecContext(ctx, `
INSERT INTO worker_deployment_state (
    worker_id, desired_digest, running_digest, last_successful_digest,
    last_operation_id, last_operation_kind, last_operation_status,
    last_operation_error, updated_at
)
VALUES (?, ?, NULL, ?, ?, ?, ?, ?, ?)
ON CONFLICT(worker_id) DO UPDATE SET
    desired_digest = excluded.desired_digest,
    last_successful_digest = CASE
        WHEN excluded.last_successful_digest <> '' THEN excluded.last_successful_digest
        ELSE worker_deployment_state.last_successful_digest
    END,
    last_operation_id = excluded.last_operation_id,
    last_operation_kind = excluded.last_operation_kind,
    last_operation_status = excluded.last_operation_status,
    last_operation_error = excluded.last_operation_error,
    updated_at = excluded.updated_at`,
		r.WorkerID, r.TargetDigest, lastSuccessful, r.DeploymentID,
		deploymentStateKind(r.IsRollback), r.Status, r.ErrorMessage,
		updatedAt.UTC().Format(time.RFC3339Nano))
	return err
}

// upsertWorkerRunningDigest records only an authenticated heartbeat value.
// Empty or absent heartbeat metadata never erases a previously observed
// running digest.
func upsertWorkerRunningDigest(ctx context.Context, exec deploymentStateExecer, workerID, digest string, updatedAt time.Time) error {
	if workerID == "" || digest == "" {
		return nil
	}
	_, err := exec.ExecContext(ctx, `
INSERT INTO worker_deployment_state (
    worker_id, desired_digest, running_digest, last_successful_digest,
    last_operation_id, last_operation_kind, last_operation_status,
    last_operation_error, updated_at
)
VALUES (?, '', ?, '', '', '', '', '', ?)
ON CONFLICT(worker_id) DO UPDATE SET
    running_digest = excluded.running_digest,
    updated_at = excluded.updated_at`,
		workerID, digest, updatedAt.UTC().Format(time.RFC3339Nano))
	return err
}
