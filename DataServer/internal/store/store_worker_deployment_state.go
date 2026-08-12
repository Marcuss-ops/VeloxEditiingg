package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

// WorkerDeploymentState is the durable operator read model. The fields are
// intentionally independent: desired_digest describes intent, running_digest
// comes only from an authenticated heartbeat, last_successful_digest is
// never replaced by a newer failed operation, and last_phase records the
// in-flight rollout phase of the last operation (written by the fleet
// executor; terminal outcomes stay in last_operation_status).
//
// Error state is split (migration 153): LastOperationErrorCode is the stable
// machine-routable code (DIGEST_MISMATCH, DRAIN_TIMEOUT, …) while
// LastOperationError is the human-readable message. A new operation clears
// both; the history lives in deployment_records.error_code / error_message.
type WorkerDeploymentState struct {
	WorkerID               string    `json:"worker_id"`
	DesiredDigest          string    `json:"desired_digest"`
	RunningDigest          string    `json:"running_digest"`
	LastSuccessfulDigest   string    `json:"last_successful_digest"`
	LastOperationID        string    `json:"last_operation_id"`
	LastOperationKind      string    `json:"last_operation_kind"`
	LastOperationStatus    string    `json:"last_operation_status"`
	LastOperationErrorCode string    `json:"last_operation_error_code,omitempty"`
	LastOperationError     string    `json:"last_operation_error,omitempty"`
	LastPhase              string    `json:"last_phase,omitempty"`
	UpdatedAt              time.Time `json:"updated_at"`
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
    last_operation_id           TEXT NOT NULL DEFAULT '',
    last_operation_kind         TEXT NOT NULL DEFAULT '',
    last_operation_status       TEXT NOT NULL DEFAULT '',
    last_operation_error_code   TEXT NOT NULL DEFAULT '',
    last_operation_error        TEXT NOT NULL DEFAULT '',
    last_phase                  TEXT NOT NULL DEFAULT '',
    updated_at                  TEXT NOT NULL
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
       last_operation_error_code, last_operation_error, last_phase, updated_at
  FROM worker_deployment_state
 WHERE worker_id = ?`, workerID).Scan(
		&state.WorkerID, &state.DesiredDigest, &state.RunningDigest,
		&state.LastSuccessfulDigest, &state.LastOperationID,
		&state.LastOperationKind, &state.LastOperationStatus,
		&state.LastOperationErrorCode, &state.LastOperationError, &state.LastPhase, &updatedAt)
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

// upsertDeploymentStateFromRecord updates intent and operation history while
// preserving last_successful_digest on non-verified transitions and always
// preserving the last recorded rollout phase (migration 152).
//
// advanceLastSuccessful gates the last-known-good write: it is true ONLY for
// transitions that carry verified-digest semantics — a rollback restoring a
// previously verified digest, a verified baseline, or MarkVerifiedSucceeded.
// The generic SUCCEEDED path never advances it.
func upsertDeploymentStateFromRecord(ctx context.Context, exec deploymentStateExecer, r DeploymentRecord, updatedAt time.Time, advanceLastSuccessful bool) error {
	if r.WorkerID == "" || r.TargetDigest == "" {
		return fmt.Errorf("worker deployment state: incomplete deployment record")
	}
	lastSuccessful := ""
	if advanceLastSuccessful {
		lastSuccessful = r.TargetDigest
	}
	_, err := exec.ExecContext(ctx, `
INSERT INTO worker_deployment_state (
    worker_id, desired_digest, running_digest, last_successful_digest,
    last_operation_id, last_operation_kind, last_operation_status,
    last_operation_error_code, last_operation_error, last_phase, updated_at
)
VALUES (?, ?, NULL, ?, ?, ?, ?, ?, ?, '', ?)
ON CONFLICT(worker_id) DO UPDATE SET
    desired_digest = excluded.desired_digest,
    last_successful_digest = CASE
        WHEN excluded.last_successful_digest <> '' THEN excluded.last_successful_digest
        ELSE worker_deployment_state.last_successful_digest
    END,
    last_operation_id = excluded.last_operation_id,
    last_operation_kind = excluded.last_operation_kind,
    last_operation_status = excluded.last_operation_status,
    last_operation_error_code = excluded.last_operation_error_code,
    last_operation_error = excluded.last_operation_error,
    last_phase = CASE
        WHEN excluded.last_phase <> '' THEN excluded.last_phase
        ELSE worker_deployment_state.last_phase
    END,
    updated_at = excluded.updated_at`,
		r.WorkerID, r.TargetDigest, lastSuccessful, r.DeploymentID,
		deploymentStateKind(r.IsRollback), r.Status, r.ErrorCode, r.ErrorMessage,
		updatedAt.UTC().Format(time.RFC3339Nano))
	return err
}

// RecordDeploymentPhase persists the in-flight rollout phase of the last
// operation (DRAINING / DEPLOYING / RESTARTING / WAITING_READY /
// VERIFYING_DIGEST) into the read model. It is written by the fleet executor
// and is strictly orthogonal to digest state: the upsert never touches
// desired / running / last_successful digests, and it preserves the last
// recorded phase across later heartbeat or record-transition writes.
func (s *SQLiteStore) RecordDeploymentPhase(ctx context.Context, workerID, phase string) error {
	if workerID == "" || phase == "" {
		return fmt.Errorf("worker deployment state: record phase requires worker_id and phase")
	}
	_, err := s.db.ExecContext(ctx, `
INSERT INTO worker_deployment_state (
    worker_id, desired_digest, running_digest, last_successful_digest,
    last_operation_id, last_operation_kind, last_operation_status,
    last_operation_error_code, last_operation_error, last_phase, updated_at
)
VALUES (?, '', NULL, '', '', '', '', '', '', ?, ?)
ON CONFLICT(worker_id) DO UPDATE SET
    last_phase = excluded.last_phase,
    updated_at = excluded.updated_at`,
		workerID, phase, time.Now().UTC().Format(time.RFC3339Nano))
	return err
}

// DigestRefsEqual compares two image references by their digest part
// (everything after the last '@'), case-insensitively, ignoring surrounding
// whitespace. The ledger stores the full pinned ref (ghcr.io/...@sha256:xx)
// while the worker advertises the bare sha256:xx digest; only the digest
// part is semantically meaningful for verification. Empty references never
// compare equal (an unknown digest is not a matching digest).
func DigestRefsEqual(a, b string) bool {
	a = strings.ToLower(strings.TrimSpace(a))
	b = strings.ToLower(strings.TrimSpace(b))
	if at := strings.LastIndexByte(a, '@'); at >= 0 {
		a = a[at+1:]
	}
	if at := strings.LastIndexByte(b, '@'); at >= 0 {
		b = b[at+1:]
	}
	return a != "" && a == b
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
    last_operation_error_code, last_operation_error, updated_at
)
VALUES (?, '', ?, '', '', '', '', '', '', ?)
ON CONFLICT(worker_id) DO UPDATE SET
    running_digest = excluded.running_digest,
    updated_at = excluded.updated_at`,
		workerID, digest, updatedAt.UTC().Format(time.RFC3339Nano))
	return err
}
