package taskattempts

import (
	"context"
	"errors"
)

// ErrIdentityMismatch is the canonical sentinel returned when a TaskResult's
// wire identity tuple (task_id, job_id, attempt_id, attempt_number,
// worker_id, lease_id) does not match the authoritative stored values.
//
// PR-2 fix/canonical-attempt-identity. Callers must drop the offending
// message without transitioning state — the request may be a legitimate
// retry from a stale worker OR an impersonation attempt, and we cannot
// distinguish without operator review.
var ErrIdentityMismatch = errors.New("taskattempts: identity mismatch")

// Reader is the read-only attempt query surface.
type Reader interface {
	// Get returns a single attempt by ID, or (nil, nil) on missing.
	Get(ctx context.Context, id string) (*TaskAttempt, error)

	// GetByTaskIDAndWorkerAndLease returns the active attempt for the
	// (task_id, worker_id, lease_id) tuple. Used by the master's
	// handleTaskResult identity validation as the wire-fallback path when
	// a worker has not yet adopted the canonical attempt_id (PR-2 rollout).
	// Returns (nil, nil) when no attempt exists.
	GetByTaskIDAndWorkerAndLease(ctx context.Context, taskID, workerID, leaseID string) (*TaskAttempt, error)

	// ListByTaskID returns all attempts for a task, ordered by attempt number.
	ListByTaskID(ctx context.Context, taskID string) ([]TaskAttempt, error)

	// GetActiveAttempt returns the current non-terminal attempt for a task, or (nil, nil).
	GetActiveAttempt(ctx context.Context, taskID string) (*TaskAttempt, error)
}

// Writer is the canonical write-only attempt mutation surface.
type Writer interface {
	// Create inserts a new attempt in PENDING state.
	Create(ctx context.Context, attempt *TaskAttempt) error

	// SetStatus performs a CAS status change from → to.
	SetStatus(ctx context.Context, id string, from, to AttemptStatus, revision int) error

	// CompleteFinal marks an attempt as terminal (SUCCEEDED or FAILED) with
	// the worker-identity CAS tuple. Idempotent on already-terminal attempts.
	CompleteFinal(ctx context.Context, id, workerID, leaseID string, status AttemptStatus, errorCode, errorMessage string, revision int) error

	// Delete hard-deletes an attempt. Returns no error if already gone.
	Delete(ctx context.Context, id string) error
}

// Repository combines Reader and Writer into a single attempt persistence contract.
type Repository interface {
	Reader
	Writer
}
