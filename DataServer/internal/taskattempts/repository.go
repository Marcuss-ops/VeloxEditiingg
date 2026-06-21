package taskattempts

import "context"

// Reader is the read-only attempt query surface.
type Reader interface {
	// Get returns a single attempt by ID, or (nil, nil) on missing.
	Get(ctx context.Context, id string) (*TaskAttempt, error)

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
