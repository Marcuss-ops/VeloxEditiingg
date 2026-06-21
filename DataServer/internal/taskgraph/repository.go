package taskgraph

import "context"

// Filter narrows list queries on the Reader surface.
// Zero-value means "no filter" — all tasks are returned.
type Filter struct {
	JobIDs   []string // empty = all jobs
	Statuses []Status // empty = all statuses
	WorkerID string   // empty = no worker filter
	Limit    int      // 0 = all
}

// Reader is the read-only task query surface.
type Reader interface {
	// Get returns a single task by ID, or (nil, nil) on missing.
	Get(ctx context.Context, id string) (*Task, error)

	// List returns tasks matching the filter.
	List(ctx context.Context, filter Filter) ([]Task, error)

	// GetByJobID returns the task for a given job, or (nil, nil) on missing.
	// Invariant: each job has exactly one task.
	GetByJobID(ctx context.Context, jobID string) (*Task, error)
}

// Writer is the canonical write-only task mutation surface.
type Writer interface {
	// Create inserts a new task in PENDING state. If id is empty the
	// repository assigns one.
	Create(ctx context.Context, task *Task) error

	// SetStatus performs a CAS status change from → to, verifying revision.
	// Returns ErrTransitionConflict on mismatch.
	SetStatus(ctx context.Context, id string, from, to Status, revision int) error

	// Lease atomically assigns a READY task to a worker.
	// Returns ErrTransitionConflict if the task is not in READY.
	Lease(ctx context.Context, id, workerID, leaseID string) error

	// Start transitions LEASED → RUNNING with full CAS tuple.
	Start(ctx context.Context, id, workerID, leaseID string, attempt, revision int) error

	// Fail marks a task FAILED.
	Fail(ctx context.Context, id, reason string, revision int) error

	// IncrementAttempt bumps the attempt counter atomically.
	IncrementAttempt(ctx context.Context, id string) error

	// Delete hard-deletes a task. Returns no error if already gone.
	Delete(ctx context.Context, id string) error
}

// Repository combines Reader and Writer into a single task persistence contract.
type Repository interface {
	Reader
	Writer
}
