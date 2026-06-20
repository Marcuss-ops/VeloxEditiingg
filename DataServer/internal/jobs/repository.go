package jobs

import "context"

// Filter narrows list queries on the Reader surface.
// Zero-value means "no filter" — all jobs are returned.
type Filter struct {
	Statuses []Status // empty = all statuses
	Limit    int      // 0 = all
}

// Counts is the aggregate count by status returned by Reader.Counts.
type Counts map[Status]int64

// Reader is the read-only job query surface.
// Implemented by store.SQLiteJobRepository.
type Reader interface {
	// Get returns a single job by ID, or (nil, nil) on missing.
	Get(ctx context.Context, id string) (*Job, error)

	// List returns jobs matching the filter.
	List(ctx context.Context, filter Filter) ([]Job, error)

	// Counts returns the aggregate count of jobs grouped by status.
	Counts(ctx context.Context) (Counts, error)
}

// Writer is the write-only job mutation surface.
// Implemented by store.SQLiteJobRepository.
type Writer interface {
	// Create inserts a new job in PENDING state. If id is empty the
	// repository assigns one.
	Create(ctx context.Context, job *Job) error

	// Transition performs a CAS status change from → to.
	// Returns ErrTransitionConflict if the precondition does not hold.
	Transition(ctx context.Context, id string, from, to Status) error

	// Lease atomically assigns a PENDING job to a worker.
	// Returns ErrTransitionConflict if the job is not in PENDING.
	Lease(ctx context.Context, id, workerID string) error

	// Fail marks a job FAILED and records the reason.
	Fail(ctx context.Context, id string, reason string) error
}

// Repository combines Reader and Writer into a single job persistence contract.
// There must be exactly ONE concrete implementation — store.SQLiteJobRepository.
// This interface is the target surface that all new code should code against;
// legacy callers using store.JobRepository will be migrated in PR 2–4.
//
// TODO(PR 2): add compile-time assertion:
//   var _ Repository = (*store.SQLiteJobRepository)(nil)
// Currently the method signatures don't match (CreateJob vs Create, ClaimNext vs Lease)
// so the assertion would fail. PR 2 will align the store implementation.
type Repository interface {
	Reader
	Writer
}
