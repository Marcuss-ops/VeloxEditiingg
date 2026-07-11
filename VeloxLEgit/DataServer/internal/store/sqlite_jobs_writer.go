package store

import (
	"context"
	"errors"
	"fmt"

	"velox-server/internal/jobs"
)

// SQLiteJobRepository implements jobs.Repository against *SQLiteStore.
//
// Writer methods (SetStatus, Lease, Fail, Start, Cancel, Delete) and
// Reader methods (Get, List, Counts) are inherited from the embedded
// baseJobRepository.
//
// fix/remove-job-lease-ops: Lease, Start, FailWithRetry, ClaimNext,
// ClaimNextForProfile, and all
// PR3 legacy wrappers (PR3Start, PR3RenewLease, PR3RecordRenderFinished,
// PR3Fail, PR3RequeueExpiredLeases) are REMOVED. Lease/claim/reap
// operations now live in the Task domain (taskgraph.Repository).
type SQLiteJobRepository struct {
	baseJobRepository
	store *SQLiteStore
}

var _ jobs.Repository = (*SQLiteJobRepository)(nil)

// NewSQLiteJobRepository wraps a SQLiteStore as a jobs.Repository.
func NewSQLiteJobRepository(store *SQLiteStore) *SQLiteJobRepository {
	return &SQLiteJobRepository{
		baseJobRepository: baseJobRepository{
			db:      store.db,
			dialect: sqliteDialect{store: store},
		},
		store: store,
	}
}

// NewJobsRepository returns the canonical jobs.Repository.
func NewJobsRepository(repo *SQLiteJobRepository) jobs.Repository { return repo }

// ── Legacy helpers (kept for compat) ────────────────────────────────

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// Transition wraps *SQLiteStore.TransitionJobStatus (kept for SetStatus compat).
func (r *SQLiteJobRepository) Transition(ctx context.Context, t TransitionParams) error {
	if t.JobID == "" {
		return fmt.Errorf("job repository: empty jobID")
	}
	_, err := r.store.TransitionJobStatus(ctx, t.JobID, string(t.ExpectedStatus), string(t.NewStatus), t.Revision)
	if err != nil {
		if errors.Is(err, ErrTransitionConflict) {
			return ErrTransitionConflict
		}
		return fmt.Errorf("transition: %w", err)
	}
	return nil
}
