// Package store / postgres_jobs_repository.go
//
// Postgres-side implementation of jobs.Repository.  Writer methods and
// Reader methods (Get / List / Counts) are inherited from the embedded
// baseJobRepository (via the postgresDialect which uses $n placeholders
// and no-op audit hooks).
//
// fix/remove-job-lease-ops: ClaimNext and ClaimNextForProfile are
// REMOVED. Task-level claiming (ClaimNextWithAttemptAtomic) is the
// canonical path.

package store

import (
	"velox-server/internal/jobs"
	"velox-server/internal/platform/database"
)

// PostgresJobRepository implements jobs.Repository on a *database.Handle.
type PostgresJobRepository struct {
	baseJobRepository
	handle *database.Handle
}

var _ jobs.Repository = (*PostgresJobRepository)(nil)

// NewPostgresJobRepository wraps a *database.Handle as a jobs.Repository.
func NewPostgresJobRepository(handle *database.Handle) *PostgresJobRepository {
	return &PostgresJobRepository{
		baseJobRepository: baseJobRepository{
			db:      handle.DB,
			dialect: postgresDialect{},
		},
		handle: handle,
	}
}
